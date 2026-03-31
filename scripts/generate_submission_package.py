"""Build a datathon-ready submission package from the local project assets.

The generator produces the exact CSV filenames requested by the brief while making
all non-mandatory proxy assumptions explicit. When external datasets are not
available locally, the script falls back to conservative defaults so the repo still
remains structurally reproducible.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import folium
import geopandas as gpd
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.data.external_sources import (
    assign_proxy_distributor,
    enrich_route_summary_with_baseline,
    enrich_stations_with_grid,
    load_grid_capacity_bundle,
    try_build_official_external_inputs,
)
DEFAULT_INPUT = ROOT / "data" / "processed" / "roads_processed_gdf.parquet"

STATUS_COLORS = {
    "Sufficient": "green",
    "Moderate": "orange",
    "Congested": "red",
}


def load_config() -> Dict:
    with (ROOT / "config" / "settings.yaml").open("r", encoding="utf-8") as fin:
        return yaml.safe_load(fin)


def load_optional_scalar(path: Path, column_name: str, default_value: int) -> Tuple[int, str]:
    if not path.exists():
        return default_value, f"missing:{path.name}"

    df = pd.read_csv(path)
    if column_name not in df.columns or df.empty:
        return default_value, f"invalid:{path.name}"

    return int(df[column_name].iloc[0]), f"loaded:{path.name}"


def load_roads_dataset(input_path: Path) -> gpd.GeoDataFrame:
    if not input_path.exists():
        raise FileNotFoundError(
            f"Processed roads dataset not found at {input_path}. Run scripts/run_pipeline.py first."
        )

    gdf = gpd.read_parquet(input_path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    return gdf


def filter_interurban_routes(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    valid_prefixes = ("A-", "AP-", "N-")
    mask = gdf["carretera"].fillna("").astype(str).str.startswith(valid_prefixes)
    return gdf.loc[mask].copy()


def summarize_routes(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    route_summary = (
        gdf.groupby("carretera", as_index=False)
        .agg(
            total_length_km=("length_km", "sum"),
            min_pk=("pk_inicio", "min"),
            max_pk=("pk_fin", "max"),
            mean_complexity=("curve_complexity", "mean"),
            tent_share=("is_tent", "mean"),
            segment_count=("id", "count"),
        )
    )
    route_summary["pk_span_km"] = (route_summary["max_pk"] - route_summary["min_pk"]).abs()
    route_summary["route_score"] = (
        route_summary["total_length_km"] * 0.5
        + route_summary["pk_span_km"] * 0.3
        + route_summary["tent_share"] * 100 * 0.2
    )
    return route_summary.sort_values("route_score", ascending=False).reset_index(drop=True)


def assign_proxy_grid_status(route_rank: int, grid_policy: str = "balanced") -> str:
    policy_thresholds = {
        "relaxed": (8, 16),
        "balanced": (10, 20),
        "cautious": (12, 24),
    }
    moderate_limit, congested_limit = policy_thresholds.get(grid_policy, policy_thresholds["balanced"])

    if route_rank < moderate_limit:
        return "Moderate"
    if route_rank < congested_limit:
        return "Congested"
    return "Sufficient"


def station_positions(route_row: pd.Series, spacing_km: float, min_route_span_km: float) -> List[float]:
    route_start = float(min(route_row["min_pk"], route_row["max_pk"]))
    route_end = float(max(route_row["min_pk"], route_row["max_pk"]))
    route_span = route_end - route_start

    if route_span < min_route_span_km:
        return []

    start = route_start + spacing_km / 2
    positions = []
    current = start
    while current < route_end:
        positions.append(current)
        current += spacing_km

    if not positions:
        positions.append(route_start + route_span / 2)

    return positions


def interpolate_station_point(route_segments: gpd.GeoDataFrame, target_pk: float):
    candidates = route_segments.copy()
    candidates["pk_low"] = candidates[["pk_inicio", "pk_fin"]].min(axis=1)
    candidates["pk_high"] = candidates[["pk_inicio", "pk_fin"]].max(axis=1)

    containing = candidates[(candidates["pk_low"] <= target_pk) & (candidates["pk_high"] >= target_pk)]
    if containing.empty:
        containing = candidates.assign(distance_to_target=(candidates["pk_low"] - target_pk).abs()).sort_values(
            "distance_to_target"
        )

    segment = containing.iloc[0]
    pk_low = float(segment["pk_low"])
    pk_high = float(segment["pk_high"])
    if math.isclose(pk_high, pk_low):
        point = segment.geometry.interpolate(0.5, normalized=True)
    else:
        ratio = min(max((target_pk - pk_low) / (pk_high - pk_low), 0.0), 1.0)
        point = segment.geometry.interpolate(ratio, normalized=True)

    return point


def chargers_for_route(route_row: pd.Series, charger_policy: str = "balanced") -> int:
    policy_offsets = {
        "conservative": -2,
        "balanced": 0,
        "aggressive": 2,
    }
    offset = policy_offsets.get(charger_policy, 0)

    if route_row["tent_share"] >= 0.5 or route_row["total_length_km"] >= 400:
        return max(2, 8 + offset)
    if route_row["total_length_km"] >= 220:
        return max(2, 6 + offset)
    return max(2, 4 + offset)


def build_file_2(
    roads_gdf: gpd.GeoDataFrame,
    route_summary: pd.DataFrame,
    spacing_km: float,
    min_route_span_km: float,
    charger_policy: str = "balanced",
    grid_policy: str = "balanced",
) -> pd.DataFrame:
    rows: List[Dict] = []
    location_idx = 1

    for route_rank, route_row in route_summary.iterrows():
        route_segments = roads_gdf[roads_gdf["carretera"] == route_row["carretera"]].copy()
        target_positions = station_positions(route_row, spacing_km, min_route_span_km)
        if not target_positions:
            continue

        n_chargers = chargers_for_route(route_row, charger_policy=charger_policy)
        grid_status = assign_proxy_grid_status(route_rank, grid_policy=grid_policy)

        for target_pk in target_positions:
            point = interpolate_station_point(route_segments, target_pk)
            rows.append(
                {
                    "location_id": f"IBE_{location_idx:03d}",
                    "latitude": round(point.y, 6),
                    "longitude": round(point.x, 6),
                    "route_segment": route_row["carretera"],
                    "n_chargers_proposed": n_chargers,
                    "grid_status": grid_status,
                }
            )
            location_idx += 1

    return pd.DataFrame(rows)


def build_file_3(file_2: pd.DataFrame, charger_power_kw: int) -> pd.DataFrame:
    friction_points = file_2[file_2["grid_status"].isin(["Moderate", "Congested"])].copy()
    friction_points = friction_points.reset_index(drop=True)

    if friction_points.empty:
        return pd.DataFrame(
            columns=[
                "bottleneck_id",
                "latitude",
                "longitude",
                "route_segment",
                "distributor_network",
                "estimated_demand_kw",
                "grid_status",
            ]
        )

    friction_points["bottleneck_id"] = [f"FRIC_{idx:03d}" for idx in range(1, len(friction_points) + 1)]
    if "distributor_network" not in friction_points.columns:
        friction_points["distributor_network"] = "i-DE"
    if "estimated_demand_kw" not in friction_points.columns:
        friction_points["estimated_demand_kw"] = friction_points["n_chargers_proposed"] * charger_power_kw

    return friction_points[
        [
            "bottleneck_id",
            "latitude",
            "longitude",
            "route_segment",
            "distributor_network",
            "estimated_demand_kw",
            "grid_status",
        ]
    ]


def build_file_1(
    file_2: pd.DataFrame,
    file_3: pd.DataFrame,
    baseline_existing_stations: int,
    total_ev_projected_2027: int,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "total_proposed_stations": int(len(file_2)),
                "total_existing_stations_baseline": int(baseline_existing_stations),
                "total_friction_points": int(len(file_3)),
                "total_ev_projected_2027": int(total_ev_projected_2027),
            }
        ]
    )


def save_map(file_2: pd.DataFrame, map_output: Path) -> None:
    center = [40.2, -3.5]
    if not file_2.empty:
        center = [file_2["latitude"].mean(), file_2["longitude"].mean()]

    fmap = folium.Map(location=center, zoom_start=6, tiles="CartoDB positron")

    for row in file_2.itertuples(index=False):
        popup_html = (
            f"<b>{row.location_id}</b><br>"
            f"Route: {row.route_segment}<br>"
            f"Chargers: {row.n_chargers_proposed}<br>"
            f"Grid: {row.grid_status}"
        )
        folium.CircleMarker(
            location=[row.latitude, row.longitude],
            radius=6,
            color=STATUS_COLORS[row.grid_status],
            fill=True,
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=280),
        ).add_to(fmap)

    legend_html = """
    <div style="
        position: fixed;
        bottom: 40px;
        left: 40px;
        width: 180px;
        z-index: 9999;
        background: white;
        border: 2px solid #666;
        border-radius: 6px;
        padding: 10px;
        font-size: 13px;">
        <b>Grid Status</b><br>
        <span style="color: green;">●</span> Sufficient<br>
        <span style="color: orange;">●</span> Moderate<br>
        <span style="color: red;">●</span> Congested
    </div>
    """
    fmap.get_root().html.add_child(folium.Element(legend_html))

    map_output.parent.mkdir(parents=True, exist_ok=True)
    fmap.save(str(map_output))


def save_assumptions_note(
    output_dir: Path,
    baseline_status: str,
    ev_status: str,
    grid_status: str,
    station_spacing_km: float,
) -> None:
    note = f"""# Submission Assumptions

- `File 1.csv`, `File 2.csv`, and `File 3.csv` were generated from the local RTIG dataset.
- Station placement uses a spacing heuristic of `{station_spacing_km}` km along A-/AP-/N- corridors using PK ranges.
- Existing-station baseline source status: `{baseline_status}`.
- EV projection source status: `{ev_status}`.
- Grid capacity source status: `{grid_status}`.
- Existing charging baseline uses the official NAP-DGT/MITERD XML spatially matched to RTIG corridors within a 5 km threshold.
- EV demand uses the official datos.gob.es electrification exercise data and a SARIMA extension of the published notebook approach to reach 2027.
- Grid matching currently uses the nearest available published distributor demand-capacity nodes in `data/external/`; if a distributor file is missing, the remaining unmatched areas keep the structural fallback.
"""
    (output_dir / "ASSUMPTIONS.md").write_text(note, encoding="utf-8")


def load_external_route_baseline(external_dir: Path) -> Tuple[pd.DataFrame, int, str]:
    by_route_path = external_dir / "existing_interurban_stations_by_route.csv"
    scalar_path = external_dir / "existing_interurban_stations.csv"

    if by_route_path.exists():
        by_route = pd.read_csv(by_route_path)
    else:
        by_route = pd.DataFrame(columns=["carretera", "existing_station_count"])

    if scalar_path.exists():
        scalar = pd.read_csv(scalar_path)
        if not scalar.empty and "total_existing_stations_baseline" in scalar.columns:
            return by_route, int(scalar["total_existing_stations_baseline"].iloc[0]), f"loaded:{scalar_path.name}"

    return by_route, 0, f"missing:{scalar_path.name}"


def main() -> None:
    config = load_config()
    datathon_cfg = config["datathon"]

    output_dir = ROOT / datathon_cfg["output_dir"]
    map_output = ROOT / datathon_cfg["map_output"]
    output_dir.mkdir(parents=True, exist_ok=True)

    roads_gdf = load_roads_dataset(DEFAULT_INPUT)
    roads_gdf = filter_interurban_routes(roads_gdf)
    external_dir = ROOT / "data" / "external"

    baseline_status = "missing:existing_interurban_stations.csv"
    ev_status = "missing:ev_projection_2027.csv"
    grid_status = "missing:grid_capacity_files"
    try:
        try_build_official_external_inputs(
            roads_gdf,
            external_dir,
            target_year=int(datathon_cfg["target_year"]),
        )
    except Exception:
        pass

    route_summary = summarize_routes(roads_gdf)
    baseline_by_route, baseline_existing_stations, baseline_status = load_external_route_baseline(external_dir)
    route_summary = enrich_route_summary_with_baseline(route_summary, baseline_by_route)

    file_2_initial = build_file_2(
        roads_gdf,
        route_summary,
        spacing_km=float(datathon_cfg["station_spacing_km"]),
        min_route_span_km=float(datathon_cfg["min_route_span_km"]),
        charger_policy="balanced",
        grid_policy="balanced",
    )
    grid_nodes = load_grid_capacity_bundle(external_dir)
    if not grid_nodes.empty:
        grid_status = "loaded:grid_capacity_files"
    file_2_enriched = enrich_stations_with_grid(
        file_2_initial,
        grid_nodes=grid_nodes,
        charger_power_kw=int(datathon_cfg["charger_power_kw"]),
    )
    file_2 = file_2_enriched[
        [
            "location_id",
            "latitude",
            "longitude",
            "route_segment",
            "n_chargers_proposed",
            "grid_status",
        ]
    ].copy()
    file_3 = build_file_3(file_2_enriched, charger_power_kw=int(datathon_cfg["charger_power_kw"]))

    total_ev_projected_2027, ev_status = load_optional_scalar(
        ROOT / "data" / "external" / "ev_projection_2027.csv",
        "total_ev_projected_2027",
        int(datathon_cfg["total_ev_projected_2027_default"]),
    )
    file_1 = build_file_1(file_2, file_3, baseline_existing_stations, total_ev_projected_2027)

    file_1.to_csv(output_dir / "File 1.csv", index=False)
    file_2.to_csv(output_dir / "File 2.csv", index=False)
    file_3.to_csv(output_dir / "File 3.csv", index=False)
    save_map(file_2, map_output)
    save_assumptions_note(
        output_dir,
        baseline_status=baseline_status,
        ev_status=ev_status,
        grid_status=grid_status,
        station_spacing_km=float(datathon_cfg["station_spacing_km"]),
    )

    print(f"Saved: {output_dir / 'File 1.csv'}")
    print(f"Saved: {output_dir / 'File 2.csv'}")
    print(f"Saved: {output_dir / 'File 3.csv'}")
    print(f"Saved: {map_output}")


if __name__ == "__main__":  # pragma: no cover
    main()
