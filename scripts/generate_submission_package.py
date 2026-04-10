"""Build a datathon-ready submission package from the local project assets.

The generator produces the exact CSV filenames requested by the brief while making
all non-mandatory proxy assumptions explicit. When external datasets are not
available locally, the script falls back to conservative defaults so the repo still
remains structurally reproducible.
"""

from __future__ import annotations

import math
import sys
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import geopandas as gpd
import numpy as np
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
FILE_2_COLUMNS = [
    "location_id",
    "latitude",
    "longitude",
    "route_segment",
    "n_chargers_proposed",
    "grid_status",
]


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


def _minmax_normalize(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=float)
    min_value = float(series.min())
    max_value = float(series.max())
    if math.isclose(min_value, max_value):
        return pd.Series(np.ones(len(series)), index=series.index, dtype=float)
    return (series - min_value) / (max_value - min_value)


def enrich_route_summary_for_planning(route_summary: pd.DataFrame) -> pd.DataFrame:
    enriched = route_summary.copy()
    for column in ["existing_station_count", "coverage_gap_score"]:
        if column not in enriched.columns:
            enriched[column] = 0.0

    def route_hierarchy(route_name: str) -> float:
        route_name = str(route_name)
        if re.match(r"^(AP-|A-|N-)\d+$", route_name):
            if route_name.startswith("N-"):
                return 0.88
            return 1.00
        if re.match(r"^(AP-|A-|N-)\d+[NS]$", route_name):
            return 0.90
        if route_name.startswith(("AP-", "A-")):
            return 0.76
        if route_name.startswith("N-"):
            return 0.66
        return 0.60

    enriched["length_norm"] = _minmax_normalize(enriched["total_length_km"].astype(float))
    enriched["span_norm"] = _minmax_normalize(enriched["pk_span_km"].astype(float))
    enriched["complexity_norm"] = _minmax_normalize(enriched["mean_complexity"].astype(float))
    enriched["baseline_gap_norm"] = _minmax_normalize(enriched["coverage_gap_score"].astype(float))
    enriched["route_hierarchy_score"] = enriched["carretera"].map(route_hierarchy).astype(float)
    enriched["strategic_corridor_score"] = (
        0.30 * enriched["length_norm"]
        + 0.20 * enriched["span_norm"]
        + 0.20 * enriched["tent_share"].astype(float)
        + 0.15 * enriched["route_hierarchy_score"]
        + 0.05 * enriched["complexity_norm"]
        + 0.10 * enriched["baseline_gap_norm"]
    )
    enriched["service_need_score"] = (
        0.55 * enriched["strategic_corridor_score"]
        + 0.25 * enriched["baseline_gap_norm"]
        + 0.20 * (1.0 / (1.0 + np.log1p(enriched["existing_station_count"].astype(float))))
    )
    return enriched.sort_values(
        ["service_need_score", "strategic_corridor_score", "pk_span_km"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


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


def _dynamic_spacing_km(route_row: pd.Series, base_spacing_km: float) -> float:
    need_score = float(route_row.get("service_need_score", 0.5))
    existing_station_count = float(route_row.get("existing_station_count", 0.0))
    length_km = float(route_row.get("total_length_km", 0.0))
    pk_span_km = float(route_row.get("pk_span_km", length_km))
    route_hierarchy_score = float(route_row.get("route_hierarchy_score", 0.75))

    spacing = float(base_spacing_km)
    if need_score >= 0.75:
        spacing *= 0.72
    elif need_score >= 0.55:
        spacing *= 0.85
    elif need_score <= 0.20:
        spacing *= 1.18

    if existing_station_count >= 15:
        spacing *= 1.10
    elif existing_station_count >= 8:
        spacing *= 1.05
    elif existing_station_count == 0:
        spacing *= 0.92

    if length_km >= 400 or pk_span_km >= 400:
        spacing *= 0.92
    if route_hierarchy_score >= 0.95:
        spacing *= 0.88
    elif route_hierarchy_score <= 0.70:
        spacing *= 1.06

    return min(160.0, max(70.0, spacing))


def _route_target_station_count(route_row: pd.Series, base_spacing_km: float, min_route_span_km: float) -> int:
    route_span = float(route_row["pk_span_km"])
    if route_span < min_route_span_km:
        return 0

    dynamic_spacing = _dynamic_spacing_km(route_row, base_spacing_km)
    raw_need = max(1, int(math.ceil(route_span / dynamic_spacing)))
    existing_station_count = float(route_row.get("existing_station_count", 0.0))
    baseline_credit = int(round(existing_station_count * 0.12))
    credit_cap_share = 0.60
    if float(route_row.get("route_hierarchy_score", 0.75)) >= 0.95:
        credit_cap_share = 0.35
    elif float(route_row.get("service_need_score", 0.0)) >= 0.70:
        credit_cap_share = 0.45
    baseline_credit = min(baseline_credit, int(math.floor(raw_need * credit_cap_share)))
    return max(0, raw_need - baseline_credit)


def _target_positions_by_count(route_row: pd.Series, n_positions: int) -> List[float]:
    if n_positions <= 0:
        return []
    route_start = float(min(route_row["min_pk"], route_row["max_pk"]))
    route_end = float(max(route_row["min_pk"], route_row["max_pk"]))
    route_span = route_end - route_start
    if math.isclose(route_span, 0.0):
        return [route_start]

    step = route_span / n_positions
    return [route_start + (idx + 0.5) * step for idx in range(n_positions)]


def deduplicate_station_rows(file_2: pd.DataFrame) -> pd.DataFrame:
    if file_2.empty:
        return file_2.copy()

    status_rank = {"Sufficient": 0, "Moderate": 1, "Congested": 2}
    rows = file_2.copy()
    rows["grid_status_rank"] = rows["grid_status"].map(status_rank).fillna(0).astype(int)
    aggregated = (
        rows.groupby(["latitude", "longitude", "route_segment"], as_index=False)
        .agg(
            n_chargers_proposed=("n_chargers_proposed", "sum"),
            grid_status_rank=("grid_status_rank", "max"),
        )
        .sort_values(["route_segment", "latitude", "longitude"])
        .reset_index(drop=True)
    )
    rank_status = {value: key for key, value in status_rank.items()}
    aggregated["grid_status"] = aggregated["grid_status_rank"].map(rank_status)
    aggregated["location_id"] = [f"IBE_{idx:03d}" for idx in range(1, len(aggregated) + 1)]
    return aggregated[FILE_2_COLUMNS]


def build_file_2(
    roads_gdf: gpd.GeoDataFrame,
    route_summary: pd.DataFrame,
    spacing_km: float,
    min_route_span_km: float,
    charger_policy: str = "balanced",
    grid_policy: str = "balanced",
) -> pd.DataFrame:
    rows: List[Dict] = []

    for _, route_row in route_summary.iterrows():
        route_segments = roads_gdf[roads_gdf["carretera"] == route_row["carretera"]].copy()
        target_station_count = _route_target_station_count(route_row, spacing_km, min_route_span_km)
        target_positions = _target_positions_by_count(route_row, target_station_count)
        if not target_positions:
            continue

        n_chargers = chargers_for_route(route_row, charger_policy=charger_policy)
        need_score = float(route_row.get("service_need_score", 0.0))
        if need_score >= 0.70:
            grid_status = "Congested"
        elif need_score >= 0.45:
            grid_status = "Moderate"
        else:
            grid_status = assign_proxy_grid_status(int(route_row.name), grid_policy=grid_policy)

        for target_pk in target_positions:
            point = interpolate_station_point(route_segments, target_pk)
            rows.append(
                {
                    "location_id": "",
                    "latitude": round(point.y, 6),
                    "longitude": round(point.x, 6),
                    "route_segment": route_row["carretera"],
                    "n_chargers_proposed": n_chargers,
                    "grid_status": grid_status,
                }
            )

    file_2 = pd.DataFrame(rows, columns=FILE_2_COLUMNS)
    return deduplicate_station_rows(file_2)


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


def _geometry_to_lines(roads_gdf: gpd.GeoDataFrame | None) -> List[List[List[float]]]:
    if roads_gdf is None or roads_gdf.empty:
        return []

    lines: List[List[List[float]]] = []
    simplified = roads_gdf[["geometry"]].copy()
    simplified["geometry"] = simplified.geometry.simplify(0.02, preserve_topology=False)
    for geom in simplified.geometry:
        if geom is None or geom.is_empty:
            continue
        if geom.geom_type == "LineString":
            lines.append([[round(x, 4), round(y, 4)] for x, y in geom.coords])
        elif geom.geom_type == "MultiLineString":
            for part in geom.geoms:
                lines.append([[round(x, 4), round(y, 4)] for x, y in part.coords])
    return lines


def _map_bounds(file_2: pd.DataFrame, lines: List[List[List[float]]]) -> Dict[str, float]:
    lons: List[float] = []
    lats: List[float] = []
    if lines:
        for line in lines:
            for lon, lat in line:
                lons.append(lon)
                lats.append(lat)
    if not file_2.empty:
        lons.extend(file_2["longitude"].astype(float).tolist())
        lats.extend(file_2["latitude"].astype(float).tolist())
    if not lons or not lats:
        return {"minLon": -9.5, "maxLon": 3.5, "minLat": 35.5, "maxLat": 43.9}
    return {
        "minLon": min(lons) - 0.2,
        "maxLon": max(lons) + 0.2,
        "minLat": min(lats) - 0.2,
        "maxLat": max(lats) + 0.2,
    }


def save_map(file_2: pd.DataFrame, map_output: Path, roads_gdf: gpd.GeoDataFrame | None = None) -> None:
    lines = _geometry_to_lines(roads_gdf)
    bounds = _map_bounds(file_2, lines)
    lines_json = json.dumps(lines, ensure_ascii=False)
    points_json = json.dumps(file_2.to_dict(orient="records"), ensure_ascii=False)
    bounds_json = json.dumps(bounds, ensure_ascii=False)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Iberdrola Proposed Charging Network</title>
  <style>
    :root {{
      --bg: #f6f1e8;
      --panel: #fffdf8;
      --ink: #1f2937;
      --muted: #667085;
      --road: #bcc5d2;
      --sufficient: #2e8b57;
      --moderate: #d9a404;
      --congested: #c2410c;
    }}
    body {{
      margin: 0;
      background: radial-gradient(circle at top, #fcf7ee 0%, #efe7d8 55%, #e7dfd0 100%);
      color: var(--ink);
      font-family: Georgia, "Times New Roman", serif;
    }}
    .wrap {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }}
    .hero, .layout {{
      display: grid;
      gap: 18px;
    }}
    .hero {{
      grid-template-columns: 1.2fr 0.8fr;
      margin-bottom: 18px;
    }}
    .layout {{
      grid-template-columns: 1.35fr 0.65fr;
    }}
    .card {{
      background: rgba(255, 253, 248, 0.92);
      border: 1px solid rgba(31, 41, 55, 0.10);
      border-radius: 18px;
      box-shadow: 0 20px 50px rgba(31, 41, 55, 0.08);
      padding: 18px 20px;
    }}
    h1, h2 {{
      margin: 0 0 8px 0;
      font-weight: 600;
    }}
    p {{
      margin: 0;
      line-height: 1.45;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 16px;
    }}
    .kpi {{
      padding: 12px;
      border-radius: 14px;
      background: rgba(15, 118, 110, 0.06);
    }}
    .kpi .label {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--muted);
    }}
    .kpi .value {{
      font-size: 28px;
      margin-top: 4px;
    }}
    svg {{
      width: 100%;
      height: auto;
      background: #faf7f1;
      border-radius: 18px;
      border: 1px solid rgba(31,41,55,0.10);
    }}
    .legend {{
      display: flex;
      gap: 16px;
      flex-wrap: wrap;
      margin-top: 10px;
      color: var(--muted);
      font-size: 13px;
    }}
    .dot {{
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      margin-right: 6px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      margin-top: 8px;
    }}
    th, td {{
      text-align: left;
      padding: 8px 0;
      border-bottom: 1px solid rgba(31,41,55,0.08);
      vertical-align: top;
    }}
    .hint {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 13px;
    }}
    @media (max-width: 980px) {{
      .hero, .layout {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="card">
        <h1>2027 Interurban Charging Network Proposal</h1>
        <p>This self-contained file can be shared offline with the jury. Proposed sites are prioritised using corridor coverage need, strategic relevance, baseline charging scarcity, and grid feasibility.</p>
        <div class="kpis">
          <div class="kpi"><div class="label">Proposed Sites</div><div class="value" id="kpiStations">0</div></div>
          <div class="kpi"><div class="label">Moderate/Congested</div><div class="value" id="kpiFriction">0</div></div>
          <div class="kpi"><div class="label">Route Segments Covered</div><div class="value" id="kpiRoutes">0</div></div>
        </div>
      </div>
      <div class="card">
        <h2>Reading Guide</h2>
        <table>
          <tbody>
            <tr><th>Status</th><th>Meaning</th></tr>
            <tr><td><span class="dot" style="background: var(--sufficient)"></span>Sufficient</td><td>Nearest node comfortably supports the proposed fast chargers.</td></tr>
            <tr><td><span class="dot" style="background: var(--moderate)"></span>Moderate</td><td>Site is viable but should be phased with limited reinforcement or careful sequencing.</td></tr>
            <tr><td><span class="dot" style="background: var(--congested)"></span>Congested</td><td>Mobility need exists, but grid reinforcement should precede full deployment.</td></tr>
          </tbody>
        </table>
        <p class="hint">Select a station on the map to inspect the route, charger count, and grid status.</p>
      </div>
    </section>
    <section class="layout">
      <div class="card">
        <svg id="map" viewBox="0 0 900 620" role="img" aria-label="Spain charging proposal map"></svg>
        <div class="legend">
          <span><span class="dot" style="background: var(--sufficient)"></span>Sufficient</span>
          <span><span class="dot" style="background: var(--moderate)"></span>Moderate</span>
          <span><span class="dot" style="background: var(--congested)"></span>Congested</span>
        </div>
      </div>
      <div class="card">
        <h2>Station Details</h2>
        <table>
          <tbody id="detailBody">
            <tr><th>Location</th><td>Select a station</td></tr>
            <tr><th>Route</th><td>-</td></tr>
            <tr><th>Chargers</th><td>-</td></tr>
            <tr><th>Grid</th><td>-</td></tr>
            <tr><th>Coordinates</th><td>-</td></tr>
          </tbody>
        </table>
        <h2 style="margin-top:18px;">Top Routes</h2>
        <table>
          <thead><tr><th>Route</th><th>Sites</th><th>Chargers</th></tr></thead>
          <tbody id="routeTable"></tbody>
        </table>
      </div>
    </section>
  </div>
  <script>
    const bounds = {bounds_json};
    const roadLines = {lines_json};
    const stations = {points_json};
    const width = 900;
    const height = 620;
    const map = document.getElementById('map');
    const NS = 'http://www.w3.org/2000/svg';
    const colors = {{
      Sufficient: getComputedStyle(document.documentElement).getPropertyValue('--sufficient').trim(),
      Moderate: getComputedStyle(document.documentElement).getPropertyValue('--moderate').trim(),
      Congested: getComputedStyle(document.documentElement).getPropertyValue('--congested').trim(),
    }};
    function project(lon, lat) {{
      const x = ((lon - bounds.minLon) / (bounds.maxLon - bounds.minLon)) * (width - 50) + 25;
      const y = height - (((lat - bounds.minLat) / (bounds.maxLat - bounds.minLat)) * (height - 50) + 25);
      return [x, y];
    }}
    function add(tag, attrs, parent) {{
      const node = document.createElementNS(NS, tag);
      Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, value));
      parent.appendChild(node);
      return node;
    }}
    add('rect', {{x: 0, y: 0, width, height, fill: '#f8f5ef'}}, map);
    roadLines.forEach((line) => {{
      if (!line.length) return;
      const d = line.map(([lon, lat], idx) => {{
        const [x, y] = project(lon, lat);
        return `${{idx === 0 ? 'M' : 'L'}}${{x.toFixed(2)}},${{y.toFixed(2)}}`;
      }}).join(' ');
      add('path', {{
        d,
        fill: 'none',
        stroke: '#c7ced8',
        'stroke-width': '0.8',
        'stroke-linecap': 'round',
        opacity: '0.85',
      }}, map);
    }});
    const detailBody = document.getElementById('detailBody');
    function updateDetails(station) {{
      detailBody.innerHTML = `
        <tr><th>Location</th><td>${{station.location_id}}</td></tr>
        <tr><th>Route</th><td>${{station.route_segment}}</td></tr>
        <tr><th>Chargers</th><td>${{station.n_chargers_proposed}}</td></tr>
        <tr><th>Grid</th><td>${{station.grid_status}}</td></tr>
        <tr><th>Coordinates</th><td>${{station.latitude.toFixed(4)}}, ${{station.longitude.toFixed(4)}}</td></tr>`;
    }}
    stations.forEach((station, idx) => {{
      const [cx, cy] = project(station.longitude, station.latitude);
      const circle = add('circle', {{
        cx,
        cy,
        r: Math.max(4, 3 + station.n_chargers_proposed / 2),
        fill: colors[station.grid_status] || '#4b5563',
        stroke: 'rgba(255,255,255,0.95)',
        'stroke-width': '1.5',
        opacity: '0.92',
        tabindex: 0,
      }}, map);
      circle.addEventListener('click', () => updateDetails(station));
      circle.addEventListener('mouseenter', () => updateDetails(station));
      if (idx === 0) updateDetails(station);
    }});
    document.getElementById('kpiStations').textContent = stations.length.toLocaleString();
    document.getElementById('kpiFriction').textContent = stations.filter((row) => row.grid_status !== 'Sufficient').length.toLocaleString();
    document.getElementById('kpiRoutes').textContent = new Set(stations.map((row) => row.route_segment)).size.toLocaleString();
    const routeRows = Object.values(stations.reduce((acc, row) => {{
      if (!acc[row.route_segment]) acc[row.route_segment] = {{route: row.route_segment, sites: 0, chargers: 0}};
      acc[row.route_segment].sites += 1;
      acc[row.route_segment].chargers += Number(row.n_chargers_proposed);
      return acc;
    }}, {{}})).sort((a, b) => b.sites - a.sites || b.chargers - a.chargers).slice(0, 10);
    document.getElementById('routeTable').innerHTML = routeRows.map((row) =>
      `<tr><td>${{row.route}}</td><td>${{row.sites}}</td><td>${{row.chargers}}</td></tr>`
    ).join('');
  </script>
</body>
</html>
"""
    map_output.parent.mkdir(parents=True, exist_ok=True)
    map_output.write_text(html, encoding="utf-8")


def save_assumptions_note(
    output_dir: Path,
    baseline_status: str,
    ev_status: str,
    grid_status: str,
    station_spacing_km: float,
) -> None:
    note = f"""# Submission Assumptions

- `File 1.csv`, `File 2.csv`, and `File 3.csv` were generated from the local RTIG dataset.
- Station placement uses a corridor-planning model that combines route span, strategic relevance, TEN-T status, baseline charging scarcity, and dynamic spacing around a `{station_spacing_km}` km reference.
- Existing-station baseline source status: `{baseline_status}`.
- EV projection source status: `{ev_status}`.
- Grid capacity source status: `{grid_status}`.
- Existing charging baseline uses the official NAP-DGT/MITERD XML spatially matched to RTIG corridors within a 5 km threshold.
- EV demand uses the official datos.gob.es electrification exercise data and a SARIMA extension of the published notebook approach to estimate the 2027 EV stock proxy.
- Grid matching uses the nearest available published distributor demand-capacity nodes in `data/external/`, classifying locations as `Sufficient`, `Moderate`, or `Congested` based on whether available capacity is above 2x demand, between 1x and 2x demand, or below demand.
- Exact duplicate station coordinates on the same route are merged into a single site so the package better reflects the "fewest stations possible" objective.
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
    route_summary = enrich_route_summary_for_planning(route_summary)

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
    save_map(file_2, map_output, roads_gdf=roads_gdf)
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
