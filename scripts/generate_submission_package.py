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
import unicodedata
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
    enrich_route_summary_with_traffic,
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
BUSINESS_CATEGORY_RULES: Tuple[Tuple[str, Tuple[str, ...], float], ...] = (
    ("food", ("restaurant", "restaurante", "cafeter", "cafe", "fast food", "burger", "meson"), 3.0),
    ("lodging", ("hotel", "hostal", "hostel", "motel", "guest house", "aparthotel"), 2.7),
    ("fuel", ("repsol", "cepsa", "bp", "shell", "galp", "gasolin", "petrol"), 2.4),
    ("parking", ("parking", "aparcamiento", "park&ride"), 1.8),
    ("retail", ("supermerc", "supermarket", "centro comercial", "mall", "convenience", "shop"), 2.0),
    ("services", ("area de servicio", "service area", "autogrill", "rest area"), 2.2),
)
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
    for column in [
        "existing_station_count",
        "coverage_gap_score",
        "business_support_score",
        "traffic_imd_total",
        "traffic_heavy_share",
        "market_access_population",
        "tourism_overnight_stays",
    ]:
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
    enriched["business_norm"] = _minmax_normalize(enriched["business_support_score"].astype(float))
    enriched["population_norm"] = _minmax_normalize(np.log1p(enriched["market_access_population"].astype(float)))
    enriched["tourism_norm"] = _minmax_normalize(np.log1p(enriched["tourism_overnight_stays"].astype(float)))
    enriched["traffic_norm"] = _minmax_normalize(np.log1p(enriched["traffic_imd_total"].astype(float)))
    enriched["heavy_share_norm"] = _minmax_normalize(enriched["traffic_heavy_share"].astype(float))
    enriched["route_hierarchy_score"] = enriched["carretera"].map(route_hierarchy).astype(float)
    enriched["strategic_corridor_score"] = (
        0.22 * enriched["length_norm"]
        + 0.15 * enriched["span_norm"]
        + 0.16 * enriched["tent_share"].astype(float)
        + 0.12 * enriched["route_hierarchy_score"]
        + 0.05 * enriched["complexity_norm"]
        + 0.10 * enriched["baseline_gap_norm"]
        + 0.08 * enriched["business_norm"]
        + 0.05 * enriched["population_norm"]
        + 0.03 * enriched["tourism_norm"]
        + 0.06 * enriched["traffic_norm"]
        + 0.02 * enriched["heavy_share_norm"]
    )
    enriched["service_need_score"] = (
        0.46 * enriched["strategic_corridor_score"]
        + 0.22 * enriched["baseline_gap_norm"]
        + 0.15 * (1.0 / (1.0 + np.log1p(enriched["existing_station_count"].astype(float))))
        + 0.09 * enriched["business_norm"]
        + 0.03 * enriched["population_norm"]
        + 0.03 * enriched["tourism_norm"]
        + 0.02 * enriched["traffic_norm"]
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

    traffic_imd_total = float(route_row.get("traffic_imd_total", 0.0))

    if route_row["tent_share"] >= 0.5 or route_row["total_length_km"] >= 400 or traffic_imd_total >= 25000:
        return max(2, 8 + offset)
    if route_row["total_length_km"] >= 220 or traffic_imd_total >= 12000:
        return max(2, 6 + offset)
    return max(2, 4 + offset)


def _dynamic_spacing_km(route_row: pd.Series, base_spacing_km: float) -> float:
    need_score = float(route_row.get("service_need_score", 0.5))
    existing_station_count = float(route_row.get("existing_station_count", 0.0))
    length_km = float(route_row.get("total_length_km", 0.0))
    pk_span_km = float(route_row.get("pk_span_km", length_km))
    route_hierarchy_score = float(route_row.get("route_hierarchy_score", 0.75))
    traffic_imd_total = float(route_row.get("traffic_imd_total", 0.0))

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
    if traffic_imd_total >= 25000:
        spacing *= 0.92
    elif traffic_imd_total >= 12000:
        spacing *= 0.97
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
    baseline_credit = int(round(existing_station_count * 0.15))
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
    if "business_score" not in rows.columns:
        rows["business_score"] = 0.0
    aggregated = (
        rows.groupby(["latitude", "longitude", "route_segment"], as_index=False)
        .agg(
            n_chargers_proposed=("n_chargers_proposed", "sum"),
            grid_status_rank=("grid_status_rank", "max"),
            business_score=("business_score", "max"),
        )
        .sort_values(["route_segment", "latitude", "longitude"])
        .reset_index(drop=True)
    )
    rank_status = {value: key for key, value in status_rank.items()}
    aggregated["grid_status"] = aggregated["grid_status_rank"].map(rank_status)
    aggregated["location_id"] = [f"IBE_{idx:03d}" for idx in range(1, len(aggregated) + 1)]
    base_columns = FILE_2_COLUMNS + [column for column in aggregated.columns if column not in FILE_2_COLUMNS]
    return aggregated[base_columns]


def _route_family(route_segment: str) -> str:
    route_segment = str(route_segment)
    digits = "".join(re.findall(r"\d+", route_segment))
    if digits:
        return digits
    return route_segment


def _haversine_pair_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _business_text(row: pd.Series) -> str:
    values = [
        str(row.get("site_name", "") or ""),
        str(row.get("address_text", "") or ""),
        str(row.get("operator_name", "") or ""),
    ]
    return " ".join(values).strip().lower()


def _extract_business_tags(text: str) -> Tuple[List[str], float]:
    tags: List[str] = []
    score = 0.0
    normalized = text.lower()
    for category, keywords, weight in BUSINESS_CATEGORY_RULES:
        if any(keyword in normalized for keyword in keywords):
            tags.append(category)
            score += weight
    return tags, score


def _normalize_lookup_text(value: str) -> str:
    normalized = unicodedata.normalize("NFD", str(value or ""))
    normalized = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
    normalized = re.sub(r"[^A-Z0-9]+", " ", normalized.upper()).strip()
    return re.sub(r"\s+", " ", normalized)


def load_business_context(external_dir: Path) -> Tuple[pd.DataFrame, str]:
    matched_path = external_dir / "existing_interurban_stations_matched.csv"
    gasolineras_path = external_dir / "geoportal_gasolineras_matched.csv"
    population_path = external_dir / "ine_municipal_population_2025.csv"
    tourism_path = external_dir / "ine_provincial_overnight_stays.csv"
    empty = pd.DataFrame(
        columns=[
            "latitude",
            "longitude",
            "route_segment",
            "route_family",
            "business_score",
            "business_tags",
            "municipal_population",
            "municipality_key",
            "province_key",
            "tourism_overnight_stays",
        ]
    )
    contexts: List[pd.DataFrame] = []
    statuses: List[str] = []
    population_df = pd.DataFrame(columns=["municipality_code", "municipality_name", "municipal_population"])
    tourism_df = pd.DataFrame(columns=["province_name", "tourism_year", "provincial_overnight_stays"])
    if population_path.exists():
        population_df = pd.read_csv(population_path)
        if not population_df.empty:
            statuses.append(f"loaded:{population_path.name}")
    if tourism_path.exists():
        tourism_df = pd.read_csv(tourism_path)
        if not tourism_df.empty:
            statuses.append(f"loaded:{tourism_path.name}")

    tourism_lookup = pd.DataFrame(columns=["province_key", "provincial_overnight_stays"])
    if not tourism_df.empty:
        tourism_lookup = tourism_df.copy()
        tourism_lookup["province_key"] = tourism_lookup["province_name"].map(_normalize_lookup_text)
        tourism_lookup = tourism_lookup[["province_key", "provincial_overnight_stays"]].drop_duplicates("province_key")

    if matched_path.exists():
        matched = pd.read_csv(matched_path)
        if matched.empty:
            statuses.append(f"empty:{matched_path.name}")
        else:
            if "is_interurban_match" in matched.columns:
                matched = matched[matched["is_interurban_match"].fillna(False)].copy()
            if "distance_to_route_km" in matched.columns:
                matched = matched[matched["distance_to_route_km"].fillna(np.inf) <= 8.0].copy()
            matched = matched.dropna(subset=["latitude", "longitude"]).copy()
            if "carretera" in matched.columns:
                matched["route_segment"] = matched["carretera"].fillna("")
            else:
                matched["route_segment"] = ""

            texts = matched.apply(_business_text, axis=1)
            tag_payload = texts.apply(_extract_business_tags)
            matched["business_tags"] = tag_payload.apply(lambda item: ", ".join(item[0]))
            matched["business_score"] = tag_payload.apply(lambda item: float(item[1]))
            matched = matched[matched["business_score"] > 0].copy()
            if not matched.empty:
                matched["route_family"] = matched["route_segment"].map(_route_family)
                matched["municipal_population"] = 0.0
                matched["municipality_key"] = ""
                matched["province_key"] = ""
                matched["tourism_overnight_stays"] = 0.0
                contexts.append(
                    matched[
                        [
                            "latitude",
                            "longitude",
                            "route_segment",
                            "route_family",
                            "business_score",
                            "business_tags",
                            "municipal_population",
                            "municipality_key",
                            "province_key",
                            "tourism_overnight_stays",
                        ]
                    ]
                )
                statuses.append(f"loaded:{matched_path.name}")
            else:
                statuses.append(f"no_business_matches:{matched_path.name}")

    if gasolineras_path.exists():
        gas = pd.read_csv(gasolineras_path)
        if gas.empty:
            statuses.append(f"empty:{gasolineras_path.name}")
        else:
            if "is_interurban_match" in gas.columns:
                gas = gas[gas["is_interurban_match"].fillna(False)].copy()
            if "distance_to_route_km" in gas.columns:
                gas = gas[gas["distance_to_route_km"].fillna(np.inf) <= 8.0].copy()
            gas = gas.dropna(subset=["latitude", "longitude"]).copy()
            gas["route_segment"] = gas.get("carretera", pd.Series("", index=gas.index)).fillna("")
            gas["business_tags"] = "fuel, roadside services, official miterd"
            gas["business_score"] = 2.4
            if "tipo_venta" in gas.columns:
                gas.loc[gas["tipo_venta"].fillna("").astype(str).str.upper() == "P", "business_score"] += 0.3
            if "horario" in gas.columns:
                hours = gas["horario"].fillna("").astype(str).str.upper()
                gas.loc[hours.str.contains("24H|24 H|24:00|00:00-24:00", regex=True), "business_score"] += 0.4
                gas.loc[hours.str.contains("L-D"), "business_score"] += 0.2
            if "tipo_servicio" in gas.columns:
                service = gas["tipo_servicio"].fillna("").astype(str).str.upper()
                gas.loc[service.str.contains("\\(P\\)|PERSONAL|ASIST", regex=True), "business_score"] += 0.1
                gas.loc[service.str.contains("\\(A\\)|AUTOSERV", regex=True), "business_score"] += 0.05
            gas["municipality_code"] = gas.get("municipality_id", pd.Series("", index=gas.index)).astype(str).str.zfill(5)
            gas["municipality_name_norm"] = gas.get("municipio", pd.Series("", index=gas.index)).fillna("").astype(str).str.upper().str.strip()
            gas["municipality_key"] = gas["municipality_code"].where(gas["municipality_code"].str.strip() != "00000", gas["municipality_name_norm"])
            gas["municipal_population"] = 0.0
            gas["province_key"] = gas.get("provincia", pd.Series("", index=gas.index)).fillna("").map(_normalize_lookup_text)
            gas["tourism_overnight_stays"] = 0.0
            if not population_df.empty:
                population_lookup = population_df.copy()
                population_lookup["municipality_code"] = population_lookup["municipality_code"].astype(str).str.zfill(5)
                population_lookup["municipality_name_norm"] = (
                    population_lookup["municipality_name"].fillna("").astype(str).str.upper().str.strip()
                )
                gas = gas.merge(
                    population_lookup[["municipality_code", "municipality_name_norm", "municipal_population"]],
                    how="left",
                    on="municipality_code",
                    suffixes=("", "_by_code"),
                )
                gas["municipal_population"] = gas["municipal_population_by_code"].fillna(gas["municipal_population"]).fillna(0.0)
                if (gas["municipal_population"].fillna(0.0) <= 0).any():
                    by_name = population_lookup[["municipality_name_norm", "municipal_population"]].rename(
                        columns={"municipal_population": "municipal_population_by_name"}
                    )
                    gas = gas.merge(by_name, how="left", on="municipality_name_norm")
                    missing_population = gas["municipal_population"].fillna(0.0) <= 0
                    gas.loc[missing_population, "municipal_population"] = (
                        gas.loc[missing_population, "municipal_population_by_name"].fillna(0.0)
                    )
                gas = gas.drop(
                    columns=["municipal_population_by_code", "municipal_population_by_name"],
                    errors="ignore",
                )
                pop_norm = np.log1p(gas["municipal_population"].fillna(0.0))
                max_pop_norm = float(pop_norm.max()) if len(pop_norm) else 0.0
                if max_pop_norm > 0:
                    gas["business_score"] += 0.75 * (pop_norm / max_pop_norm)
            if not tourism_lookup.empty:
                gas = gas.merge(tourism_lookup, how="left", on="province_key", suffixes=("", "_tourism"))
                gas["tourism_overnight_stays"] = gas["provincial_overnight_stays"].fillna(0.0)
                tourism_norm = np.log1p(gas["tourism_overnight_stays"].fillna(0.0))
                max_tourism_norm = float(tourism_norm.max()) if len(tourism_norm) else 0.0
                if max_tourism_norm > 0:
                    gas["business_score"] += 0.55 * (tourism_norm / max_tourism_norm)
                gas = gas.drop(columns=["provincial_overnight_stays"], errors="ignore")
            gas["route_family"] = gas["route_segment"].map(_route_family)
            if not gas.empty:
                contexts.append(
                    gas[
                        [
                            "latitude",
                            "longitude",
                            "route_segment",
                            "route_family",
                            "business_score",
                            "business_tags",
                            "municipal_population",
                            "municipality_key",
                            "province_key",
                            "tourism_overnight_stays",
                        ]
                    ]
                )
                statuses.append(f"loaded:{gasolineras_path.name}")
            else:
                statuses.append(f"no_business_matches:{gasolineras_path.name}")

    if not contexts:
        if statuses:
            return empty, " + ".join(statuses)
        return empty, f"missing:{matched_path.name}+{gasolineras_path.name}"

    business_context = pd.concat(contexts, ignore_index=True).reset_index(drop=True)
    return business_context, " + ".join(statuses)


def enrich_route_summary_with_business(route_summary: pd.DataFrame, business_context: pd.DataFrame) -> pd.DataFrame:
    enriched = route_summary.copy()
    if "business_support_score" not in enriched.columns:
        enriched["business_support_score"] = 0.0
    if "business_anchor_count" not in enriched.columns:
        enriched["business_anchor_count"] = 0
    if "market_access_population" not in enriched.columns:
        enriched["market_access_population"] = 0.0
    if "tourism_overnight_stays" not in enriched.columns:
        enriched["tourism_overnight_stays"] = 0.0
    if business_context is None or business_context.empty:
        return enriched

    population_support = pd.DataFrame(columns=["route_segment", "market_access_population"])
    tourism_support = pd.DataFrame(columns=["route_segment", "tourism_overnight_stays"])
    if "municipal_population" in business_context.columns:
        population_support = (
            business_context.drop_duplicates(subset=["route_segment", "municipality_key"])
            .groupby("route_segment", as_index=False)
            .agg(market_access_population=("municipal_population", "sum"))
        )
    if "tourism_overnight_stays" in business_context.columns:
        tourism_support = (
            business_context.drop_duplicates(subset=["route_segment", "province_key"])
            .groupby("route_segment", as_index=False)
            .agg(tourism_overnight_stays=("tourism_overnight_stays", "sum"))
        )
    grouped = (
        business_context.groupby("route_segment", as_index=False)
        .agg(
            business_support_score=("business_score", "sum"),
            business_anchor_count=("business_score", "size"),
        )
    )
    grouped = grouped.merge(population_support, how="left", on="route_segment")
    grouped = grouped.merge(tourism_support, how="left", on="route_segment")
    grouped = grouped.rename(
        columns={
            "business_support_score": "business_support_score_new",
            "business_anchor_count": "business_anchor_count_new",
            "market_access_population": "market_access_population_new",
            "tourism_overnight_stays": "tourism_overnight_stays_new",
        }
    )
    enriched = enriched.merge(grouped, how="left", left_on="carretera", right_on="route_segment")
    enriched["business_support_score"] = enriched["business_support_score_new"].fillna(
        enriched["business_support_score"]
    )
    enriched["business_anchor_count"] = (
        enriched["business_anchor_count_new"].fillna(enriched["business_anchor_count"]).fillna(0).astype(int)
    )
    enriched["market_access_population"] = (
        enriched["market_access_population_new"].fillna(enriched["market_access_population"]).fillna(0.0)
    )
    enriched["tourism_overnight_stays"] = (
        enriched["tourism_overnight_stays_new"].fillna(enriched["tourism_overnight_stays"]).fillna(0.0)
    )
    return enriched.drop(
        columns=[
            "route_segment",
            "business_support_score_new",
            "business_anchor_count_new",
            "market_access_population_new",
            "tourism_overnight_stays_new",
        ],
        errors="ignore",
    )


def _business_signal_for_point(
    latitude: float,
    longitude: float,
    route_segment: str,
    business_context: pd.DataFrame | None,
    radius_km: float = 20.0,
) -> float:
    if business_context is None or business_context.empty:
        return 0.0

    route_family = _route_family(route_segment)
    candidates = business_context[business_context["route_family"] == route_family].copy()
    if candidates.empty:
        candidates = business_context

    score = 0.0
    for _, anchor in candidates.iterrows():
        distance_km = _haversine_pair_km(
            float(latitude),
            float(longitude),
            float(anchor["latitude"]),
            float(anchor["longitude"]),
        )
        if distance_km > radius_km:
            continue
        score += float(anchor["business_score"]) * math.exp(-(distance_km ** 2) / (2 * (radius_km / 2.5) ** 2))
    return score


def select_business_weighted_point(
    route_segments: gpd.GeoDataFrame,
    route_row: pd.Series,
    target_pk: float,
    business_context: pd.DataFrame | None,
    search_window_km: float,
) -> Tuple[object, float]:
    route_start = float(min(route_row["min_pk"], route_row["max_pk"]))
    route_end = float(max(route_row["min_pk"], route_row["max_pk"]))
    offsets = sorted({0.0, -search_window_km, search_window_km, -(search_window_km / 2), search_window_km / 2})
    best_point = None
    best_signal = -1.0
    best_penalty = float("inf")

    for offset in offsets:
        candidate_pk = min(route_end, max(route_start, target_pk + offset))
        point = interpolate_station_point(route_segments, candidate_pk)
        signal = _business_signal_for_point(
            latitude=float(point.y),
            longitude=float(point.x),
            route_segment=str(route_row["carretera"]),
            business_context=business_context,
        )
        center_penalty = abs(offset)
        if signal > best_signal or (math.isclose(signal, best_signal) and center_penalty < best_penalty):
            best_point = point
            best_signal = signal
            best_penalty = center_penalty

    return best_point, best_signal


def _should_merge_nearby_sites(left: pd.Series, right: pd.Series, merge_distance_km: float) -> bool:
    distance_km = _haversine_pair_km(
        float(left["latitude"]),
        float(left["longitude"]),
        float(right["latitude"]),
        float(right["longitude"]),
    )
    if distance_km > merge_distance_km:
        return False
    if distance_km <= 2.0:
        return True
    return _route_family(str(left["route_segment"])) == _route_family(str(right["route_segment"]))


def merge_nearby_station_rows(file_2: pd.DataFrame, merge_distance_km: float = 8.0) -> pd.DataFrame:
    if file_2.empty:
        return file_2.copy()

    status_rank = {"Sufficient": 0, "Moderate": 1, "Congested": 2}
    working = file_2.copy().reset_index(drop=True)
    consumed: set[int] = set()
    merged_rows: List[Dict] = []

    for idx, row in working.iterrows():
        if idx in consumed:
            continue

        cluster = [idx]
        consumed.add(idx)
        for other_idx in range(idx + 1, len(working)):
            if other_idx in consumed:
                continue
            if _should_merge_nearby_sites(row, working.loc[other_idx], merge_distance_km=merge_distance_km):
                cluster.append(other_idx)
                consumed.add(other_idx)

        cluster_df = working.loc[cluster].copy()
        if "business_score" not in cluster_df.columns:
            cluster_df["business_score"] = 0.0
        representative = cluster_df.sort_values(
            by=["business_score", "n_chargers_proposed", "route_segment"],
            ascending=[False, False, True],
        ).iloc[0]
        business_score = float(cluster_df["business_score"].max())
        merged_rows.append(
            {
                "location_id": "",
                "latitude": round(float(cluster_df["latitude"].mean()), 6),
                "longitude": round(float(cluster_df["longitude"].mean()), 6),
                "route_segment": representative["route_segment"],
                "n_chargers_proposed": int(cluster_df["n_chargers_proposed"].sum()),
                "grid_status": max(cluster_df["grid_status"], key=lambda value: status_rank.get(value, 0)),
                "business_score": round(business_score, 2),
            }
        )

    merged = pd.DataFrame(merged_rows)
    merged["location_id"] = [f"IBE_{idx:03d}" for idx in range(1, len(merged) + 1)]
    base_columns = FILE_2_COLUMNS + [column for column in merged.columns if column not in FILE_2_COLUMNS]
    return merged[base_columns]


def build_file_2(
    roads_gdf: gpd.GeoDataFrame,
    route_summary: pd.DataFrame,
    spacing_km: float,
    min_route_span_km: float,
    charger_policy: str = "balanced",
    grid_policy: str = "balanced",
    business_context: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rows: List[Dict] = []

    for _, route_row in route_summary.iterrows():
        route_segments = roads_gdf[roads_gdf["carretera"] == route_row["carretera"]].copy()
        target_station_count = _route_target_station_count(route_row, spacing_km, min_route_span_km)
        target_positions = _target_positions_by_count(route_row, target_station_count)
        if not target_positions:
            continue

        n_chargers = chargers_for_route(route_row, charger_policy=charger_policy)
        search_window_km = min(18.0, max(6.0, _dynamic_spacing_km(route_row, spacing_km) * 0.18))
        need_score = float(route_row.get("service_need_score", 0.0))
        if need_score >= 0.70:
            grid_status = "Congested"
        elif need_score >= 0.45:
            grid_status = "Moderate"
        else:
            grid_status = assign_proxy_grid_status(int(route_row.name), grid_policy=grid_policy)

        for target_pk in target_positions:
            point, business_signal = select_business_weighted_point(
                route_segments,
                route_row,
                target_pk,
                business_context=business_context,
                search_window_km=search_window_km,
            )
            rows.append(
                {
                    "location_id": "",
                    "latitude": round(point.y, 6),
                    "longitude": round(point.x, 6),
                    "route_segment": route_row["carretera"],
                    "n_chargers_proposed": n_chargers,
                    "grid_status": grid_status,
                    "business_score": round(float(business_signal), 2),
                }
            )

    file_2 = pd.DataFrame(rows)
    file_2 = deduplicate_station_rows(file_2)
    return merge_nearby_station_rows(file_2)


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
	    .legend-block {{
	      margin-top: 12px;
	      padding: 12px 14px;
	      border-radius: 14px;
	      background: rgba(255,255,255,0.62);
	      border: 1px solid rgba(31,41,55,0.08);
	    }}
	    .legend-title {{
	      font-size: 12px;
	      letter-spacing: 0.06em;
	      text-transform: uppercase;
	      color: var(--muted);
	      margin-bottom: 8px;
	    }}
	    .dot {{
	      display: inline-block;
	      width: 10px;
	      height: 10px;
	      border-radius: 50%;
	      margin-right: 6px;
	    }}
	    .size-chip {{
	      display: inline-flex;
	      align-items: center;
	      gap: 8px;
	      margin-right: 14px;
	    }}
	    .size-circle {{
	      display: inline-block;
	      border-radius: 50%;
	      background: rgba(31,41,55,0.16);
	      border: 1px solid rgba(31,41,55,0.18);
	      vertical-align: middle;
	      flex: 0 0 auto;
	    }}
        .halo-chip {{
          display: inline-flex;
          align-items: center;
          gap: 8px;
          margin-right: 14px;
        }}
        .halo-sample {{
          display: inline-block;
          border-radius: 999px;
          border: 2px solid rgba(217, 164, 4, 0.45);
          box-shadow: 0 0 0 4px rgba(217, 164, 4, 0.10);
          flex: 0 0 auto;
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
        <p>This self-contained file can be shared offline with the jury. Proposed sites are prioritised using corridor coverage need, strategic relevance, baseline charging scarcity, nearby business amenities, and grid feasibility.</p>
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
	            <tr><td><span class="dot" style="background: var(--sufficient)"></span>Sufficient</td><td>Color indicates <b>grid feasibility</b>. Here the nearest published grid node appears to support the proposed site comfortably.</td></tr>
	            <tr><td><span class="dot" style="background: var(--moderate)"></span>Moderate</td><td>Color still refers to <b>grid feasibility</b>. The site is plausible, but phased rollout or limited reinforcement may be needed.</td></tr>
	            <tr><td><span class="dot" style="background: var(--congested)"></span>Congested</td><td>Color still refers to <b>grid feasibility</b>. Mobility need exists, but grid reinforcement should precede full deployment.</td></tr>
	          </tbody>
	        </table>
	        <div class="legend-block">
	          <div class="legend-title">Dot Size</div>
	          <div class="legend">
	            <span class="size-chip"><span class="size-circle" style="width:10px;height:10px;"></span>Smaller dot = fewer chargers at that site</span>
	            <span class="size-chip"><span class="size-circle" style="width:18px;height:18px;"></span>Larger dot = more chargers at that site</span>
	          </div>
	        </div>
	        <div class="legend-block">
	          <div class="legend-title">Business Fit Halo</div>
	          <div class="legend">
	            <span class="halo-chip"><span class="halo-sample" style="width:12px;height:12px;"></span>Faint halo = lower business fit</span>
	            <span class="halo-chip"><span class="halo-sample" style="width:18px;height:18px;border-color:rgba(217,164,4,0.85);box-shadow:0 0 0 7px rgba(217,164,4,0.18);"></span>Stronger halo = higher business fit</span>
	          </div>
	        </div>
	        <p class="hint">Each dot is one proposed station site. Candidate locations are also nudged toward nearby restaurants, hotels, fuel, parking, and retail services when those amenities are visible in the baseline interurban ecosystem.</p>
	      </div>
	    </section>
	    <section class="layout">
	      <div class="card">
	        <svg id="map" viewBox="0 0 900 620" role="img" aria-label="Spain charging proposal map"></svg>
	        <div class="legend">
	          <span><span class="dot" style="background: var(--sufficient)"></span>Sufficient grid</span>
	          <span><span class="dot" style="background: var(--moderate)"></span>Moderate grid</span>
	          <span><span class="dot" style="background: var(--congested)"></span>Congested grid</span>
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
            <tr><th>Business Fit Score</th><td>-</td></tr>
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
        const maxBusinessScore = Math.max(...stations.map((station) => Number(station.business_score || 0)), 0);
	    function add(tag, attrs, parent) {{
	      const node = document.createElementNS(NS, tag);
	      Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, value));
	      parent.appendChild(node);
	      return node;
	    }}
	    function spreadPoint(baseX, baseY, radius, placed) {{
	      const minGap = Math.max(12, radius * 1.8);
	      let x = baseX;
	      let y = baseY;
	      let attempts = 0;
	      while (attempts < 24) {{
	        const conflict = placed.find((point) => Math.hypot(point.x - x, point.y - y) < point.r + minGap);
	        if (!conflict) return [x, y];
	        attempts += 1;
	        const angle = attempts * 1.9;
	        const offset = minGap * (0.45 + attempts / 10);
	        x = baseX + Math.cos(angle) * offset;
	        y = baseY + Math.sin(angle) * offset;
	      }}
	      return [x, y];
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
	        <tr><th>Business Fit Score</th><td>${{station.business_score ? (station.business_score.toFixed(2) + ' (higher = better)') : 'Low / neutral'}}</td></tr>
	        <tr><th>Coordinates</th><td>${{station.latitude.toFixed(4)}}, ${{station.longitude.toFixed(4)}}</td></tr>`;
	    }}
	    const placedStations = [];
	    stations.forEach((station, idx) => {{
	      const [baseX, baseY] = project(station.longitude, station.latitude);
	      const radius = Math.max(4, 3 + station.n_chargers_proposed / 2);
	      const [cx, cy] = spreadPoint(baseX, baseY, radius, placedStations);
	      placedStations.push({{x: cx, y: cy, r: radius}});
          const businessScore = Number(station.business_score || 0);
          if (maxBusinessScore > 0 && businessScore > 0) {{
            const businessShare = businessScore / maxBusinessScore;
            const haloRadius = radius + 3 + businessShare * 7;
            add('circle', {{
              cx,
              cy,
              r: haloRadius,
              fill: 'rgba(217, 164, 4, 0.08)',
              stroke: `rgba(217, 164, 4, ${{(0.18 + businessShare * 0.5).toFixed(2)}})`,
              'stroke-width': `${{(1.2 + businessShare * 1.6).toFixed(2)}}`,
            }}, map);
          }}
	      const circle = add('circle', {{
	        cx,
	        cy,
	        r: radius,
	        fill: colors[station.grid_status] || '#4b5563',
	        stroke: 'rgba(255,255,255,0.95)',
	        'stroke-width': '1.5',
	        opacity: '0.92',
	        tabindex: 0,
	      }}, map);
	      if (Math.hypot(cx - baseX, cy - baseY) > 1.5) {{
	        add('line', {{
	          x1: baseX,
	          y1: baseY,
	          x2: cx,
	          y2: cy,
	          stroke: 'rgba(31,41,55,0.18)',
	          'stroke-width': '0.8',
	          'stroke-dasharray': '2 2',
	        }}, map);
	      }}
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
    business_status: str,
    traffic_status: str,
    station_spacing_km: float,
) -> None:
    note = f"""# Submission Assumptions

- `File 1.csv`, `File 2.csv`, and `File 3.csv` were generated from the local RTIG dataset.
- Station placement uses a corridor-planning model that combines route span, strategic relevance, TEN-T status, baseline charging scarcity, and dynamic spacing around a `{station_spacing_km}` km reference.
- Existing-station baseline source status: `{baseline_status}`.
- EV projection source status: `{ev_status}`.
- Grid capacity source status: `{grid_status}`.
- Business-attractiveness proxy status: `{business_status}`.
- Traffic intensity source status: `{traffic_status}`.
- Existing charging baseline uses the official NAP-DGT/MITERD XML spatially matched to RTIG corridors within a 5 km threshold.
- EV demand uses the official datos.gob.es electrification exercise data and a SARIMA extension of the published notebook approach to estimate the 2027 EV stock proxy.
- Grid matching uses the nearest available published distributor demand-capacity nodes in `data/external/`, classifying locations as `Sufficient`, `Moderate`, or `Congested` based on whether available capacity is above 2x demand, between 1x and 2x demand, or below demand.
- Business attractiveness uses a conservative proxy based on nearby interurban charging-site metadata plus the official MITERD/Geoportal Gasolineras REST roadside-station inventory. Those roadside anchors receive additional weight when they sit near larger municipalities according to the official INE municipal-population API and in provinces with stronger official INE overnight-stay demand.
- Traffic intensity uses MITMA annual traffic-map segments (`IMD total` and `IMD pesados`) spatially matched to RTIG corridors to strengthen route prioritization, while influencing spacing conservatively so higher demand is absorbed first through stronger corridor ranking and charger sizing rather than an excessive proliferation of sites.
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
    business_status = "missing:existing_interurban_stations_matched.csv"
    traffic_status = "missing:mitma_traffic_by_route.csv"
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
    business_context, business_status = load_business_context(external_dir)
    traffic_by_route_path = external_dir / "mitma_traffic_by_route.csv"
    if traffic_by_route_path.exists():
        route_summary = enrich_route_summary_with_traffic(route_summary, pd.read_csv(traffic_by_route_path))
        traffic_status = f"loaded:{traffic_by_route_path.name}"
    route_summary = enrich_route_summary_with_baseline(route_summary, baseline_by_route)
    route_summary = enrich_route_summary_with_business(route_summary, business_context)
    route_summary = enrich_route_summary_for_planning(route_summary)

    file_2_initial = build_file_2(
        roads_gdf,
        route_summary,
        spacing_km=float(datathon_cfg["station_spacing_km"]),
        min_route_span_km=float(datathon_cfg["min_route_span_km"]),
        charger_policy="balanced",
        grid_policy="balanced",
        business_context=business_context,
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
    save_map(file_2_enriched, map_output, roads_gdf=roads_gdf)
    save_assumptions_note(
        output_dir,
        baseline_status=baseline_status,
        ev_status=ev_status,
        grid_status=grid_status,
        business_status=business_status,
        traffic_status=traffic_status,
        station_spacing_km=float(datathon_cfg["station_spacing_km"]),
    )

    print(f"Saved: {output_dir / 'File 1.csv'}")
    print(f"Saved: {output_dir / 'File 2.csv'}")
    print(f"Saved: {output_dir / 'File 3.csv'}")
    print(f"Saved: {map_output}")


if __name__ == "__main__":  # pragma: no cover
    main()
