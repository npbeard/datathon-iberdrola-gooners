"""Build a self-contained offline scenario explorer for the datathon package."""

from __future__ import annotations

import json
import math
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List

import geopandas as gpd
import pandas as pd
from shapely.ops import nearest_points

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from scripts.generate_submission_package import (
    DEFAULT_INPUT,
    build_file_1,
    build_file_2,
    build_file_3,
    enrich_route_summary_for_planning,
    enrich_route_summary_with_business,
    filter_interurban_routes,
    load_business_context,
    load_config,
    load_roads_dataset,
    summarize_routes,
)
from src.data.external_sources import (
    enrich_route_summary_with_baseline,
    enrich_route_summary_with_traffic,
    enrich_stations_with_grid,
    load_grid_capacity_bundle,
)
OUTPUT_PATH = ROOT / "maps" / "offline_scenario_explorer.html"

MUNICIPALITY_PATTERN = re.compile(r"Municipio: ([^|]+)")
PROVINCE_PATTERN = re.compile(r"Provincia: ([^|]+)")
VEHICLE_RANGE_LIBRARY = [
    {"model": "Tesla Model 3 RWD", "autonomy_km": 513},
    {"model": "Tesla Model 3 Long Range", "autonomy_km": 629},
    {"model": "Tesla Model Y RWD", "autonomy_km": 455},
    {"model": "Tesla Model Y Long Range", "autonomy_km": 533},
    {"model": "Tesla Model S", "autonomy_km": 634},
    {"model": "Tesla Model X", "autonomy_km": 576},
    {"model": "BYD Dolphin", "autonomy_km": 427},
    {"model": "BYD Atto 3", "autonomy_km": 420},
    {"model": "BYD Seal", "autonomy_km": 570},
    {"model": "MG4 Electric", "autonomy_km": 450},
    {"model": "MG ZS EV", "autonomy_km": 440},
    {"model": "Kia EV3", "autonomy_km": 560},
    {"model": "Kia EV6", "autonomy_km": 528},
    {"model": "Hyundai Kona Electric", "autonomy_km": 514},
    {"model": "Hyundai Ioniq 5", "autonomy_km": 507},
    {"model": "Hyundai Ioniq 6", "autonomy_km": 614},
    {"model": "Renault 5 E-Tech", "autonomy_km": 410},
    {"model": "Renault Megane E-Tech", "autonomy_km": 470},
    {"model": "Renault Scenic E-Tech", "autonomy_km": 625},
    {"model": "Volkswagen ID.3", "autonomy_km": 557},
    {"model": "Volkswagen ID.4", "autonomy_km": 520},
    {"model": "Skoda Enyaq", "autonomy_km": 565},
    {"model": "Cupra Born", "autonomy_km": 548},
    {"model": "BMW i4", "autonomy_km": 590},
    {"model": "BMW iX1", "autonomy_km": 474},
    {"model": "Mercedes EQE", "autonomy_km": 639},
    {"model": "Volvo EX30", "autonomy_km": 476},
    {"model": "Volvo EX40", "autonomy_km": 576},
    {"model": "Peugeot e-208", "autonomy_km": 433},
    {"model": "Peugeot e-3008", "autonomy_km": 525},
    {"model": "Opel Corsa Electric", "autonomy_km": 405},
    {"model": "Nissan Leaf", "autonomy_km": 385},
]


def geometry_to_lines(roads_gdf: gpd.GeoDataFrame) -> List[List[List[float]]]:
    simplified = roads_gdf[["geometry"]].copy()
    simplified["geometry"] = simplified.geometry.simplify(0.02, preserve_topology=False)

    lines: List[List[List[float]]] = []
    for geom in simplified.geometry:
        if geom is None or geom.is_empty:
            continue

        if geom.geom_type == "LineString":
            coords = [[round(x, 4), round(y, 4)] for x, y in geom.coords]
            lines.append(coords)
        elif geom.geom_type == "MultiLineString":
            for part in geom.geoms:  # pragma: no branch
                coords = [[round(x, 4), round(y, 4)] for x, y in part.coords]
                lines.append(coords)

    return lines


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFD", str(value or ""))
    normalized = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
    return re.sub(r"\s+", " ", normalized).strip().lower()


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    return 2 * radius_km * math.asin(math.sqrt(a))


def _segment_parts(geom) -> List[List[List[float]]]:
    if geom is None or geom.is_empty:
        return []
    if geom.geom_type == "LineString":
        return [[[round(x, 4), round(y, 4)] for x, y in geom.coords]]
    if geom.geom_type == "MultiLineString":
        return [
            [[round(x, 4), round(y, 4)] for x, y in part.coords]
            for part in geom.geoms
            if part is not None and not part.is_empty
        ]
    return []


def build_segment_graph(roads_gdf: gpd.GeoDataFrame) -> Dict[str, object]:
    roads = roads_gdf.reset_index(drop=True).copy()
    metric = roads.to_crs("EPSG:3857")
    tolerance_m = 1500.0

    segments: List[Dict[str, object]] = []
    adjacency: Dict[str, List[int]] = {}
    for idx, row in roads.iterrows():
        centroid = row.geometry.centroid if row.geometry is not None else None
        segment = {
            "id": int(idx),
            "route_segment": str(row["carretera"]),
            "length_km": round(float(row.get("length_km", 0.0)), 3),
            "centroid_lat": round(float(centroid.y), 6) if centroid else 0.0,
            "centroid_lon": round(float(centroid.x), 6) if centroid else 0.0,
            "parts": _segment_parts(row.geometry),
        }
        segments.append(segment)
        adjacency[str(idx)] = []

    sindex = metric.sindex
    for idx, geom in enumerate(metric.geometry):
        if geom is None or geom.is_empty:
            continue
        nearby = sindex.query(geom.buffer(tolerance_m), predicate="intersects")
        for neighbor_idx in nearby:
            neighbor_idx = int(neighbor_idx)
            if neighbor_idx <= idx:
                continue
            other = metric.geometry.iloc[neighbor_idx]
            if geom.distance(other) <= tolerance_m or geom.intersects(other):
                adjacency[str(idx)].append(neighbor_idx)
                adjacency[str(neighbor_idx)].append(idx)

    return {"segments": segments, "adjacency": adjacency}


def build_place_index(external_dir: Path, roads_gdf: gpd.GeoDataFrame) -> List[Dict[str, object]]:
    matched_path = external_dir / "existing_interurban_stations_matched.csv"
    if not matched_path.exists():
        return []

    stations = pd.read_csv(matched_path, usecols=["latitude", "longitude", "address_text"])
    stations = stations.dropna(subset=["latitude", "longitude"]).copy()
    if stations.empty:
        return []

    rows: List[Dict[str, object]] = []
    for station in stations.itertuples(index=False):
        address_text = str(getattr(station, "address_text", "") or "")
        municipality_match = MUNICIPALITY_PATTERN.search(address_text)
        if not municipality_match:
            continue
        province_match = PROVINCE_PATTERN.search(address_text)
        municipality = municipality_match.group(1).strip()
        province = province_match.group(1).strip() if province_match else ""
        display_name = municipality if not province else f"{municipality} ({province})"
        rows.append(
            {
                "municipality": municipality,
                "province": province,
                "display_name": display_name,
                "normalized_name": _normalize_text(municipality),
                "normalized_display": _normalize_text(display_name),
                "latitude": float(station.latitude),
                "longitude": float(station.longitude),
            }
        )

    if not rows:
        return []

    places = pd.DataFrame(rows)
    places = (
        places.groupby(["municipality", "province", "display_name", "normalized_name", "normalized_display"], as_index=False)
        .agg(latitude=("latitude", "median"), longitude=("longitude", "median"), anchor_count=("latitude", "size"))
        .sort_values(["anchor_count", "display_name"], ascending=[False, True])
        .reset_index(drop=True)
    )

    roads_metric = roads_gdf.reset_index(drop=True).to_crs("EPSG:3857")
    place_points = gpd.GeoDataFrame(
        places,
        geometry=gpd.points_from_xy(places["longitude"], places["latitude"]),
        crs="EPSG:4326",
    ).to_crs("EPSG:3857")
    nearest = gpd.sjoin_nearest(
        place_points,
        roads_metric[["carretera", "geometry"]].reset_index(names="segment_id"),
        how="left",
        distance_col="distance_to_segment_m",
    )
    nearest["nearest_segment_id"] = nearest["segment_id"].fillna(-1).astype(int)
    nearest["distance_to_segment_km"] = (nearest["distance_to_segment_m"] / 1000.0).round(2)
    snapped_lats: List[float] = []
    snapped_lons: List[float] = []
    for row in nearest.itertuples(index=False):
        segment_id = int(getattr(row, "nearest_segment_id", -1))
        if segment_id < 0:
            snapped_lats.append(float(row.latitude))
            snapped_lons.append(float(row.longitude))
            continue
        point_metric = row.geometry
        segment_metric = roads_metric.geometry.iloc[segment_id]
        snapped_metric = nearest_points(point_metric, segment_metric)[1]
        snapped_geo = gpd.GeoSeries([snapped_metric], crs="EPSG:3857").to_crs("EPSG:4326").iloc[0]
        snapped_lats.append(round(float(snapped_geo.y), 6))
        snapped_lons.append(round(float(snapped_geo.x), 6))
    nearest["snapped_latitude"] = snapped_lats
    nearest["snapped_longitude"] = snapped_lons

    return (
        nearest[
            [
                "display_name",
                "municipality",
                "province",
                "normalized_name",
                "normalized_display",
                "latitude",
                "longitude",
                "anchor_count",
                "nearest_segment_id",
                "distance_to_segment_km",
                "snapped_latitude",
                "snapped_longitude",
            ]
        ]
        .to_dict(orient="records")
    )


def build_scenarios(
    roads_gdf: gpd.GeoDataFrame,
    route_summary: pd.DataFrame,
    datathon_cfg: Dict,
) -> Dict[str, Dict]:
    scenarios: Dict[str, Dict] = {}

    spacing_options = [100, 120, 150]
    charger_policies = ["conservative", "balanced", "aggressive"]
    grid_policies = ["relaxed", "balanced", "cautious"]

    external_dir = ROOT / "data" / "external"
    baseline_existing_stations = int(datathon_cfg["baseline_existing_stations_default"])
    total_ev_projected_2027 = int(datathon_cfg["total_ev_projected_2027_default"])
    baseline_scalar = external_dir / "existing_interurban_stations.csv"
    ev_scalar = external_dir / "ev_projection_2027.csv"
    if baseline_scalar.exists():
        baseline_df = pd.read_csv(baseline_scalar)
        if not baseline_df.empty and "total_existing_stations_baseline" in baseline_df.columns:
            baseline_existing_stations = int(baseline_df["total_existing_stations_baseline"].iloc[0])
    if ev_scalar.exists():
        ev_df = pd.read_csv(ev_scalar)
        if not ev_df.empty and "total_ev_projected_2027" in ev_df.columns:
            total_ev_projected_2027 = int(ev_df["total_ev_projected_2027"].iloc[0])
    grid_nodes = load_grid_capacity_bundle(external_dir)
    business_context, _ = load_business_context(external_dir)
    traffic_by_route_path = external_dir / "mitma_traffic_by_route.csv"
    if traffic_by_route_path.exists():
        route_summary = enrich_route_summary_with_traffic(route_summary, pd.read_csv(traffic_by_route_path))
    route_summary = enrich_route_summary_with_business(route_summary, business_context)
    route_summary = enrich_route_summary_for_planning(route_summary)

    for spacing in spacing_options:
        for charger_policy in charger_policies:
            for grid_policy in grid_policies:
                key = f"{spacing}|{charger_policy}|{grid_policy}"
                file_2_raw = build_file_2(
                    roads_gdf,
                    route_summary,
                    spacing_km=spacing,
                    min_route_span_km=float(datathon_cfg["min_route_span_km"]),
                    charger_policy=charger_policy,
                    grid_policy=grid_policy,
                    business_context=business_context,
                )
                file_2_enriched = enrich_stations_with_grid(
                    file_2_raw,
                    grid_nodes=grid_nodes,
                    charger_power_kw=int(datathon_cfg["charger_power_kw"]),
                )
                file_2 = file_2_enriched[
                    ["location_id", "latitude", "longitude", "route_segment", "n_chargers_proposed", "grid_status"]
                ].copy()
                file_3 = build_file_3(file_2_enriched, charger_power_kw=int(datathon_cfg["charger_power_kw"]))
                file_1 = build_file_1(file_2, file_3, baseline_existing_stations, total_ev_projected_2027)

                scenarios[key] = {
                    "file1": file_1.to_dict(orient="records")[0],
                    "file2": file_2.to_dict(orient="records"),
                    "file3": file_3.to_dict(orient="records"),
                }

    return scenarios


def render_html(
    lines: List[List[List[float]]],
    scenarios: Dict[str, Dict],
    bounds: Dict[str, float],
    route_graph: Dict[str, object],
    places: List[Dict[str, object]],
) -> str:
    lines_json = json.dumps(lines, ensure_ascii=False)
    scenarios_json = json.dumps(scenarios, ensure_ascii=False)
    bounds_json = json.dumps(bounds, ensure_ascii=False)
    route_graph_json = json.dumps(route_graph, ensure_ascii=False)
    places_json = json.dumps(places, ensure_ascii=False)
    vehicle_range_json = json.dumps(VEHICLE_RANGE_LIBRARY, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Offline Scenario Explorer</title>
  <style>
    * {{
      box-sizing: border-box;
    }}
    :root {{
      --bg: #f5f1e8;
      --panel: #fffdf7;
      --ink: #1f2a37;
      --accent: #0f766e;
      --muted: #6b7280;
      --sufficient: #2e8b57;
      --moderate: #e0a100;
      --congested: #c2410c;
      --line: #9ca3af;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: linear-gradient(180deg, #f8f5ec 0%, #ece6d8 100%);
      color: var(--ink);
    }}
    .wrap {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 20px;
      margin-bottom: 20px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid rgba(31,42,55,0.12);
      border-radius: 18px;
      padding: 18px 20px;
      box-shadow: 0 18px 40px rgba(31,42,55,0.08);
    }}
    h1, h2, h3 {{
      margin: 0 0 8px 0;
      font-weight: 600;
    }}
    p {{
      margin: 0;
      line-height: 1.45;
    }}
    .controls {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 16px;
    }}
    .route-controls {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px 18px;
      margin-top: 20px;
      align-items: end;
    }}
    .route-controls > div {{
      min-width: 0;
    }}
    .route-field-origin,
    .route-field-destination {{
      grid-column: span 1;
    }}
    .route-field-vehicle {{
      grid-column: 1 / -1;
    }}
    .route-field-autonomy {{
      grid-column: span 1;
    }}
    .route-field-action {{
      grid-column: span 1;
    }}
    label {{
      display: block;
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 6px;
    }}
    select, input, button {{
      width: 100%;
      min-width: 0;
      max-width: 100%;
      border-radius: 12px;
      border: 1px solid rgba(31,42,55,0.15);
      padding: 10px 12px;
      font-size: 14px;
      background: white;
    }}
    button {{
      cursor: pointer;
      background: #f1f5f9;
      min-height: 48px;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 18px 0;
    }}
    .kpi {{
      background: rgba(15,118,110,0.06);
      border-radius: 14px;
      padding: 14px;
    }}
    .kpi .label {{
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .kpi .value {{
      font-size: 28px;
      margin-top: 4px;
    }}
    .main {{
      display: grid;
      grid-template-columns: 1.4fr 0.6fr;
      gap: 20px;
    }}
    svg {{
      width: 100%;
      height: auto;
      background: #fbfaf6;
      border-radius: 18px;
      border: 1px solid rgba(31,42,55,0.12);
    }}
    .legend {{
      display: flex;
      gap: 16px;
      margin-top: 10px;
      font-size: 13px;
      color: var(--muted);
      flex-wrap: wrap;
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
      margin-top: 10px;
    }}
    th, td {{
      padding: 8px 0;
      border-bottom: 1px solid rgba(31,42,55,0.08);
      text-align: left;
      vertical-align: top;
    }}
    .actions {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 26px;
    }}
    .section-label {{
      margin-top: 28px;
      margin-bottom: 10px;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }}
    .planner-metrics {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 24px 28px;
      margin-top: 0;
      margin-bottom: 6px;
      align-items: stretch;
    }}
    .planner-metric {{
      background: linear-gradient(180deg, rgba(15,118,110,0.05) 0%, rgba(15,118,110,0.02) 100%);
      border: 1px solid rgba(31,42,55,0.08);
      border-radius: 20px;
      padding: 22px 24px;
      min-height: 132px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      box-shadow: 0 10px 24px rgba(31,42,55,0.05);
    }}
    .planner-metric .label {{
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      line-height: 1.2;
      max-width: 14ch;
    }}
    .planner-metric .value {{
      margin-top: 14px;
      font-size: 28px;
      line-height: 1;
    }}
    .note {{
      margin-top: 14px;
      font-size: 13px;
      color: var(--muted);
    }}
    @media (max-width: 1000px) {{
      .hero, .main, .kpis, .controls, .actions, .route-controls, .planner-metrics {{
        grid-template-columns: 1fr;
      }}
      .route-field-origin,
      .route-field-destination,
      .route-field-vehicle,
      .route-field-autonomy,
      .route-field-action {{
        grid-column: auto;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="card">
        <h1>Offline Scenario Explorer</h1>
        <p>This prototype is meant to show how our team compares a few rollout assumptions locally, without installing anything or depending on an internet connection.</p>
        <div class="controls">
          <div>
            <label for="spacing">Station spacing</label>
            <select id="spacing">
              <option value="100">100 km</option>
              <option value="120" selected>120 km</option>
              <option value="150">150 km</option>
            </select>
          </div>
          <div>
            <label for="chargerPolicy">Charger policy</label>
            <select id="chargerPolicy">
              <option value="conservative">Conservative</option>
              <option value="balanced" selected>Balanced</option>
              <option value="aggressive">Aggressive</option>
            </select>
          </div>
          <div>
            <label for="gridPolicy">Grid policy</label>
            <select id="gridPolicy">
              <option value="relaxed">Relaxed</option>
              <option value="balanced" selected>Balanced</option>
              <option value="cautious">Cautious</option>
            </select>
          </div>
        </div>
        <div class="kpis">
          <div class="kpi"><div class="label">Proposed stations</div><div class="value" id="kpiStations">-</div></div>
          <div class="kpi"><div class="label">Friction points</div><div class="value" id="kpiFriction">-</div></div>
          <div class="kpi"><div class="label">Projected EVs 2027</div><div class="value" id="kpiEVs">-</div></div>
          <div class="kpi"><div class="label">Baseline stations</div><div class="value" id="kpiBaseline">-</div></div>
        </div>
      </div>
      <div class="card">
        <h2>Why this matters</h2>
        <p>The point is not to show a huge web platform. The point is to let someone explore a few planning assumptions and immediately see how the network changes. This keeps the artifact simple while still giving Iberdrola something interactive.</p>
        <div class="route-controls">
          <div class="route-field-origin">
            <label for="originInput">Origin city or town</label>
            <input id="originInput" list="placeOptions" placeholder="e.g. Madrid" />
          </div>
          <div class="route-field-destination">
            <label for="destinationInput">Destination city or town</label>
            <input id="destinationInput" list="placeOptions" placeholder="e.g. Valencia" />
          </div>
          <div class="route-field-vehicle">
            <label for="vehicleModelInput">Vehicle model</label>
            <input id="vehicleModelInput" list="vehicleOptions" placeholder="e.g. Tesla Model 3 Long Range" />
          </div>
          <div class="route-field-autonomy">
            <label for="autonomyInput">Autonomy (km)</label>
            <input id="autonomyInput" type="number" min="50" step="10" placeholder="e.g. 450" />
          </div>
          <div class="route-field-action">
            <label>&nbsp;</label>
            <button id="planRoute">Find best route</button>
          </div>
        </div>
        <datalist id="placeOptions"></datalist>
        <datalist id="vehicleOptions"></datalist>
        <p class="section-label">Trip summary</p>
        <div class="planner-metrics">
          <div class="planner-metric"><div class="label">Chosen route</div><div class="value" id="routeDistance">-</div></div>
          <div class="planner-metric"><div class="label">Vehicle range used</div><div class="value" id="routeRange">-</div></div>
          <div class="planner-metric"><div class="label">Charging support</div><div class="value" id="routeSupport">-</div></div>
          <div class="planner-metric"><div class="label">Suggested stops</div><div class="value" id="routeStops">-</div></div>
        </div>
        <div class="actions">
          <button id="downloadFile1">Download File 1</button>
          <button id="downloadFile2">Download File 2</button>
          <button id="downloadFile3">Download File 3</button>
        </div>
        <p class="note" id="routeMessage">Type two places and optionally add a car model and/or autonomy. If only the model is provided, the explorer estimates the autonomy and then recommends only the charging stops actually needed for the selected journey.</p>
      </div>
    </div>

    <div class="main">
      <div class="card">
        <h2>Spain corridor map</h2>
        <svg id="map" viewBox="0 0 1000 760" preserveAspectRatio="xMidYMid meet"></svg>
        <div class="legend">
          <span><span class="dot" style="background: var(--sufficient);"></span>Sufficient</span>
          <span><span class="dot" style="background: var(--moderate);"></span>Moderate</span>
          <span><span class="dot" style="background: var(--congested);"></span>Congested</span>
        </div>
      </div>
      <div class="card">
        <h2 id="tableTitle">Top proposed locations</h2>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Route</th>
              <th>Chargers</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody id="tableBody"></tbody>
        </table>
        <p class="note" id="tableNote">Before a route is selected, this table shows the most prominent proposed sites in the current scenario.</p>
      </div>
    </div>
  </div>

  <script>
    const bounds = {bounds_json};
    const roadLines = {lines_json};
    const scenarios = {scenarios_json};
    const routeGraph = {route_graph_json};
    const places = {places_json};
    const vehicleRangeLibrary = {vehicle_range_json};
    const svg = document.getElementById('map');
    const placeOptions = document.getElementById('placeOptions');
    placeOptions.innerHTML = places.slice(0, 2200).map(place => `<option value="${{place.display_name}}"></option>`).join('');
    const vehicleOptions = document.getElementById('vehicleOptions');
    vehicleOptions.innerHTML = vehicleRangeLibrary.map(vehicle => `<option value="${{vehicle.model}}"></option>`).join('');
    const vehicleRangeLookup = new Map(vehicleRangeLibrary.map(vehicle => [vehicle.model.toLowerCase(), vehicle]));
    const ROUTE_BUFFER_RATIO = 0.80;
    const MIN_STOP_SPACING_KM = 35;
    const STATION_MATCH_DISTANCE_KM = 22;

    function currentKey() {{
      return `${{document.getElementById('spacing').value}}|${{document.getElementById('chargerPolicy').value}}|${{document.getElementById('gridPolicy').value}}`;
    }}

    function project(lon, lat) {{
      const padX = 60;
      const padY = 40;
      const width = 880;
      const height = 680;
      const x = padX + ((lon - bounds.minLon) / (bounds.maxLon - bounds.minLon)) * width;
      const y = padY + (1 - (lat - bounds.minLat) / (bounds.maxLat - bounds.minLat)) * height;
      return [x, y];
    }}

    function drawBase() {{
      const fragments = [];
      fragments.push('<rect x="0" y="0" width="1000" height="760" fill="#fbfaf6" />');
      for (const line of roadLines) {{
        const path = line.map((coord, idx) => {{
          const [x, y] = project(coord[0], coord[1]);
          return `${{idx === 0 ? 'M' : 'L'}}${{x.toFixed(1)}},${{y.toFixed(1)}}`;
        }}).join(' ');
        fragments.push(`<path d="${{path}}" fill="none" stroke="rgba(156,163,175,0.35)" stroke-width="1.1" />`);
      }}
      svg.innerHTML = fragments.join('');
    }}

    function statusColor(status) {{
      if (status === 'Moderate') return '#e0a100';
      if (status === 'Congested') return '#c2410c';
      return '#2e8b57';
    }}

    function normalizeText(value) {{
      return (value || '')
        .normalize('NFD')
        .replace(/[\\u0300-\\u036f]/g, '')
        .replace(/\\s+/g, ' ')
        .trim()
        .toLowerCase();
    }}

    function haversineKm(lat1, lon1, lat2, lon2) {{
      const radius = 6371.0;
      const dLat = (lat2 - lat1) * Math.PI / 180;
      const dLon = (lon2 - lon1) * Math.PI / 180;
      const a = Math.sin(dLat / 2) ** 2
        + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
      return 2 * radius * Math.asin(Math.sqrt(a));
    }}

    function routeFamily(routeSegment) {{
      const match = String(routeSegment || '').match(/\\d+/g);
      return match ? match.join('') : String(routeSegment || '');
    }}

    function resolvePlace(query) {{
      const normalized = normalizeText(query);
      if (!normalized) return null;
      let exact = places.find(place => place.normalized_display === normalized || place.normalized_name === normalized);
      if (exact) return exact;
      let substring = places.find(place => place.normalized_display.includes(normalized) || normalized.includes(place.normalized_name));
      if (substring) return substring;
      let best = null;
      let bestScore = -1;
      for (const place of places) {{
        const score = longestCommonPrefix(normalized, place.normalized_display);
        if (score > bestScore) {{
          bestScore = score;
          best = place;
        }}
      }}
      return bestScore >= 3 ? best : null;
    }}

    function longestCommonPrefix(left, right) {{
      const limit = Math.min(left.length, right.length);
      let count = 0;
      while (count < limit && left[count] === right[count]) count += 1;
      return count;
    }}

    function resolveVehicleProfile() {{
      const modelInput = document.getElementById('vehicleModelInput').value.trim();
      const autonomyInput = Number(document.getElementById('autonomyInput').value);
      const hasAutonomy = Number.isFinite(autonomyInput) && autonomyInput > 0;
      let matchedVehicle = null;

      if (modelInput) {{
        const normalized = modelInput.toLowerCase();
        matchedVehicle = vehicleRangeLookup.get(normalized) || vehicleRangeLibrary.find(vehicle =>
          vehicle.model.toLowerCase() === normalized || vehicle.model.toLowerCase().includes(normalized),
        ) || null;
      }}

      if (!hasAutonomy && !matchedVehicle) {{
        return null;
      }}

      const autonomyKm = hasAutonomy ? autonomyInput : matchedVehicle.autonomy_km;
      if (!Number.isFinite(autonomyKm) || autonomyKm <= 0) return null;
      return {{
        model: matchedVehicle ? matchedVehicle.model : (modelInput || 'Custom EV'),
        autonomyKm,
        source: hasAutonomy ? (matchedVehicle ? 'manual-with-model' : 'manual') : 'estimated-from-model',
        planningRangeKm: autonomyKm * ROUTE_BUFFER_RATIO,
      }};
    }}

    const segmentIds = routeGraph.segments.map(segment => Number(segment.id));
    const segmentById = new Map(routeGraph.segments.map(segment => [Number(segment.id), segment]));
    const adjacencyById = new Map(
      Object.entries(routeGraph.adjacency).map(([segmentId, neighbors]) => [
        Number(segmentId),
        (neighbors || []).map(neighbor => Number(neighbor)),
      ]),
    );
    const scenarioSupportCache = {{}};

    function segmentSupportMap(scenario) {{
      const cacheKey = currentKey();
      if (scenarioSupportCache[cacheKey]) return scenarioSupportCache[cacheKey];

      const support = {{}};
      for (const segment of routeGraph.segments) {{
        let best = 0;
        for (const station of scenario.file2) {{
          const distance = haversineKm(segment.centroid_lat, segment.centroid_lon, station.latitude, station.longitude);
          if (distance > 35) continue;
          const routeBoost = routeFamily(segment.route_segment) === routeFamily(station.route_segment) ? 1.15 : 1.0;
          const gridWeight = station.grid_status === 'Sufficient' ? 1.0 : station.grid_status === 'Moderate' ? 0.78 : 0.55;
          const chargerWeight = Math.min(1.6, 0.7 + Number(station.n_chargers_proposed || 0) / 8);
          const proximityWeight = distance <= 12 ? 1.0 : distance <= 20 ? 0.7 : 0.45;
          best = Math.max(best, routeBoost * gridWeight * chargerWeight * proximityWeight);
        }}
        support[Number(segment.id)] = Number(best.toFixed(3));
      }}
      scenarioSupportCache[cacheKey] = support;
      return support;
    }}

    function segmentCost(segment, supportValue, mode='balanced') {{
      if (mode === 'distance') return segment.length_km;
      let factor = 1.03;
      if (supportValue >= 1.1) factor = 0.94;
      else if (supportValue >= 0.85) factor = 0.97;
      else if (supportValue >= 0.6) factor = 1.0;
      if (segment.length_km >= 45 && supportValue < 0.6) factor += 0.06;
      return segment.length_km * factor;
    }}

    function shortestPath(startId, endId, supportMap, mode='balanced') {{
      const normalizedStart = Number(startId);
      const normalizedEnd = Number(endId);
      const distances = new Map();
      const previous = new Map();
      const visited = new Set();
      for (const segmentId of segmentIds) distances.set(segmentId, Infinity);
      distances.set(normalizedStart, 0);

      while (true) {{
        let current = null;
        let bestDistance = Infinity;
        for (const segmentId of segmentIds) {{
          const candidateDistance = distances.get(segmentId);
          if (!visited.has(segmentId) && candidateDistance < bestDistance) {{
            bestDistance = candidateDistance;
            current = segmentId;
          }}
        }}
        if (current === null || current === normalizedEnd) break;
        visited.add(current);
        for (const neighbor of adjacencyById.get(current) || []) {{
          if (visited.has(neighbor)) continue;
          const neighborSegment = segmentById.get(neighbor);
          if (!neighborSegment) continue;
          const alt = distances.get(current) + segmentCost(neighborSegment, supportMap[neighbor] || 0, mode);
          if (alt < distances.get(neighbor)) {{
            distances.set(neighbor, alt);
            previous.set(neighbor, current);
          }}
        }}
      }}

      if (!Number.isFinite(distances.get(normalizedEnd))) return null;
      const path = [];
      let cursor = normalizedEnd;
      while (cursor !== undefined) {{
        path.push(cursor);
        cursor = previous.get(cursor);
      }}
      path.reverse();
      return path[0] === normalizedStart ? path : null;
    }}

    function pathDistanceKm(pathIds) {{
      return pathIds.reduce((sum, id) => sum + Number((segmentById.get(Number(id)) || {{}}).length_km || 0), 0);
    }}

    function pathSegmentsWithProgress(pathIds) {{
      let cumulativeKm = 0;
      return pathIds.map(id => {{
        const segment = segmentById.get(Number(id));
        const lengthKm = Number((segment || {{}}).length_km || 0);
        const row = {{
          id: Number(id),
          segment,
          startKm: cumulativeKm,
          midKm: cumulativeKm + lengthKm / 2,
          endKm: cumulativeKm + lengthKm,
          lengthKm,
        }};
        cumulativeKm += lengthKm;
        return row;
      }}).filter(row => row.segment);
    }}

    function candidateSegmentIds(place, maxCandidates=6, maxDistanceKm=90) {{
      const ranked = routeGraph.segments
        .map(segment => ({{
          id: segment.id,
          distanceKm: haversineKm(place.latitude, place.longitude, segment.centroid_lat, segment.centroid_lon),
        }}))
        .filter(item => Number.isFinite(item.distanceKm))
        .sort((a, b) => a.distanceKm - b.distanceKm);
      const nearby = ranked.filter(item => item.distanceKm <= maxDistanceKm).slice(0, maxCandidates);
      if (nearby.length) return nearby;
      return ranked.slice(0, Math.min(maxCandidates, ranked.length));
    }}

    function bestCorridorPath(originPlace, destinationPlace, supportMap) {{
      const originCandidates = candidateSegmentIds(originPlace);
      const destinationCandidates = candidateSegmentIds(destinationPlace);
      let best = null;
      let bestDistanceOnly = null;
      const directDistanceKm = haversineKm(
        originPlace.latitude,
        originPlace.longitude,
        destinationPlace.latitude,
        destinationPlace.longitude,
      );
      const maxReasonableDistanceKm = Math.max(directDistanceKm * 2.15, directDistanceKm + 250);

      for (const originCandidate of originCandidates) {{
        for (const destinationCandidate of destinationCandidates) {{
          const distancePath = shortestPath(originCandidate.id, destinationCandidate.id, supportMap, 'distance');
          if (!distancePath || !distancePath.length) continue;
          const supportPath = shortestPath(originCandidate.id, destinationCandidate.id, supportMap, 'balanced');
          const distanceKm = pathDistanceKm(distancePath);
          const supportDistanceKm = supportPath && supportPath.length ? pathDistanceKm(supportPath) : Infinity;
          const distanceSlackKm = Math.max(35, distanceKm * 0.12);
          const chosenPath = supportDistanceKm <= distanceKm + distanceSlackKm ? supportPath : distancePath;
          const chosenDistanceKm = pathDistanceKm(chosenPath);
          const accessKm = originCandidate.distanceKm + destinationCandidate.distanceKm;
          const score = chosenDistanceKm + accessKm * 1.35;
          const distanceOnlyScore = distanceKm + accessKm * 1.35;
          const distanceOnlyCandidate = {{
            pathIds: distancePath,
            totalDistanceKm: distanceKm,
            accessDistanceKm: accessKm,
            originSegmentId: originCandidate.id,
            destinationSegmentId: destinationCandidate.id,
            mode: 'distance-first corridor routing',
          }};
          if (!bestDistanceOnly || distanceOnlyScore < bestDistanceOnly.score) {{
            bestDistanceOnly = {{ score: distanceOnlyScore, ...distanceOnlyCandidate }};
          }}
          if (!best || score < best.score) {{
            best = {{
              pathIds: chosenPath,
              totalDistanceKm: chosenDistanceKm,
              accessDistanceKm: accessKm,
              originSegmentId: originCandidate.id,
              destinationSegmentId: destinationCandidate.id,
              mode: chosenPath === distancePath ? 'distance-first corridor routing' : 'distance-first corridor routing with charging-support tie-breaking',
            }};
          }}
        }}
      }}

      if (best && best.totalDistanceKm > maxReasonableDistanceKm && bestDistanceOnly) {{
        return {{
          ...bestDistanceOnly,
          mode: 'distance-first corridor routing (sanity fallback)',
        }};
      }}
      return best;
    }}

    function stationsAlongPath(pathIds, scenario) {{
      const touched = pathSegmentsWithProgress(pathIds);
      const seen = new Set();
      const matches = [];

      for (const station of scenario.file2) {{
        let bestMatch = null;
        for (const touchedSegment of touched) {{
          const segment = touchedSegment.segment;
          const distanceKm = haversineKm(segment.centroid_lat, segment.centroid_lon, station.latitude, station.longitude);
          const routeMatches = routeFamily(segment.route_segment) === routeFamily(station.route_segment);
          if (!routeMatches && distanceKm > STATION_MATCH_DISTANCE_KM) continue;
          const candidate = {{
            progressKm: touchedSegment.midKm,
            corridorDistanceKm: distanceKm,
          }};
          if (!bestMatch || candidate.corridorDistanceKm < bestMatch.corridorDistanceKm) {{
            bestMatch = candidate;
          }}
        }}
        if (!bestMatch || seen.has(station.location_id)) continue;
        seen.add(station.location_id);
        matches.push({{
          ...station,
          progressKm: bestMatch.progressKm,
          corridorDistanceKm: bestMatch.corridorDistanceKm,
        }});
      }}

      return matches.sort((left, right) => {{
        if (left.progressKm !== right.progressKm) return left.progressKm - right.progressKm;
        if (left.corridorDistanceKm !== right.corridorDistanceKm) return left.corridorDistanceKm - right.corridorDistanceKm;
        return Number(right.n_chargers_proposed) - Number(left.n_chargers_proposed);
      }});
    }}

    function stationPriorityScore(station) {{
      const gridScore = station.grid_status === 'Sufficient' ? 3 : station.grid_status === 'Moderate' ? 2 : 1;
      return gridScore * 1000 + Number(station.n_chargers_proposed || 0) * 10 - Number(station.corridorDistanceKm || 0);
    }}

    function suggestStops(pathIds, scenario, vehicleProfile) {{
      const totalDistanceKm = pathDistanceKm(pathIds);
      if (!vehicleProfile) return [];

      const planningRangeKm = Number(vehicleProfile.planningRangeKm || 0);
      if (!Number.isFinite(planningRangeKm) || planningRangeKm <= 0) return [];
      if (totalDistanceKm <= planningRangeKm) return [];

      const stations = stationsAlongPath(pathIds, scenario);
      const chosenStops = [];
      let currentKm = 0;

      while (currentKm + planningRangeKm < totalDistanceKm) {{
        const reachable = stations.filter(station =>
          station.progressKm > currentKm + MIN_STOP_SPACING_KM &&
          station.progressKm <= currentKm + planningRangeKm,
        );
        if (!reachable.length) {{
          return null;
        }}

        reachable.sort((left, right) => {{
          if (right.progressKm !== left.progressKm) return right.progressKm - left.progressKm;
          return stationPriorityScore(right) - stationPriorityScore(left);
        }});

        const nextStop = reachable[0];
        chosenStops.push(nextStop);
        currentKm = nextStop.progressKm;
      }}

      return chosenStops;
    }}

    function drawRouteOverlay(pathIds, originPlace, destinationPlace, scenario, routeStops) {{
      const fragments = [];
      for (const id of pathIds) {{
        const segment = segmentById.get(Number(id));
        if (!segment) continue;
        for (const part of segment.parts) {{
          const path = part.map((coord, idx) => {{
            const [x, y] = project(coord[0], coord[1]);
            return `${{idx === 0 ? 'M' : 'L'}}${{x.toFixed(1)}},${{y.toFixed(1)}}`;
          }}).join(' ');
          fragments.push(`<path d="${{path}}" fill="none" stroke="#0f766e" stroke-width="4.4" stroke-linecap="round" stroke-linejoin="round" opacity="0.92"></path>`);
        }}
      }}
      for (const stop of routeStops || []) {{
        const [x, y] = project(stop.longitude, stop.latitude);
        fragments.push(`<circle cx="${{x.toFixed(1)}}" cy="${{y.toFixed(1)}}" r="6.6" fill="${{statusColor(stop.grid_status)}}" stroke="white" stroke-width="2"></circle>`);
      }}
      const [ox, oy] = project(originPlace.longitude, originPlace.latitude);
      const [dx, dy] = project(destinationPlace.longitude, destinationPlace.latitude);
      const originSegment = segmentById.get(Number(pathIds[0]));
      const destinationSegment = segmentById.get(Number(pathIds[pathIds.length - 1]));
      const [osx, osy] = project(originSegment.centroid_lon, originSegment.centroid_lat);
      const [dsx, dsy] = project(destinationSegment.centroid_lon, destinationSegment.centroid_lat);
      fragments.push(`<line x1="${{ox.toFixed(1)}}" y1="${{oy.toFixed(1)}}" x2="${{osx.toFixed(1)}}" y2="${{osy.toFixed(1)}}" stroke="#1d4ed8" stroke-width="2" stroke-dasharray="6 4" opacity="0.7"></line>`);
      fragments.push(`<line x1="${{dx.toFixed(1)}}" y1="${{dy.toFixed(1)}}" x2="${{dsx.toFixed(1)}}" y2="${{dsy.toFixed(1)}}" stroke="#7c3aed" stroke-width="2" stroke-dasharray="6 4" opacity="0.7"></line>`);
      fragments.push(`<circle cx="${{osx.toFixed(1)}}" cy="${{osy.toFixed(1)}}" r="4.5" fill="#1d4ed8" fill-opacity="0.18" stroke="#1d4ed8" stroke-width="1.6"></circle>`);
      fragments.push(`<circle cx="${{dsx.toFixed(1)}}" cy="${{dsy.toFixed(1)}}" r="4.5" fill="#7c3aed" fill-opacity="0.18" stroke="#7c3aed" stroke-width="1.6"></circle>`);
      fragments.push(`<circle cx="${{ox.toFixed(1)}}" cy="${{oy.toFixed(1)}}" r="7" fill="#1d4ed8" stroke="white" stroke-width="2"></circle>`);
      fragments.push(`<circle cx="${{dx.toFixed(1)}}" cy="${{dy.toFixed(1)}}" r="7" fill="#7c3aed" stroke="white" stroke-width="2"></circle>`);
      svg.innerHTML += fragments.join('');
    }}

    function planRoute() {{
      const origin = resolvePlace(document.getElementById('originInput').value);
      const destination = resolvePlace(document.getElementById('destinationInput').value);
      if (!origin || !destination || origin.nearest_segment_id < 0 || destination.nearest_segment_id < 0) {{
        document.getElementById('routeMessage').textContent = 'Please choose two known municipalities from the suggestions. The planner uses a local municipality index built from the interurban baseline.';
        return;
      }}
      const scenario = scenarios[currentKey()];
      const supportMap = segmentSupportMap(scenario);
      const routeChoice = bestCorridorPath(origin, destination, supportMap);
      if (!routeChoice || !routeChoice.pathIds || !routeChoice.pathIds.length) {{
        document.getElementById('routeMessage').textContent = `No corridor path was found between ${{origin.display_name}} and ${{destination.display_name}} in the local RTIG graph.`;
        return;
      }}
      const vehicleProfile = resolveVehicleProfile();
      if (!vehicleProfile) {{
        document.getElementById('routeMessage').textContent = 'Please provide a vehicle model, an autonomy value, or both. If only the model is provided, the explorer estimates the autonomy automatically.';
        return;
      }}
      const pathIds = routeChoice.pathIds;
      const routeStops = suggestStops(pathIds, scenario, vehicleProfile);
      renderScenario();
      if (routeStops === null) {{
        document.getElementById('routeDistance').textContent = `${{Math.round(routeChoice.totalDistanceKm)}} km`;
        document.getElementById('routeRange').textContent = `${{Math.round(vehicleProfile.autonomyKm)}} km`;
        document.getElementById('routeSupport').textContent = 'Insufficient';
        document.getElementById('routeStops').textContent = '0';
        document.getElementById('tableTitle').textContent = 'Charging stops needed on this route';
        document.getElementById('tableBody').innerHTML = '<tr><td colspan="4">No feasible stop sequence was found within the usable vehicle range on the selected corridor path.</td></tr>';
        document.getElementById('tableNote').textContent = 'Try a longer-range vehicle, increase the autonomy value, or explore a different scenario.';
        document.getElementById('routeMessage').textContent = `A route was found from ${{origin.display_name}} to ${{destination.display_name}}, but a vehicle with ${{Math.round(vehicleProfile.autonomyKm)}} km of autonomy does not find enough charging support within the explorer's usable planning range of roughly ${{Math.round(vehicleProfile.planningRangeKm)}} km per leg.`;
        return;
      }}
      drawRouteOverlay(pathIds, origin, destination, scenario, routeStops);
      const totalDistance = routeChoice.totalDistanceKm;
      const avgSupport = pathIds.reduce((sum, id) => sum + Number(supportMap[id] || 0), 0) / pathIds.length;
      const accessDistance = routeChoice.accessDistanceKm;
      const routeRows = routeStops.map(stop => '<tr><td>' + stop.location_id + '</td><td>' + stop.route_segment + '</td><td>' + stop.n_chargers_proposed + ' chargers · ' + Math.round(stop.progressKm) + ' km</td><td>' + stop.grid_status + '</td></tr>').join('');
      document.getElementById('routeDistance').textContent = `${{Math.round(totalDistance)}} km`;
      document.getElementById('routeRange').textContent = `${{Math.round(vehicleProfile.autonomyKm)}} km`;
      document.getElementById('routeSupport').textContent = avgSupport >= 1.0 ? 'High' : avgSupport >= 0.7 ? 'Medium' : 'Low';
      document.getElementById('routeStops').textContent = routeStops.length.toString();
      document.getElementById('tableTitle').textContent = 'Charging stops needed on this route';
      document.getElementById('tableBody').innerHTML = routeRows || '<tr><td colspan="4">This trip is feasible without intermediate charging under the current range assumption.</td></tr>';
      document.getElementById('tableNote').textContent = 'These are the specific planned stops needed for this trip given the selected vehicle range, not just the strongest stations near the corridor.';
      const autonomySource = vehicleProfile.source === 'estimated-from-model'
        ? `using the estimated range for ${{vehicleProfile.model}}`
        : vehicleProfile.source === 'manual-with-model'
          ? `using the manual autonomy override for ${{vehicleProfile.model}}`
          : 'using the manual autonomy input';
      document.getElementById('routeMessage').textContent = `Best route from ${{origin.display_name}} to ${{destination.display_name}} selected using ${{routeChoice.mode}}. The explorer then filtered the corridor to only the charging stops actually needed ${{autonomySource}}. Dashed access legs connect each municipality to the chosen RTIG corridor entry point (combined access distance: ${{accessDistance.toFixed(1)}} km).`;
    }}

    function renderScenario() {{
      drawBase();
      const scenario = scenarios[currentKey()];
      const baseMarkup = svg.innerHTML;
      const stationMarkup = scenario.file2.map(row => {{
        const [x, y] = project(row.longitude, row.latitude);
        const fill = statusColor(row.grid_status);
        return `<g><circle cx="${{x.toFixed(1)}}" cy="${{y.toFixed(1)}}" r="4.5" fill="${{fill}}" fill-opacity="0.9" stroke="white" stroke-width="1.2"></circle><title>${{row.location_id}} | ${{row.route_segment}} | ${{row.n_chargers_proposed}} chargers | ${{row.grid_status}}</title></g>`;
      }}).join('');
      svg.innerHTML = baseMarkup + stationMarkup;

      const file1 = scenario.file1;
      document.getElementById('kpiStations').textContent = file1.total_proposed_stations.toLocaleString();
      document.getElementById('kpiFriction').textContent = file1.total_friction_points.toLocaleString();
      document.getElementById('kpiEVs').textContent = file1.total_ev_projected_2027.toLocaleString();
      document.getElementById('kpiBaseline').textContent = file1.total_existing_stations_baseline.toLocaleString();

      const rows = scenario.file2.slice(0, 12).map(row => `<tr><td>${{row.location_id}}</td><td>${{row.route_segment}}</td><td>${{row.n_chargers_proposed}}</td><td>${{row.grid_status}}</td></tr>`).join('');
      document.getElementById('tableBody').innerHTML = rows;
      document.getElementById('tableTitle').textContent = 'Top proposed locations';
      document.getElementById('tableNote').textContent = 'Before a route is selected, this table shows the most prominent proposed sites in the current scenario.';
      document.getElementById('routeRange').textContent = '-';
    }}

    function toCsv(rows) {{
      if (!rows.length) return '';
      const headers = Object.keys(rows[0]);
      const lines = [
        headers.join(','),
        ...rows.map(row => headers.map(h => JSON.stringify(row[h] ?? '')).join(','))
      ];
      return lines.join('\\n');
    }}

    function download(name, rows) {{
      const blob = new Blob([toCsv(rows)], {{ type: 'text/csv;charset=utf-8;' }});
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = name;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }}

    document.getElementById('downloadFile1').addEventListener('click', () => {{
      download('File 1.csv', [scenarios[currentKey()].file1]);
    }});
    document.getElementById('downloadFile2').addEventListener('click', () => {{
      download('File 2.csv', scenarios[currentKey()].file2);
    }});
    document.getElementById('downloadFile3').addEventListener('click', () => {{
      download('File 3.csv', scenarios[currentKey()].file3);
    }});

    for (const id of ['spacing', 'chargerPolicy', 'gridPolicy']) {{
      document.getElementById(id).addEventListener('change', () => {{
        renderScenario();
      }});
    }}
    document.getElementById('planRoute').addEventListener('click', planRoute);

    renderScenario();
  </script>
</body>
</html>
"""


def main() -> None:
    config = load_config()
    datathon_cfg = config["datathon"]

    roads_gdf = load_roads_dataset(DEFAULT_INPUT)
    roads_gdf = filter_interurban_routes(roads_gdf)
    route_summary = summarize_routes(roads_gdf)
    baseline_by_route_path = ROOT / "data" / "external" / "existing_interurban_stations_by_route.csv"
    if baseline_by_route_path.exists():
        route_summary = enrich_route_summary_with_baseline(route_summary, pd.read_csv(baseline_by_route_path))
    traffic_by_route_path = ROOT / "data" / "external" / "mitma_traffic_by_route.csv"
    if traffic_by_route_path.exists():
        route_summary = enrich_route_summary_with_traffic(route_summary, pd.read_csv(traffic_by_route_path))

    lines = geometry_to_lines(roads_gdf)
    scenarios = build_scenarios(roads_gdf, route_summary, datathon_cfg)
    route_graph = build_segment_graph(roads_gdf)
    places = build_place_index(ROOT / "data" / "external", roads_gdf)
    min_lon, min_lat, max_lon, max_lat = roads_gdf.total_bounds
    bounds = {
        "minLon": float(min_lon),
        "minLat": float(min_lat),
        "maxLon": float(max_lon),
        "maxLat": float(max_lat),
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(render_html(lines, scenarios, bounds, route_graph, places), encoding="utf-8")
    print(f"Saved: {OUTPUT_PATH}")


if __name__ == "__main__":  # pragma: no cover
    main()
