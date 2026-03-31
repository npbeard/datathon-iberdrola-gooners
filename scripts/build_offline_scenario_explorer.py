"""Build a self-contained offline scenario explorer for the datathon package."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List

import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from scripts.generate_submission_package import (
    DEFAULT_INPUT,
    build_file_1,
    build_file_2,
    build_file_3,
    filter_interurban_routes,
    load_config,
    load_roads_dataset,
    summarize_routes,
)
from src.data.external_sources import (
    enrich_route_summary_with_baseline,
    enrich_stations_with_grid,
    load_grid_capacity_bundle,
)
OUTPUT_PATH = ROOT / "maps" / "offline_scenario_explorer.html"


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


def render_html(lines: List[List[List[float]]], scenarios: Dict[str, Dict], bounds: Dict[str, float]) -> str:
    lines_json = json.dumps(lines, ensure_ascii=False)
    scenarios_json = json.dumps(scenarios, ensure_ascii=False)
    bounds_json = json.dumps(bounds, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Offline Scenario Explorer</title>
  <style>
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
    label {{
      display: block;
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 6px;
    }}
    select, button {{
      width: 100%;
      border-radius: 12px;
      border: 1px solid rgba(31,42,55,0.15);
      padding: 10px 12px;
      font-size: 14px;
      background: white;
    }}
    button {{
      cursor: pointer;
      background: #f1f5f9;
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
      gap: 10px;
      margin-top: 16px;
    }}
    .note {{
      margin-top: 14px;
      font-size: 13px;
      color: var(--muted);
    }}
    @media (max-width: 1000px) {{
      .hero, .main, .kpis, .controls, .actions {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="card">
        <h1>Offline Scenario Explorer</h1>
        <p>This prototype is meant to show the jury how a planner at Iberdrola could compare a few rollout assumptions locally, without installing anything or depending on an internet connection.</p>
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
        <p>The point is not to show a huge web platform. The point is to let the jury test a few planning assumptions and immediately see how the network changes. This keeps the artifact simple and still gives Iberdrola something interactive.</p>
        <div class="actions">
          <button id="downloadFile1">Download File 1</button>
          <button id="downloadFile2">Download File 2</button>
          <button id="downloadFile3">Download File 3</button>
        </div>
        <p class="note">The current grid-status logic is still based on the provisional assumptions already declared in the repository. This explorer is intended as a decision-support prototype, not as a replacement for real node-level grid matching.</p>
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
        <h2>Top proposed locations</h2>
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
      </div>
    </div>
  </div>

  <script>
    const bounds = {bounds_json};
    const roadLines = {lines_json};
    const scenarios = {scenarios_json};
    const svg = document.getElementById('map');

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
      document.getElementById(id).addEventListener('change', renderScenario);
    }}

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

    lines = geometry_to_lines(roads_gdf)
    scenarios = build_scenarios(roads_gdf, route_summary, datathon_cfg)
    min_lon, min_lat, max_lon, max_lat = roads_gdf.total_bounds
    bounds = {
        "minLon": float(min_lon),
        "minLat": float(min_lat),
        "maxLon": float(max_lon),
        "maxLat": float(max_lat),
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(render_html(lines, scenarios, bounds), encoding="utf-8")
    print(f"Saved: {OUTPUT_PATH}")


if __name__ == "__main__":  # pragma: no cover
    main()
