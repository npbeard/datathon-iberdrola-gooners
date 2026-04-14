"""Build polished offline-safe reference maps for local browser use."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List

import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

PROCESSED_GDF_PATH = ROOT / "data" / "processed" / "roads_processed_gdf.parquet"
SCORED_GDF_PATH = ROOT / "data" / "processed" / "roads_scored_final.parquet"
MAPS_DIR = ROOT / "maps"

PRIORITY_COLORS = {
    "High": "#c4571a",
    "Medium": "#d9a441",
    "Low": "#8397ac",
}


def load_datasets() -> gpd.GeoDataFrame:
    roads = gpd.read_parquet(PROCESSED_GDF_PATH)
    scored = gpd.read_parquet(SCORED_GDF_PATH)

    if roads.crs is None:
        roads = roads.set_crs("EPSG:4326")
    elif roads.crs.to_string() != "EPSG:4326":
        roads = roads.to_crs("EPSG:4326")

    merged = roads.reset_index(drop=True).copy()
    for column in ["priority_score", "priority_level"]:
        if column in scored.columns:
            merged[column] = scored.reset_index(drop=True)[column]

    if "priority_level" not in merged.columns:
        merged["priority_level"] = "Low"
    if "priority_score" not in merged.columns:
        merged["priority_score"] = 0.0

    return merged


def geometry_to_features(roads: gpd.GeoDataFrame, color_mode: str) -> List[Dict]:
    features: List[Dict] = []
    for row in roads.itertuples(index=False):
        geometry = row.geometry
        if geometry is None or geometry.is_empty:
            continue

        parts = [geometry] if geometry.geom_type == "LineString" else list(getattr(geometry, "geoms", []))
        color = "#92a4b6" if color_mode == "base" else PRIORITY_COLORS.get(str(row.priority_level), "#8397ac")

        for part in parts:
            coords = [[round(x, 4), round(y, 4)] for x, y in part.coords]
            features.append(
                {
                    "coords": coords,
                    "color": color,
                    "carretera": str(getattr(row, "carretera", "Unknown")),
                    "length_km": round(float(getattr(row, "length_km", 0.0)), 2),
                    "priority_level": str(getattr(row, "priority_level", "Low")),
                    "priority_score": round(float(getattr(row, "priority_score", 0.0)), 2),
                    "is_tent": int(getattr(row, "is_tent", 0)),
                }
            )
    return features


def centroid_points(roads: gpd.GeoDataFrame) -> List[Dict]:
    points: List[Dict] = []
    for row in roads.itertuples(index=False):
        lat = getattr(row, "center_lat", None)
        lon = getattr(row, "center_lon", None)
        if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
            geometry = row.geometry
            if geometry is None or geometry.is_empty:
                continue
            centroid = geometry.centroid
            lon = centroid.x
            lat = centroid.y
        points.append(
            {
                "lat": round(float(lat), 4),
                "lon": round(float(lon), 4),
                "weight": round(float(getattr(row, "length_km", 1.0)), 2),
                "carretera": str(getattr(row, "carretera", "Unknown")),
                "length_km": round(float(getattr(row, "length_km", 0.0)), 2),
            }
        )
    return points


def bounds_from_features(features: List[Dict]) -> Dict[str, float]:
    lons = [lon for feature in features for lon, _ in feature["coords"]]
    lats = [lat for feature in features for _, lat in feature["coords"]]
    return {
        "minLon": min(lons) - 0.3,
        "maxLon": max(lons) + 0.3,
        "minLat": min(lats) - 0.3,
        "maxLat": max(lats) + 0.3,
    }


def backdrop_features(features: List[Dict]) -> List[List[List[float]]]:
    sampled = features[:: max(1, len(features) // 420)]
    return [feature["coords"] for feature in sampled if feature["coords"]]


def build_summary(roads: gpd.GeoDataFrame) -> Dict[str, object]:
    return {
        "segments": int(len(roads)),
        "network_km": int(round(float(roads["length_km"].sum()))),
        "tent_share": round(float(roads["is_tent"].mean() * 100.0), 1),
        "high_priority": int((roads["priority_level"] == "High").sum()),
    }


def base_styles() -> str:
    return """
    :root {
      --bg: #efe4cf;
      --panel: rgba(255, 250, 242, 0.95);
      --ink: #243040;
      --muted: #6c7a88;
      --frame: rgba(36, 48, 64, 0.10);
      --sea: #f7f1e5;
      --land: #efe3cf;
      --land-edge: #d9c6ab;
      --road: #8ea3b5;
      --high: #c4571a;
      --medium: #d9a441;
      --low: #8397ac;
      --cool: #97c9d8;
      --warm: #e7b24d;
      --hot: #c4571a;
    }
    body {
      margin: 0;
      background:
        radial-gradient(circle at 20% 0%, rgba(255,255,255,0.7), transparent 32%),
        linear-gradient(180deg, #f4ebdc 0%, #e6d7c0 100%);
      color: var(--ink);
      font-family: Georgia, "Times New Roman", serif;
    }
    .wrap {
      max-width: 1460px;
      margin: 0 auto;
      padding: 24px;
    }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(320px, 0.65fr);
      gap: 20px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--frame);
      border-radius: 22px;
      box-shadow: 0 24px 60px rgba(36, 48, 64, 0.10);
      padding: 22px 24px;
      backdrop-filter: blur(8px);
    }
    .eyebrow {
      margin: 0 0 6px 0;
      color: var(--muted);
      font-size: 13px;
      letter-spacing: 0.10em;
      text-transform: uppercase;
    }
    h1, h2 {
      margin: 0 0 10px 0;
      font-weight: 600;
      line-height: 1.05;
    }
    h1 {
      font-size: 2.05rem;
    }
    p {
      margin: 0;
      line-height: 1.45;
      color: #314154;
    }
    .kpis {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 18px 0 14px 0;
    }
    .kpi {
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(255,255,255,0.55);
      border: 1px solid rgba(36,48,64,0.08);
    }
    .kpi .label {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }
    .kpi .value {
      font-size: 1.75rem;
      margin-top: 4px;
      color: var(--ink);
    }
    svg {
      width: 100%;
      height: auto;
      background:
        radial-gradient(circle at 20% 20%, rgba(255,255,255,0.45), transparent 38%),
        linear-gradient(180deg, #fbf6ee 0%, #f5ede1 100%);
      border-radius: 20px;
      border: 1px solid rgba(36,48,64,0.10);
    }
    .legend {
      display: flex;
      gap: 16px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 13px;
      margin-top: 12px;
    }
    .dot, .band {
      display: inline-block;
      margin-right: 7px;
      vertical-align: middle;
    }
    .dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
    }
    .band {
      width: 24px;
      height: 10px;
      border-radius: 999px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
      margin-top: 6px;
    }
    th, td {
      text-align: left;
      padding: 10px 0;
      border-bottom: 1px solid rgba(36,48,64,0.08);
      vertical-align: top;
    }
    th {
      width: 42%;
      color: #2c3a4b;
      font-weight: 600;
    }
    .note {
      margin-top: 14px;
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 980px) {
      .layout {
        grid-template-columns: 1fr;
      }
      .kpis {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
    """


def render_shell(title: str, subtitle: str, summary: Dict[str, object], sidebar_title: str, body_rows: str, note: str, legend_html: str, script: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>{base_styles()}</style>
</head>
<body>
  <div class="wrap">
    <div class="layout">
      <section class="card">
        <p class="eyebrow">Offline Reference Map</p>
        <h1>{title}</h1>
        <p>{subtitle}</p>
        <div class="kpis">
          <div class="kpi"><div class="label">Segments</div><div class="value">{summary["segments"]:,}</div></div>
          <div class="kpi"><div class="label">Network Km</div><div class="value">{summary["network_km"]:,}</div></div>
          <div class="kpi"><div class="label">TEN-T Share</div><div class="value">{summary["tent_share"]}%</div></div>
          <div class="kpi"><div class="label">High Priority</div><div class="value">{summary["high_priority"]:,}</div></div>
        </div>
        <svg id="map" viewBox="0 0 920 640" role="img" aria-label="{title}"></svg>
        {legend_html}
      </section>
      <aside class="card">
        <h2>{sidebar_title}</h2>
        <table>
          <tbody id="detailBody">
            {body_rows}
          </tbody>
        </table>
        <p class="note">{note}</p>
      </aside>
    </div>
  </div>
  <script>{script}</script>
</body>
</html>
"""


def shared_script_prelude(bounds: Dict[str, float], backdrops: List[List[List[float]]]) -> str:
    return f"""
    const bounds = {json.dumps(bounds, ensure_ascii=False)};
    const backdrops = {json.dumps(backdrops, ensure_ascii=False)};
    const width = 920;
    const height = 640;
    const NS = 'http://www.w3.org/2000/svg';
    const map = document.getElementById('map');
    function project(lon, lat) {{
      const x = ((lon - bounds.minLon) / (bounds.maxLon - bounds.minLon)) * (width - 56) + 28;
      const y = height - (((lat - bounds.minLat) / (bounds.maxLat - bounds.minLat)) * (height - 56) + 28);
      return [x, y];
    }}
    function add(tag, attrs, parent) {{
      const node = document.createElementNS(NS, tag);
      Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, value));
      parent.appendChild(node);
      return node;
    }}
    add('rect', {{x: 0, y: 0, width, height, fill: 'var(--sea)'}}, map);
    backdrops.forEach((shape) => {{
      const d = shape.map(([lon, lat], idx) => {{
        const [x, y] = project(lon, lat);
        return `${{idx === 0 ? 'M' : 'L'}}${{x.toFixed(2)}},${{y.toFixed(2)}}`;
      }}).join(' ');
      add('path', {{
        d,
        fill: 'none',
        stroke: 'rgba(216, 198, 172, 0.36)',
        'stroke-width': '10',
        'stroke-linecap': 'round',
        opacity: '0.55',
      }}, map);
      add('path', {{
        d,
        fill: 'none',
        stroke: 'rgba(255,255,255,0.22)',
        'stroke-width': '5',
        'stroke-linecap': 'round',
        opacity: '0.65',
      }}, map);
    }});
    """


def render_line_map(title: str, subtitle: str, features: List[Dict], bounds: Dict[str, float], backdrops: List[List[List[float]]], color_mode: str, summary: Dict[str, object]) -> str:
    legend_html = ""
    if color_mode == "priority":
        legend_html = """
        <div class="legend">
          <span><span class="dot" style="background: var(--high)"></span>High</span>
          <span><span class="dot" style="background: var(--medium)"></span>Medium</span>
          <span><span class="dot" style="background: var(--low)"></span>Low</span>
        </div>
        """
    script = shared_script_prelude(bounds, backdrops) + f"""
    const features = {json.dumps(features, ensure_ascii=False)};
    function updateDetails(feature) {{
      document.getElementById('detailBody').innerHTML = `
        <tr><th>Route</th><td>${{feature.carretera}}</td></tr>
        <tr><th>Length</th><td>${{feature.length_km}} km</td></tr>
        <tr><th>Priority</th><td>${{feature.priority_level}}</td></tr>
        <tr><th>Score</th><td>${{feature.priority_score}}</td></tr>
        <tr><th>TEN-T</th><td>${{feature.is_tent ? 'Yes' : 'No'}}</td></tr>`;
    }}
    features.forEach((feature, idx) => {{
      const d = feature.coords.map(([lon, lat], i) => {{
        const [x, y] = project(lon, lat);
        return `${{i === 0 ? 'M' : 'L'}}${{x.toFixed(2)}},${{y.toFixed(2)}}`;
      }}).join(' ');
      const halo = add('path', {{
        d,
        fill: 'none',
        stroke: 'rgba(255,255,255,0.44)',
        'stroke-width': '{'3.1' if color_mode == 'priority' else '2.3'}',
        'stroke-linecap': 'round',
        opacity: '0.8',
      }}, map);
      const path = add('path', {{
        d,
        fill: 'none',
        stroke: feature.color,
        'stroke-width': '{'1.95' if color_mode == 'priority' else '1.35'}',
        'stroke-linecap': 'round',
        opacity: '{'0.94' if color_mode == 'priority' else '0.84'}',
      }}, map);
      [halo, path].forEach((node) => {{
        node.addEventListener('mouseenter', () => updateDetails(feature));
        node.addEventListener('click', () => updateDetails(feature));
      }});
      if (idx === 0) updateDetails(feature);
    }});
    """
    return render_shell(
        title=title,
        subtitle=subtitle,
        summary=summary,
        sidebar_title="Segment Details",
        body_rows="""
          <tr><th>Route</th><td>Hover or click a road segment</td></tr>
          <tr><th>Length</th><td>-</td></tr>
          <tr><th>Priority</th><td>-</td></tr>
          <tr><th>Score</th><td>-</td></tr>
          <tr><th>TEN-T</th><td>-</td></tr>
        """,
        note="These reference maps are fully self-contained and can be opened locally without any external tile service.",
        legend_html=legend_html,
        script=script,
    )


def render_heatmap(title: str, subtitle: str, points: List[Dict], bounds: Dict[str, float], backdrops: List[List[List[float]]], summary: Dict[str, object]) -> str:
    script = shared_script_prelude(bounds, backdrops) + f"""
    const points = {json.dumps(points, ensure_ascii=False)};
    const maxWeight = Math.max(...points.map((point) => point.weight), 1);
    function colorFor(weight) {{
      const t = Math.max(0, Math.min(1, weight / maxWeight));
      if (t > 0.78) return 'var(--hot)';
      if (t > 0.48) return 'var(--warm)';
      return 'var(--cool)';
    }}
    function updateDetails(point) {{
      document.getElementById('detailBody').innerHTML = `
        <tr><th>Route</th><td>${{point.carretera}}</td></tr>
        <tr><th>Length</th><td>${{point.length_km}} km</td></tr>
        <tr><th>Weight</th><td>${{point.weight}}</td></tr>
        <tr><th>Coordinates</th><td>${{point.lat.toFixed(4)}}, ${{point.lon.toFixed(4)}}</td></tr>`;
    }}
    points.forEach((point, idx) => {{
      const [cx, cy] = project(point.lon, point.lat);
      const radius = 8 + (point.weight / maxWeight) * 18;
      add('circle', {{
        cx, cy, r: radius * 1.8,
        fill: colorFor(point.weight),
        opacity: '0.05',
        stroke: 'none',
      }}, map);
      add('circle', {{
        cx, cy, r: radius,
        fill: colorFor(point.weight),
        opacity: '0.13',
        stroke: 'none',
      }}, map);
      const marker = add('circle', {{
        cx, cy, r: Math.max(2.6, radius * 0.17),
        fill: colorFor(point.weight),
        opacity: '0.96',
        stroke: 'rgba(255,255,255,0.9)',
        'stroke-width': '1.1',
      }}, map);
      marker.addEventListener('mouseenter', () => updateDetails(point));
      marker.addEventListener('click', () => updateDetails(point));
      if (idx === 0) updateDetails(point);
    }});
    """
    return render_shell(
        title=title,
        subtitle=subtitle,
        summary=summary,
        sidebar_title="Density Details",
        body_rows="""
          <tr><th>Route</th><td>Hover or click a hotspot</td></tr>
          <tr><th>Length</th><td>-</td></tr>
          <tr><th>Weight</th><td>-</td></tr>
          <tr><th>Coordinates</th><td>-</td></tr>
        """,
        note="The density view uses route centroids weighted by segment length to surface where the interurban network concentrates spatially.",
        legend_html="""
        <div class="legend">
          <span><span class="band" style="background: var(--cool)"></span>Lower density</span>
          <span><span class="band" style="background: var(--warm)"></span>Medium density</span>
          <span><span class="band" style="background: var(--hot)"></span>Higher density</span>
        </div>
        """,
        script=script,
    )


def save_html(filename: str, html: str) -> None:
    MAPS_DIR.mkdir(parents=True, exist_ok=True)
    (MAPS_DIR / filename).write_text(html, encoding="utf-8")


def main() -> None:
    roads = load_datasets()
    base_features = geometry_to_features(roads, color_mode="base")
    priority_features = geometry_to_features(roads, color_mode="priority")
    points = centroid_points(roads)
    bounds = bounds_from_features(base_features)
    backdrops = backdrop_features(base_features)
    summary = build_summary(roads)

    save_html(
        "base_map.html",
        render_line_map(
            title="Base RTIG Map",
            subtitle="Offline reference view of Spain's processed RTIG road network, framed for local review and judge-side sharing.",
            features=base_features,
            bounds=bounds,
            backdrops=backdrops,
            color_mode="base",
            summary=summary,
        ),
    )
    save_html(
        "priority_map.html",
        render_line_map(
            title="Priority Corridor Map",
            subtitle="Offline corridor reference map with segments coloured by the scoring model to make strategic emphasis visible at a glance.",
            features=priority_features,
            bounds=bounds,
            backdrops=backdrops,
            color_mode="priority",
            summary=summary,
        ),
    )
    save_html(
        "density_heatmap.html",
        render_heatmap(
            title="Network Density Heatmap",
            subtitle="Offline density view of processed RTIG segments using weighted centroids, with the national road silhouette retained for geographic context.",
            points=points,
            bounds=bounds,
            backdrops=backdrops,
            summary=summary,
        ),
    )
    print(f"Saved: {MAPS_DIR / 'base_map.html'}")
    print(f"Saved: {MAPS_DIR / 'priority_map.html'}")
    print(f"Saved: {MAPS_DIR / 'density_heatmap.html'}")


if __name__ == "__main__":  # pragma: no cover
    main()
