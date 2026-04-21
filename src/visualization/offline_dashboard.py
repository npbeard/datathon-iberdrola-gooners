"""Build a polished self-contained dashboard for the datathon submission."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Dict, Iterable, List

import geopandas as gpd
import pandas as pd

STATUS_ORDER = ["Sufficient", "Moderate", "Congested"]
STATUS_COLORS = {
    "Sufficient": "#2f7d4a",
    "Moderate": "#d2a127",
    "Congested": "#b6452c",
}
ACTION_LABELS = {
    "Sufficient": "Build now",
    "Moderate": "Phase next",
    "Congested": "Reinforce first",
}
ACTION_DESCRIPTIONS = {
    "Sufficient": "Nearest grid node appears to support the proposed site with comfortable headroom.",
    "Moderate": "Promising site, but rollout should be staged with limited reinforcement or sequencing.",
    "Congested": "Strategically important mobility need, but grid reinforcement should lead the timeline.",
}
DEFAULT_BOUNDS = {"minLon": -9.7, "maxLon": 4.0, "minLat": 35.2, "maxLat": 44.3}


def _round_or_none(value: object, digits: int = 2) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _clean_record(record: Dict[str, object]) -> Dict[str, object]:
    cleaned: Dict[str, object] = {}
    for key, value in record.items():
        if pd.isna(value):
            cleaned[key] = None
        elif isinstance(value, float):
            cleaned[key] = round(value, 4)
        else:
            cleaned[key] = value
    return cleaned


def _geometry_to_lines(roads_gdf: gpd.GeoDataFrame | None) -> List[List[List[float]]]:
    if roads_gdf is None or roads_gdf.empty:
        return []

    simplified = roads_gdf[["geometry"]].copy()
    simplified["geometry"] = simplified.geometry.simplify(0.02, preserve_topology=False)

    lines: List[List[List[float]]] = []
    for geom in simplified.geometry:
        if geom is None or geom.is_empty:
            continue
        if geom.geom_type == "LineString":
            lines.append([[round(x, 4), round(y, 4)] for x, y in geom.coords])
        elif geom.geom_type == "MultiLineString":
            for part in geom.geoms:
                lines.append([[round(x, 4), round(y, 4)] for x, y in part.coords])
    return lines


def _route_lines(roads_gdf: gpd.GeoDataFrame | None) -> Dict[str, List[List[List[float]]]]:
    if roads_gdf is None or roads_gdf.empty or "carretera" not in roads_gdf.columns:
        return {}

    simplified = roads_gdf[["carretera", "geometry"]].copy()
    simplified["geometry"] = simplified.geometry.simplify(0.03, preserve_topology=False)
    route_map: Dict[str, List[List[List[float]]]] = {}

    for row in simplified.itertuples(index=False):
        route = str(getattr(row, "carretera", "") or "").strip()
        geom = row.geometry
        if not route or geom is None or geom.is_empty:
            continue
        route_map.setdefault(route, [])
        if geom.geom_type == "LineString":
            route_map[route].append([[round(x, 4), round(y, 4)] for x, y in geom.coords])
        elif geom.geom_type == "MultiLineString":
            for part in geom.geoms:
                route_map[route].append([[round(x, 4), round(y, 4)] for x, y in part.coords])
    return route_map


def _map_bounds(stations_df: pd.DataFrame, lines: List[List[List[float]]]) -> Dict[str, float]:
    lons: List[float] = []
    lats: List[float] = []

    for line in lines:
        for lon, lat in line:
            lons.append(float(lon))
            lats.append(float(lat))

    if not stations_df.empty:
        lons.extend(stations_df["longitude"].astype(float).tolist())
        lats.extend(stations_df["latitude"].astype(float).tolist())

    if not lons or not lats:
        return DEFAULT_BOUNDS.copy()

    return {
        "minLon": min(lons) - 0.25,
        "maxLon": max(lons) + 0.25,
        "minLat": min(lats) - 0.25,
        "maxLat": max(lats) + 0.25,
    }


def _status_summary(stations_df: pd.DataFrame) -> List[Dict[str, object]]:
    summary = (
        stations_df.groupby("grid_status", dropna=False)
        .agg(
            sites=("location_id", "count"),
            chargers=("n_chargers_proposed", "sum"),
            average_chargers=("n_chargers_proposed", "mean"),
            demand_kw=("estimated_demand_kw", "sum"),
        )
        .reindex(STATUS_ORDER, fill_value=0)
        .reset_index()
    )

    total_sites = max(int(summary["sites"].sum()), 1)
    rows: List[Dict[str, object]] = []
    for row in summary.itertuples(index=False):
        status = str(row.grid_status)
        rows.append(
            {
                "status": status,
                "action": ACTION_LABELS[status],
                "description": ACTION_DESCRIPTIONS[status],
                "sites": int(row.sites),
                "chargers": int(row.chargers),
                "average_chargers": round(float(row.average_chargers or 0.0), 1),
                "demand_kw": int(round(float(row.demand_kw or 0.0))),
                "share_pct": round((float(row.sites) / total_sites) * 100.0, 1),
                "color": STATUS_COLORS[status],
            }
        )
    return rows


def _route_summary_payload(route_summary: pd.DataFrame, stations_df: pd.DataFrame, file_3: pd.DataFrame) -> List[Dict[str, object]]:
    agg_spec = {
        "sites": ("location_id", "count"),
        "chargers": ("n_chargers_proposed", "sum"),
    }
    if "business_score" in stations_df.columns:
        agg_spec["avg_business_score"] = ("business_score", "mean")

    route_sites = stations_df.groupby("route_segment", as_index=False).agg(**agg_spec).rename(
        columns={"route_segment": "carretera"}
    )
    if "avg_business_score" not in route_sites.columns:
        route_sites["avg_business_score"] = None
    route_friction = (
        file_3.groupby("route_segment", as_index=False)
        .agg(
            friction_points=("bottleneck_id", "count"),
            friction_kw=("estimated_demand_kw", "sum"),
        )
        .rename(columns={"route_segment": "carretera"})
    )

    merged = route_sites.merge(route_friction, on="carretera", how="left")
    merged["friction_points"] = merged["friction_points"].fillna(0).astype(int)
    merged["friction_kw"] = merged["friction_kw"].fillna(0.0)

    route_cols = [
        "carretera",
        "total_length_km",
        "pk_span_km",
        "existing_station_count",
        "service_need_score",
        "strategic_corridor_score",
        "traffic_imd_total",
        "market_access_population",
        "tourism_overnight_stays",
        "business_support_score",
    ]
    if route_summary is not None and not route_summary.empty:
        available_cols = [column for column in route_cols if column in route_summary.columns]
        merged = merged.merge(route_summary[available_cols], on="carretera", how="left")

    merged["priority_index"] = (
        merged.get("service_need_score", 0).fillna(0.0) * 100.0
        + merged["chargers"].astype(float) * 0.45
        + merged["friction_points"].astype(float) * 0.30
    )
    merged = merged.sort_values(
        ["priority_index", "chargers", "sites", "carretera"],
        ascending=[False, False, False, True],
    ).head(12)

    rows: List[Dict[str, object]] = []
    for row in merged.itertuples(index=False):
        rows.append(
            _clean_record(
                {
                    "route": row.carretera,
                    "sites": int(row.sites),
                    "chargers": int(row.chargers),
                    "friction_points": int(getattr(row, "friction_points", 0) or 0),
                    "friction_kw": _round_or_none(getattr(row, "friction_kw", 0.0), 0) or 0,
                    "length_km": _round_or_none(getattr(row, "total_length_km", None)),
                    "pk_span_km": _round_or_none(getattr(row, "pk_span_km", None)),
                    "service_need_score": _round_or_none(getattr(row, "service_need_score", None), 3),
                    "strategic_corridor_score": _round_or_none(getattr(row, "strategic_corridor_score", None), 3),
                    "existing_station_count": int(getattr(row, "existing_station_count", 0) or 0),
                    "business_support_score": _round_or_none(getattr(row, "business_support_score", None)),
                    "avg_business_score": _round_or_none(getattr(row, "avg_business_score", None)),
                    "traffic_imd_total": _round_or_none(getattr(row, "traffic_imd_total", None), 0),
                    "market_access_population": _round_or_none(getattr(row, "market_access_population", None), 0),
                    "tourism_overnight_stays": _round_or_none(getattr(row, "tourism_overnight_stays", None), 0),
                }
            )
        )
    return rows


def _friction_summary(file_3: pd.DataFrame) -> List[Dict[str, object]]:
    if file_3.empty:
        return []

    summary = (
        file_3.groupby("distributor_network", as_index=False)
        .agg(
            friction_points=("bottleneck_id", "count"),
            demand_kw=("estimated_demand_kw", "sum"),
        )
        .sort_values(["friction_points", "demand_kw"], ascending=[False, False])
    )
    rows: List[Dict[str, object]] = []
    for row in summary.itertuples(index=False):
        rows.append(
            {
                "distributor": str(row.distributor_network),
                "friction_points": int(row.friction_points),
                "demand_kw": int(round(float(row.demand_kw))),
            }
        )
    return rows


def _station_payload(stations_df: pd.DataFrame) -> List[Dict[str, object]]:
    ordered = stations_df.copy()
    ordered["grid_status"] = pd.Categorical(ordered["grid_status"], categories=STATUS_ORDER, ordered=True)
    sort_columns: List[str] = ["grid_status", "n_chargers_proposed"]
    ascending = [False, False]
    if "business_score" in ordered.columns:
        sort_columns.append("business_score")
        ascending.append(False)
    ordered = ordered.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)

    business_max = float(ordered["business_score"].fillna(0).max()) if "business_score" in ordered.columns else 0.0
    rows: List[Dict[str, object]] = []
    for row in ordered.itertuples(index=False):
        business_score = float(getattr(row, "business_score", 0.0) or 0.0)
        rows.append(
            _clean_record(
                {
                    "location_id": str(row.location_id),
                    "latitude": float(row.latitude),
                    "longitude": float(row.longitude),
                    "route_segment": str(row.route_segment),
                    "n_chargers_proposed": int(row.n_chargers_proposed),
                    "grid_status": str(row.grid_status),
                    "action_label": ACTION_LABELS[str(row.grid_status)],
                    "color": STATUS_COLORS[str(row.grid_status)],
                    "estimated_demand_kw": _round_or_none(getattr(row, "estimated_demand_kw", None), 0),
                    "distributor_network": str(getattr(row, "distributor_network", "") or "Not classified"),
                    "distance_to_grid_node_km": _round_or_none(getattr(row, "distance_to_grid_node_km", None)),
                    "available_capacity_kw": _round_or_none(getattr(row, "available_capacity_kw", None), 0),
                    "nearest_grid_node": str(getattr(row, "nearest_grid_node", "") or ""),
                    "business_score": _round_or_none(business_score),
                    "business_share": round((business_score / business_max), 4) if business_max > 0 else 0.0,
                    "service_need_score": _round_or_none(getattr(row, "service_need_score", None), 3),
                    "strategic_corridor_score": _round_or_none(getattr(row, "strategic_corridor_score", None), 3),
                    "existing_station_count": _round_or_none(getattr(row, "existing_station_count", None), 0),
                    "business_support_score": _round_or_none(getattr(row, "business_support_score", None), 1),
                    "traffic_imd_total": _round_or_none(getattr(row, "traffic_imd_total", None), 0),
                    "market_access_population": _round_or_none(getattr(row, "market_access_population", None), 0),
                    "tourism_overnight_stays": _round_or_none(getattr(row, "tourism_overnight_stays", None), 0),
                    "coverage_gap_score": _round_or_none(getattr(row, "coverage_gap_score", None), 2),
                }
            )
        )
    return rows


def _stations_with_route_context(stations_df: pd.DataFrame, route_summary: pd.DataFrame | None) -> pd.DataFrame:
    enriched = stations_df.copy()
    if route_summary is None or route_summary.empty:
        return enriched

    route_cols = [
        "carretera",
        "service_need_score",
        "strategic_corridor_score",
        "existing_station_count",
        "business_support_score",
        "traffic_imd_total",
        "market_access_population",
        "tourism_overnight_stays",
        "coverage_gap_score",
    ]
    available = [column for column in route_cols if column in route_summary.columns]
    if "carretera" not in available:
        return enriched

    route_lookup = route_summary[available].rename(columns={"carretera": "route_segment"})
    return enriched.merge(route_lookup, on="route_segment", how="left")


def _source_cards() -> List[Dict[str, str]]:
    return [
        {
            "source": "RTIG Road Network",
            "owner": "Ministry of Transport",
            "purpose": "Defined the interurban A-, AP-, and N- corridor universe and the geometry used for candidate siting.",
        },
        {
            "source": "Official Charging Baseline",
            "owner": "NAP-DGT / MITERD",
            "purpose": "Measured where public charging already exists so we reduce redundant placement and reward uncovered corridors.",
        },
        {
            "source": "EV Adoption Forecast",
            "owner": "datos.gob.es exercise",
            "purpose": "Anchored the 2027 demand scenario with the mandatory electrification workflow instead of an arbitrary growth assumption.",
        },
        {
            "source": "Grid Capacity Nodes",
            "owner": "i-DE, Endesa, Viesgo",
            "purpose": "Tested each proposed site against the nearest published node to classify build-now, phase-next, or reinforce-first locations.",
        },
        {
            "source": "Traffic Intensity",
            "owner": "MITMA traffic map",
            "purpose": "Strengthened corridor ranking with annual intensity and heavy-vehicle signals to prioritize nationally relevant routes.",
        },
        {
            "source": "Stop Quality Proxies",
            "owner": "INE + Geoportal Gasolineras",
            "purpose": "Reinforced placement near stronger stop environments using population, overnight stays, and roadside service ecosystems.",
        },
    ]


def _assumption_cards() -> List[Dict[str, str]]:
    return [
        {
            "title": "Lean network objective",
            "body": "We use dynamic spacing around 120 km and merge duplicate coordinates so the network stays as sparse as possible while still covering interurban need.",
        },
        {
            "title": "Fixed charger power",
            "body": "Every charger is evaluated at the brief’s fixed 150 kW standard, making friction-point calculations directly comparable across teams.",
        },
        {
            "title": "Nearest-node grid realism",
            "body": "Grid feasibility is a planning approximation based on the nearest published distributor node, which is more realistic than placing chargers without a capacity check.",
        },
        {
            "title": "Commercial viability signal",
            "body": "We do not place chargers only by geometry. We also reward routes and stops where nearby services and market access make a real charging stop more credible.",
        },
    ]


def _business_score_cards() -> List[Dict[str, str]]:
    return [
        {
            "title": "Roadside anchor quality",
            "body": "We score nearby stop anchors such as food, lodging, fuel, parking, retail, and service-area signals. Stronger roadside ecosystems contribute more than isolated points.",
        },
        {
            "title": "Population and tourism lift",
            "body": "The score is strengthened with public demand proxies, especially market-access population and provincial overnight stays, so routes with stronger stop economics receive more support.",
        },
        {
            "title": "Closer anchors matter more",
            "body": "At the station level, candidate points near the target spacing position get more credit when the strongest anchors are closest, so the final site favors a more natural stop environment.",
        },
    ]


def build_offline_dashboard(
    file_1: pd.DataFrame,
    stations_df: pd.DataFrame,
    file_3: pd.DataFrame,
    roads_gdf: gpd.GeoDataFrame | None,
    route_summary: pd.DataFrame | None,
    output_path: Path,
    title: str = "Iberdrola Interurban Charging Network Dashboard",
    embed_explorer: bool = False,
    explorer_path: str = "offline_scenario_explorer.html",
    map_only: bool = False,
) -> None:
    """Render a winning-oriented, offline-safe dashboard to a single HTML file."""

    stations_df = _stations_with_route_context(stations_df, route_summary)
    lines = _geometry_to_lines(roads_gdf)
    route_lines = _route_lines(roads_gdf)
    bounds = _map_bounds(stations_df, lines)

    stations_payload = _station_payload(stations_df)
    status_payload = _status_summary(stations_df)
    route_payload = _route_summary_payload(route_summary if route_summary is not None else pd.DataFrame(), stations_df, file_3)
    friction_payload = _friction_summary(file_3)
    source_cards = _source_cards()
    assumption_cards = _assumption_cards()
    business_score_cards = _business_score_cards()

    file_1_row = file_1.iloc[0].to_dict() if not file_1.empty else {}
    total_chargers = int(stations_df["n_chargers_proposed"].sum()) if not stations_df.empty else 0
    friction_share = round((len(file_3) / max(len(stations_df), 1)) * 100.0, 1)
    route_count = int(stations_df["route_segment"].nunique()) if not stations_df.empty else 0

    payload = {
        "title": title,
        "metrics": {
            "proposed_sites": int(file_1_row.get("total_proposed_stations", len(stations_df))),
            "existing_baseline": int(file_1_row.get("total_existing_stations_baseline", 0)),
            "friction_points": int(file_1_row.get("total_friction_points", len(file_3))),
            "ev_2027": int(file_1_row.get("total_ev_projected_2027", 0)),
            "total_chargers": total_chargers,
            "route_count": route_count,
            "friction_share": friction_share,
        },
        "recommendations": [
            {
                "title": "Prioritize build-now corridors first",
                "body": "Start with the green share of the network to secure visible interurban coverage quickly, then use those wins to phase investment into the more constrained corridors.",
            },
            {
                "title": "Treat friction points as a roadmap, not a flaw",
                "body": "More than half of the proposed sites surface real grid tension. That is valuable because it shows exactly where commercial need and reinforcement planning must be aligned.",
            },
            {
                "title": "Focus early attention on the strongest national corridors",
                "body": "The proposal concentrates the heaviest charger counts on corridors such as A-7, A-66, and A-2, where network relevance, route continuity, and strategic value are hard to ignore.",
            },
            {
                "title": "Defend every placement with public evidence",
                "body": "Each site is supported by a reproducible mix of official baseline charging data, forecasted EV demand, traffic intensity, stop-quality proxies, and published distributor capacity files.",
            },
        ],
        "bounds": bounds,
        "roads": lines,
        "routeLines": route_lines,
        "stations": stations_payload,
        "statusSummary": status_payload,
        "routes": route_payload,
        "frictionSummary": friction_payload,
        "sourceSummary": source_cards,
        "assumptionSummary": assumption_cards,
    }
    payload_json = json.dumps(payload, ensure_ascii=False)

    source_cards_html = "".join(
        f"""
          <article class="source-card">
            <p class="source-owner">{card["owner"]}</p>
            <h3>{card["source"]}</h3>
            <p>{card["purpose"]}</p>
          </article>
        """
        for card in source_cards
    )
    assumption_cards_html = "".join(
        f"""
          <article class="assumption-card">
            <h3>{card["title"]}</h3>
            <p>{card["body"]}</p>
          </article>
        """
        for card in assumption_cards
    )
    business_score_cards_html = "".join(
        f"""
          <article class="source-card">
            <p class="source-owner">Business Score</p>
            <h3>{card["title"]}</h3>
            <p>{card["body"]}</p>
          </article>
        """
        for card in business_score_cards
    )

    explorer_section = ""
    if embed_explorer and not map_only:
        explorer_section = f"""
    <section class="analytics">
      <article class="card" style="grid-column: 1 / -1;">
        <p class="eyebrow">Scenario Explorer</p>
        <h2>Interactive companion for scenario testing</h2>
        <p style="margin-bottom: 14px;">This embedded explorer is a companion tool for presentations and internal discussion. The official submission-safe file remains the main offline dashboard and network map.</p>
        <div class="explorer-frame-wrap">
          <iframe class="explorer-frame" src="{explorer_path}" loading="lazy" title="Offline scenario explorer"></iframe>
        </div>
        <p class="footnote">If the embed does not load in your browser, open <code>{explorer_path}</code> directly from the <code>maps/</code> folder.</p>
      </article>
    </section>
"""

    hero_section = ""
    impact_section = ""
    recommendation_section = ""
    story_section = ""
    analytics_sections = ""
    main_class = "main"
    map_note_secondary = "Click a route row or station marker to inspect that corridor in detail."

    if map_only:
        main_class = "main map-only"
        map_note_secondary = "Use the filters to inspect the proposed network by status, distributor, or corridor."
    else:
        hero_section = """
    <section class="hero">
      <article class="card hero-card">
        <p class="eyebrow">IE Sustainability Datathon 2026 · Deliverable 3</p>
        <h1>Our 2027 interurban charging rollout proposal for Iberdrola.</h1>
        <p class="lead">This dashboard is built to defend the placement logic behind the network, not just display points on a map. It combines corridor need, current charging scarcity, 2027 EV demand, stop quality, and grid feasibility into a phased rollout strategy.</p>
        <div class="kpi-grid">
          <div class="kpi"><div class="label">Proposed Sites</div><div class="value" id="metricSites">0</div><div class="sub">File 2 footprint</div></div>
          <div class="kpi"><div class="label">Total Chargers</div><div class="value" id="metricChargers">0</div><div class="sub">Installed points proposed</div></div>
          <div class="kpi"><div class="label">Friction Points</div><div class="value" id="metricFriction">0</div><div class="sub">File 3 bottlenecks</div></div>
          <div class="kpi"><div class="label">Existing Baseline</div><div class="value" id="metricBaseline">0</div><div class="sub">Official interurban context</div></div>
          <div class="kpi"><div class="label">Projected EV 2027</div><div class="value" id="metricEV">0</div><div class="sub">Mandatory forecast anchor</div></div>
          <div class="kpi"><div class="label">Covered Corridors</div><div class="value" id="metricRoutes">0</div><div class="sub">Distinct route segments</div></div>
        </div>
      </article>
      <aside class="card">
        <p class="eyebrow">How To Read The Map</p>
        <div class="guide-list">
          <div class="guide-item">
            <strong>Colors explain buildability, not demand.</strong>
            Green means the nearest published grid node looks comfortable. Yellow means phased deployment. Red means reinforce first.
          </div>
          <div class="guide-item">
            <strong>Marker size shows station scale.</strong>
            Larger dots represent sites with more chargers, so spatial importance is visible immediately.
          </div>
          <div class="guide-item">
            <strong>The dashboard is meant to tell a rollout story.</strong>
            It connects the mandatory CSVs to corridor prioritisation, distributor pressure, and a practical 2027 deployment sequence.
          </div>
        </div>
      </aside>
    </section>
"""
        impact_section = """
    <section class="impact-band">
      <article class="card impact-card">
        <p class="eyebrow">Why Iberdrola Should Care</p>
        <div class="metric" id="impactCoverage">0</div>
        <p>interurban corridors are covered by the proposal, turning the output into a national network decision rather than a list of isolated sites.</p>
      </article>
      <article class="card impact-card">
        <p class="eyebrow">Capital Discipline</p>
        <div class="metric" id="impactBaseline">0</div>
        <p>existing baseline stations are already credited in the planning logic so the proposal avoids overbuilding where supply already exists.</p>
      </article>
      <article class="card impact-card">
        <p class="eyebrow">Execution Risk</p>
        <div class="metric" id="impactFriction">0</div>
        <p>of proposed sites are friction points, making the grid-constrained part of the rollout visible before investment decisions are made.</p>
      </article>
    </section>
"""
        recommendation_section = """
    <section class="recommendation-strip">
      <article class="recommendation-card">
        <p class="eyebrow">Recommendation 1</p>
        <h3>Prioritize build-now corridors first</h3>
        <p>Start with the green share of the network to secure visible interurban coverage quickly, then use those wins to phase investment into the more constrained corridors.</p>
      </article>
      <article class="recommendation-card">
        <p class="eyebrow">Recommendation 2</p>
        <h3>Treat friction points as a roadmap, not a flaw</h3>
        <p>More than half of the proposed sites surface real grid tension. That is valuable because it shows exactly where commercial need and reinforcement planning must be aligned.</p>
      </article>
      <article class="recommendation-card">
        <p class="eyebrow">Recommendation 3</p>
        <h3>Focus early attention on the strongest national corridors</h3>
        <p>The proposal concentrates the heaviest charger counts on corridors such as A-7, A-66, and A-2, where network relevance, route continuity, and strategic value are hard to ignore.</p>
      </article>
      <article class="recommendation-card">
        <p class="eyebrow">Recommendation 4</p>
        <h3>Defend every placement with public evidence</h3>
        <p>Each site is supported by a reproducible mix of official baseline charging data, forecasted EV demand, traffic intensity, stop-quality proxies, and published distributor capacity files.</p>
      </article>
    </section>
"""
        story_section = """
    <section class="story-grid">
      <article class="card story-card">
        <p class="eyebrow">Strategic Thesis</p>
        <h2>We optimize for the fewest useful stations, not the most points on a map.</h2>
        <p>The proposal starts from interurban coverage need and takes credit for the existing baseline. That means long-distance corridors with weak current charging supply rise first, while already-served routes are penalized.</p>
      </article>
      <article class="card story-card">
        <p class="eyebrow">Placement Logic</p>
        <h2>Every site must pass three tests.</h2>
        <p>First, the corridor must matter strategically. Second, the stop must make commercial sense. Third, the nearest published grid node must support immediate deployment or justify phased reinforcement.</p>
      </article>
      <article class="card story-card">
        <p class="eyebrow">Investment Framing</p>
        <h2>The map is a rollout roadmap, not a static inventory.</h2>
        <p>Green sites are immediate candidates, yellow sites are sequencing candidates, and red sites identify where mobility need collides with network constraints. That turns geography into an actionable plan.</p>
      </article>
    </section>
"""
        analytics_sections = f"""
    <section class="analytics">
      <article class="card">
        <p class="eyebrow">Deployment Ladder</p>
        <h2>Three action buckets for a practical 2027 rollout</h2>
        <div class="status-stack" id="statusStack" style="margin-top: 16px;"></div>
      </article>

      <article class="card">
        <p class="eyebrow">National Priority Corridors</p>
        <h2>Where the proposal is strongest and why</h2>
        <div class="corridor-callouts" id="corridorCallouts"></div>
        <table class="route-table">
          <thead>
            <tr>
              <th>Route</th>
              <th>Sites</th>
              <th>Chargers</th>
              <th>Friction</th>
            </tr>
          </thead>
          <tbody id="routeTableBody"></tbody>
        </table>
      </article>
    </section>

    <section class="analytics">
      <article class="card">
        <p class="eyebrow">Business-Fit Logic</p>
        <h2>How the business score helps break ties between plausible sites</h2>
        <div class="source-grid">
{business_score_cards_html}
        </div>
        <p class="footnote">The route-level `business_support_score` aggregates stronger commercial ecosystems by corridor, while the station-level `business_score` gives more weight to the closest anchors near the final chosen point.</p>
      </article>

      <article class="card">
        <p class="eyebrow">Evidence Backbone</p>
        <h2>Public and competition-relevant sources behind the proposal</h2>
        <div class="source-grid">
{source_cards_html}
        </div>
      </article>
    </section>

    <section class="analytics">
      <article class="card">
        <p class="eyebrow">Assumptions</p>
        <h2>How we kept key siting assumptions explicit and defensible</h2>
        <div class="assumption-grid">
{assumption_cards_html}
        </div>
      </article>
    </section>

    <section class="analytics">
      <article class="card">
        <p class="eyebrow">Grid Pressure</p>
        <h2>Where reinforcement conversations cluster by distributor</h2>
        <table class="friction-table">
          <thead>
            <tr>
              <th>Distributor</th>
              <th>Friction Points</th>
              <th>Demand (kW)</th>
            </tr>
          </thead>
          <tbody id="frictionTableBody"></tbody>
        </table>
      </article>

      <article class="card">
        <p class="eyebrow">Project Framing</p>
        <h2>Why this view supports our strategy</h2>
        <div class="spotlight-grid">
          <div class="spotlight">
            <div class="label muted">Offline-ready</div>
            <div class="value">100%</div>
            <p>No installs, no logins, no internet dependency, and all data is embedded in the file.</p>
          </div>
          <div class="spotlight">
            <div class="label muted">Grid Friction Share</div>
            <div class="value" id="spotlightFriction">0%</div>
            <p>Share of proposed sites that also become friction points, making the build-now versus reinforce-first trade-off visible.</p>
          </div>
          <div class="spotlight">
            <div class="label muted">Deployment Logic</div>
            <div class="value">3-step</div>
            <p>Build now, phase next, reinforce first. The map is designed to make the deployment sequence understandable at a glance.</p>
          </div>
        </div>
        <p class="footnote">Data story: RTIG interurban corridors + official charging baseline + mandatory EV forecast + published distributor capacity files + business-stop proxies from MITERD and INE.</p>
      </article>
    </section>
{explorer_section}"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #efe4d1;
      --sand: #f6efe5;
      --panel: rgba(255, 251, 245, 0.94);
      --panel-strong: rgba(255, 253, 249, 0.98);
      --ink: #223142;
      --muted: #667381;
      --line: rgba(34, 49, 66, 0.10);
      --shadow: 0 24px 60px rgba(34, 49, 66, 0.10);
      --sea: #e5efe9;
      --route: #c7d0d9;
      --route-highlight: #1f5965;
      --sufficient: {STATUS_COLORS["Sufficient"]};
      --moderate: {STATUS_COLORS["Moderate"]};
      --congested: {STATUS_COLORS["Congested"]};
      --accent: #0d5f63;
      --accent-soft: #dbeceb;
      --warm: #d19046;
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
      background:
        radial-gradient(circle at top left, rgba(255,255,255,0.8), transparent 32%),
        radial-gradient(circle at top right, rgba(219,236,235,0.65), transparent 28%),
        linear-gradient(180deg, #f3ebdf 0%, #eadcc9 100%);
    }}
    .wrap {{
      max-width: 1500px;
      margin: 0 auto;
      padding: 28px 24px 42px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(320px, 0.65fr);
      gap: 20px;
      margin-bottom: 22px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(8px);
      padding: 22px 24px;
    }}
    .hero-card {{
      padding: 28px 28px 24px;
      background:
        linear-gradient(135deg, rgba(255,255,255,0.94), rgba(246,239,229,0.92)),
        linear-gradient(180deg, rgba(219,236,235,0.18), transparent);
    }}
    .eyebrow {{
      margin: 0 0 10px 0;
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }}
    h1, h2, h3 {{
      margin: 0;
      font-weight: 600;
      line-height: 1.02;
    }}
    h1 {{
      font-size: clamp(2.2rem, 4vw, 4rem);
      max-width: 11ch;
    }}
    h2 {{
      font-size: 1.45rem;
      margin-bottom: 10px;
    }}
    h3 {{
      font-size: 1.05rem;
      margin-bottom: 10px;
    }}
    p {{
      margin: 0;
      color: #334254;
      line-height: 1.52;
    }}
    .lead {{
      max-width: 70ch;
      margin-top: 14px;
      font-size: 1.02rem;
    }}
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 12px;
      margin-top: 22px;
    }}
    .kpi {{
      padding: 14px 16px;
      border-radius: 18px;
      background: rgba(255,255,255,0.62);
      border: 1px solid rgba(34,49,66,0.08);
    }}
    .kpi .label {{
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.09em;
    }}
    .kpi .value {{
      margin-top: 6px;
      font-size: 1.8rem;
      color: var(--ink);
    }}
    .kpi .sub {{
      margin-top: 4px;
      font-size: 0.86rem;
      color: var(--muted);
    }}
    .guide-list {{
      display: grid;
      gap: 12px;
    }}
    .guide-item {{
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(255,255,255,0.66);
      border: 1px solid rgba(34,49,66,0.08);
    }}
    .guide-item strong {{
      display: block;
      font-size: 0.95rem;
      margin-bottom: 4px;
    }}
    .impact-band {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 20px;
      margin-bottom: 20px;
    }}
    .impact-card {{
      background:
        linear-gradient(135deg, rgba(13,95,99,0.10), rgba(255,255,255,0.78)),
        rgba(255,255,255,0.85);
      border: 1px solid rgba(13,95,99,0.12);
    }}
    .impact-card .metric {{
      font-size: 2.1rem;
      margin: 8px 0 6px;
      color: #13494f;
    }}
    .recommendation-strip {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 16px;
      margin-bottom: 20px;
    }}
    .recommendation-card {{
      padding: 18px 18px 16px;
      border-radius: 20px;
      background:
        linear-gradient(135deg, rgba(255,255,255,0.92), rgba(13,95,99,0.08)),
        rgba(255,255,255,0.84);
      border: 1px solid rgba(13,95,99,0.10);
      box-shadow: 0 18px 40px rgba(34,49,66,0.06);
    }}
    .recommendation-card h3 {{
      font-size: 1.02rem;
      margin-bottom: 8px;
    }}
    .story-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 20px;
      margin-bottom: 20px;
    }}
    .story-card {{
      min-height: 100%;
    }}
    .story-card p + p {{
      margin-top: 10px;
    }}
    .source-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 16px;
      margin-top: 16px;
    }}
    .source-card, .assumption-card {{
      padding: 16px 18px;
      border-radius: 18px;
      background: rgba(255,255,255,0.66);
      border: 1px solid rgba(34,49,66,0.08);
      min-height: 100%;
    }}
    .source-owner {{
      color: var(--muted);
      font-size: 0.76rem;
      letter-spacing: 0.09em;
      text-transform: uppercase;
      margin-bottom: 8px;
    }}
    .assumption-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 16px;
      margin-top: 16px;
    }}
    .main {{
      display: grid;
      grid-template-columns: minmax(0, 1.32fr) minmax(340px, 0.68fr);
      gap: 20px;
      align-items: start;
    }}
    .main.map-only {{
      grid-template-columns: 1fr;
    }}
    .map-card {{
      padding: 18px;
      background:
        linear-gradient(180deg, rgba(255,255,255,0.92), rgba(246,239,229,0.92)),
        linear-gradient(135deg, rgba(219,236,235,0.16), transparent);
    }}
    .toolbar {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      margin-bottom: 14px;
    }}
    .pill-group {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .pill {{
      border: 1px solid rgba(34,49,66,0.10);
      background: rgba(255,255,255,0.74);
      color: var(--ink);
      border-radius: 999px;
      padding: 9px 14px;
      font: inherit;
      font-size: 0.92rem;
      cursor: pointer;
      transition: transform 140ms ease, background 140ms ease, border-color 140ms ease;
    }}
    .pill:hover {{
      transform: translateY(-1px);
    }}
    .pill.active {{
      background: var(--accent-soft);
      border-color: rgba(13,95,99,0.24);
      color: #12444a;
    }}
    .select, .search {{
      border: 1px solid rgba(34,49,66,0.10);
      background: rgba(255,255,255,0.78);
      border-radius: 14px;
      padding: 10px 12px;
      font: inherit;
      color: var(--ink);
    }}
    .search {{
      min-width: 180px;
      flex: 1 1 220px;
    }}
    .map-wrap {{
      overflow: hidden;
      border-radius: 22px;
      border: 1px solid rgba(34,49,66,0.10);
      background:
        radial-gradient(circle at 20% 10%, rgba(255,255,255,0.55), transparent 34%),
        linear-gradient(180deg, #fbf7f1 0%, #eef2ee 100%);
    }}
    svg {{
      width: 100%;
      height: auto;
      display: block;
    }}
    .map-legend-row {{
      display: flex;
      justify-content: flex-start;
      margin: 14px 0 12px;
    }}
    .legend-card {{
      background: rgba(255,253,248,0.92);
      border: 1px solid rgba(34,49,66,0.10);
      border-radius: 16px;
      padding: 12px 14px;
      box-shadow: 0 12px 30px rgba(34,49,66,0.08);
      backdrop-filter: blur(6px);
      max-width: 300px;
    }}
    .legend-row {{
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 0.87rem;
      margin-top: 7px;
    }}
    .legend-dot {{
      width: 11px;
      height: 11px;
      border-radius: 999px;
      display: inline-block;
      flex: 0 0 auto;
    }}
    .legend-size {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.87rem;
    }}
    .legend-size span {{
      display: inline-block;
      border-radius: 999px;
      background: rgba(13,95,99,0.18);
      border: 1px solid rgba(13,95,99,0.18);
      flex: 0 0 auto;
    }}
    .map-note {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
      margin-top: 12px;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .sidebar {{
      display: grid;
      gap: 16px;
    }}
    .detail-grid {{
      display: grid;
      gap: 10px;
    }}
    .reason-list {{
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }}
    .reason-item {{
      padding: 11px 12px;
      border-radius: 14px;
      background: rgba(13,95,99,0.06);
      border: 1px solid rgba(13,95,99,0.10);
      font-size: 0.92rem;
      line-height: 1.45;
    }}
    .detail-row {{
      display: grid;
      grid-template-columns: 120px minmax(0, 1fr);
      gap: 10px;
      padding-bottom: 10px;
      border-bottom: 1px solid rgba(34,49,66,0.08);
      font-size: 0.95rem;
    }}
    .detail-row .key {{
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-size: 0.78rem;
      padding-top: 2px;
    }}
    .action-chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 7px 11px;
      border-radius: 999px;
      color: white;
      font-size: 0.85rem;
      letter-spacing: 0.03em;
    }}
    .spotlight-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
      margin-top: 20px;
    }}
    .spotlight {{
      padding: 16px 18px;
      border-radius: 20px;
      background: rgba(255,255,255,0.62);
      border: 1px solid rgba(34,49,66,0.08);
    }}
    .spotlight .value {{
      font-size: 2rem;
      margin: 8px 0 6px;
    }}
    .analytics {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 20px;
      margin-top: 20px;
    }}
    .status-stack {{
      display: grid;
      gap: 12px;
    }}
    .status-row {{
      padding: 14px 16px;
      border-radius: 18px;
      border: 1px solid rgba(34,49,66,0.08);
      background: rgba(255,255,255,0.64);
    }}
    .status-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      margin-bottom: 8px;
    }}
    .status-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      font-size: 0.9rem;
      color: var(--muted);
    }}
    .progress {{
      height: 10px;
      border-radius: 999px;
      background: rgba(34,49,66,0.08);
      overflow: hidden;
      margin-top: 10px;
    }}
    .progress > span {{
      display: block;
      height: 100%;
      border-radius: inherit;
    }}
    .route-table, .friction-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.95rem;
    }}
    .route-table th, .route-table td,
    .friction-table th, .friction-table td {{
      text-align: left;
      padding: 11px 0;
      border-bottom: 1px solid rgba(34,49,66,0.08);
      vertical-align: top;
    }}
    .route-table th, .friction-table th {{
      color: var(--muted);
      font-size: 0.78rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .route-row {{
      cursor: pointer;
      transition: background 140ms ease;
    }}
    .route-row:hover {{
      background: rgba(13,95,99,0.04);
    }}
    .route-row.active {{
      background: rgba(13,95,99,0.08);
    }}
    .mini-bar {{
      height: 8px;
      border-radius: 999px;
      background: rgba(34,49,66,0.08);
      overflow: hidden;
      margin-top: 6px;
    }}
    .mini-bar > span {{
      display: block;
      height: 100%;
      background: linear-gradient(90deg, var(--accent), #5ca08d);
      border-radius: inherit;
    }}
    .chip-line {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }}
    .corridor-callouts {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 16px;
      margin-top: 14px;
    }}
    .corridor-card {{
      padding: 16px 18px;
      border-radius: 18px;
      background:
        linear-gradient(135deg, rgba(13,95,99,0.08), rgba(255,255,255,0.75)),
        rgba(255,255,255,0.72);
      border: 1px solid rgba(13,95,99,0.10);
      cursor: pointer;
      transition: transform 140ms ease, border-color 140ms ease;
    }}
    .corridor-card:hover {{
      transform: translateY(-2px);
      border-color: rgba(13,95,99,0.22);
    }}
    .corridor-card .route-name {{
      font-size: 1.25rem;
      margin-bottom: 6px;
    }}
    .corridor-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      color: var(--muted);
      font-size: 0.88rem;
      margin-top: 8px;
    }}
    .route-chip {{
      border: none;
      background: rgba(13,95,99,0.10);
      color: #0f4d52;
      border-radius: 999px;
      padding: 8px 12px;
      font: inherit;
      cursor: pointer;
    }}
    .muted {{
      color: var(--muted);
    }}
    .footnote {{
      margin-top: 18px;
      font-size: 0.88rem;
      color: var(--muted);
    }}
    .explorer-frame-wrap {{
      border-radius: 20px;
      overflow: hidden;
      border: 1px solid rgba(34,49,66,0.10);
      background: rgba(255,255,255,0.70);
      min-height: 920px;
    }}
    .explorer-frame {{
      width: 100%;
      min-height: 920px;
      border: 0;
      display: block;
      background: white;
    }}
    @media (max-width: 1200px) {{
      .hero, .main, .analytics {{
        grid-template-columns: 1fr;
      }}
      .kpi-grid {{
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }}
      .spotlight-grid {{
        grid-template-columns: 1fr;
      }}
      .story-grid, .source-grid, .assumption-grid {{
        grid-template-columns: 1fr;
      }}
      .impact-band, .corridor-callouts {{
        grid-template-columns: 1fr;
      }}
      .recommendation-strip {{
        grid-template-columns: 1fr;
      }}
    }}
    @media (max-width: 720px) {{
      .wrap {{
        padding: 18px 14px 28px;
      }}
      .kpi-grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .detail-row {{
        grid-template-columns: 1fr;
        gap: 4px;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
{hero_section}
{impact_section}
{recommendation_section}
{story_section}
    <section class="{main_class}">
      <article class="card map-card">
        <div class="toolbar">
          <div class="pill-group" id="statusPills"></div>
          <select class="select" id="distributorFilter"></select>
          <input class="search" id="routeSearch" type="search" placeholder="Filter by route, e.g. A-7 or AP-7N">
        </div>
        <div class="map-legend-row">
          <div class="legend-card">
            <p class="eyebrow" style="margin-bottom:6px;">Visual Legend</p>
            <div class="legend-row"><span class="legend-dot" style="background: var(--sufficient);"></span>Green: build now</div>
            <div class="legend-row"><span class="legend-dot" style="background: var(--moderate);"></span>Yellow: phase next</div>
            <div class="legend-row"><span class="legend-dot" style="background: var(--congested);"></span>Red: reinforce first</div>
            <div class="legend-size"><span style="width:10px;height:10px;"></span>smaller site</div>
            <div class="legend-size"><span style="width:18px;height:18px;"></span>larger site</div>
          </div>
        </div>
        <div class="map-wrap">
          <svg id="networkMap" viewBox="0 0 980 680" role="img" aria-label="Map of Spain with proposed charging stations"></svg>
        </div>
        <div class="map-note">
          <span id="filterSummary">Showing full network proposal.</span>
          <span>{map_note_secondary}</span>
        </div>
      </article>

      {"<aside class=\"sidebar\">" if not map_only else ""}
      {"""
        <article class="card">
          <p class="eyebrow">Selected Site</p>
          <div id="stationAction"></div>
          <h2 id="stationTitle" style="margin-top: 10px;">Select a station</h2>
          <p id="stationNarrative" class="muted" style="margin-top: 8px;">The detail panel updates when you hover or click a point on the map.</p>
          <div class="detail-grid" id="stationDetails" style="margin-top: 16px;"></div>
          <div class="reason-list" id="stationWhy"></div>
        </article>

        <article class="card">
          <p class="eyebrow">Route Spotlight</p>
          <h2 id="routeTitle">Highest-priority corridors</h2>
          <p id="routeNarrative" class="muted">Rows below combine planned station density with route-level service need, friction exposure, and corridor relevance.</p>
          <div class="chip-line" id="spotlightChips"></div>
        </article>
      """ if not map_only else ""}
      {"</aside>" if not map_only else ""}
    </section>
{analytics_sections}
  </div>

  <script>
    const payload = {payload_json};
    const NS = 'http://www.w3.org/2000/svg';
    const svg = document.getElementById('networkMap');
    const width = 980;
    const height = 680;
    const bounds = payload.bounds;
    const state = {{
      status: 'All',
      distributor: 'All',
      routeQuery: '',
      selectedRoute: null,
      selectedStationId: null,
      stationNodes: [],
      routeNodes: [],
    }};

    const metricSites = document.getElementById('metricSites');
    const metricChargers = document.getElementById('metricChargers');
    const metricFriction = document.getElementById('metricFriction');
    const metricBaseline = document.getElementById('metricBaseline');
    const metricEV = document.getElementById('metricEV');
    const metricRoutes = document.getElementById('metricRoutes');
    const impactCoverage = document.getElementById('impactCoverage');
    const impactBaseline = document.getElementById('impactBaseline');
    const impactFriction = document.getElementById('impactFriction');
    const spotlightFriction = document.getElementById('spotlightFriction');
    const filterSummary = document.getElementById('filterSummary');
    const routeSearch = document.getElementById('routeSearch');
    const distributorFilter = document.getElementById('distributorFilter');
    const routeTableBody = document.getElementById('routeTableBody');
    const frictionTableBody = document.getElementById('frictionTableBody');
    const stationAction = document.getElementById('stationAction');
    const stationTitle = document.getElementById('stationTitle');
    const stationNarrative = document.getElementById('stationNarrative');
    const stationDetails = document.getElementById('stationDetails');
    const stationWhy = document.getElementById('stationWhy');
    const routeTitle = document.getElementById('routeTitle');
    const routeNarrative = document.getElementById('routeNarrative');
    const spotlightChips = document.getElementById('spotlightChips');
    const corridorCallouts = document.getElementById('corridorCallouts');

    function project(lon, lat) {{
      const x = ((lon - bounds.minLon) / (bounds.maxLon - bounds.minLon)) * (width - 60) + 30;
      const y = height - ((((lat - bounds.minLat) / (bounds.maxLat - bounds.minLat)) * (height - 60)) + 30);
      return [x, y];
    }}

    function fmt(value) {{
      if (value === null || value === undefined || Number.isNaN(value)) return '—';
      return Number(value).toLocaleString();
    }}

    function fmtMaybe(value, digits = 1) {{
      if (value === null || value === undefined || Number.isNaN(value)) return '—';
      return Number(value).toLocaleString(undefined, {{ minimumFractionDigits: digits, maximumFractionDigits: digits }});
    }}

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
      while (attempts < 26) {{
        const conflict = placed.find((point) => Math.hypot(point.x - x, point.y - y) < point.r + minGap);
        if (!conflict) return [x, y];
        attempts += 1;
        const angle = attempts * 1.85;
        const offset = minGap * (0.4 + attempts / 9);
        x = baseX + Math.cos(angle) * offset;
        y = baseY + Math.sin(angle) * offset;
      }}
      return [x, y];
    }}

    function makePath(line) {{
      return line.map(([lon, lat], idx) => {{
        const [x, y] = project(lon, lat);
        return `${{idx === 0 ? 'M' : 'L'}}${{x.toFixed(2)}},${{y.toFixed(2)}}`;
      }}).join(' ');
    }}

    function buildBaseMap() {{
      add('rect', {{ x: 0, y: 0, width, height, fill: '#f8f4ed' }}, svg);
      const roadsGroup = add('g', {{ id: 'roadsGroup' }}, svg);
      payload.roads.forEach((line) => {{
        if (!line.length) return;
        add('path', {{
          d: makePath(line),
          fill: 'none',
          stroke: getComputedStyle(document.documentElement).getPropertyValue('--route').trim(),
          'stroke-width': '0.85',
          'stroke-linecap': 'round',
          opacity: '0.9',
        }}, roadsGroup);
      }});

      const routeGroup = add('g', {{ id: 'routeHighlightGroup' }}, svg);
      Object.entries(payload.routeLines).forEach(([route, lines]) => {{
        lines.forEach((line) => {{
          if (!line.length) return;
          const node = add('path', {{
            d: makePath(line),
            fill: 'none',
            stroke: getComputedStyle(document.documentElement).getPropertyValue('--route-highlight').trim(),
            'stroke-width': '2.4',
            'stroke-linecap': 'round',
            opacity: '0',
            'data-route': route,
          }}, routeGroup);
          state.routeNodes.push(node);
        }});
      }});

      const stationGroup = add('g', {{ id: 'stationGroup' }}, svg);
      const placed = [];
      payload.stations.forEach((station) => {{
        const [baseX, baseY] = project(station.longitude, station.latitude);
        const radius = Math.max(5, 4 + Number(station.n_chargers_proposed) * 0.45);
        const [cx, cy] = spreadPoint(baseX, baseY, radius, placed);
        placed.push({{ x: cx, y: cy, r: radius }});

        const group = add('g', {{
          'data-location-id': station.location_id,
          'data-route': station.route_segment,
          'data-status': station.grid_status,
          'data-distributor': station.distributor_network,
        }}, stationGroup);

        if (Math.hypot(cx - baseX, cy - baseY) > 1.5) {{
          add('line', {{
            x1: baseX,
            y1: baseY,
            x2: cx,
            y2: cy,
            stroke: 'rgba(34,49,66,0.18)',
            'stroke-width': '0.9',
            'stroke-dasharray': '2 3',
          }}, group);
        }}

        if (station.business_share > 0) {{
          add('circle', {{
            cx,
            cy,
            r: radius + 4 + station.business_share * 8,
            fill: 'rgba(209,145,70,0.08)',
            stroke: `rgba(209,145,70, ${{(0.16 + station.business_share * 0.44).toFixed(2)}})`,
            'stroke-width': `${{(1.1 + station.business_share * 1.5).toFixed(2)}}`,
          }}, group);
        }}

        const frictionRing = station.grid_status === 'Sufficient' ? null : add('circle', {{
          cx,
          cy,
          r: radius + 3.5,
          fill: 'none',
          stroke: station.color,
          'stroke-width': '1.2',
          opacity: '0.45',
        }}, group);

        const circle = add('circle', {{
          cx,
          cy,
          r: radius,
          fill: station.color,
          stroke: 'rgba(255,255,255,0.96)',
          'stroke-width': '1.6',
          opacity: '0.95',
          tabindex: 0,
          role: 'button',
          'aria-label': `${{station.location_id}} on ${{station.route_segment}}`,
        }}, group);

        const title = add('title', {{}}, circle);
        title.textContent = `${{station.location_id}} · ${{station.route_segment}} · ${{station.n_chargers_proposed}} chargers · ${{station.grid_status}}`;

        const selectStation = () => {{
          state.selectedStationId = station.location_id;
          renderStationDetail(station);
          if (state.selectedRoute !== station.route_segment) {{
            state.selectedRoute = station.route_segment;
            renderRouteFocus();
            refreshMap();
          }}
        }};
        circle.addEventListener('mouseenter', () => renderStationDetail(station));
        circle.addEventListener('focus', () => renderStationDetail(station));
        circle.addEventListener('click', selectStation);

        state.stationNodes.push({{
          station,
          group,
          circle,
          frictionRing,
          radius,
        }});
      }});
    }}

    function filteredStations() {{
      return payload.stations.filter((station) => {{
        const statusOk =
          state.status === 'All' ? true :
          state.status === 'Friction only' ? station.grid_status !== 'Sufficient' :
          station.grid_status === state.status;
        const distributorOk = state.distributor === 'All' ? true : station.distributor_network === state.distributor;
        const query = state.routeQuery.trim().toLowerCase();
        const routeOk = !query ? true : station.route_segment.toLowerCase().includes(query);
        return statusOk && distributorOk && routeOk;
      }});
    }}

    function refreshMap() {{
      const visibleIds = new Set(filteredStations().map((station) => station.location_id));
      state.stationNodes.forEach((node) => {{
        const visible = visibleIds.has(node.station.location_id);
        node.group.style.display = visible ? '' : 'none';
        node.circle.setAttribute('opacity', visible ? (state.selectedStationId === node.station.location_id ? '1' : '0.95') : '0');
        node.circle.setAttribute('stroke-width', state.selectedStationId === node.station.location_id ? '3' : '1.6');
      }});

      state.routeNodes.forEach((node) => {{
        const visible = !!state.selectedRoute && node.getAttribute('data-route') === state.selectedRoute;
        node.setAttribute('opacity', visible ? '0.92' : '0');
      }});

      const filtered = filteredStations();
      const uniqueRoutes = new Set(filtered.map((row) => row.route_segment)).size;
      const chargers = filtered.reduce((acc, row) => acc + Number(row.n_chargers_proposed), 0);
      const summaryRoute = state.selectedRoute ? ` Highlighting ${{state.selectedRoute}}.` : '';
      if (filterSummary) {{
        filterSummary.textContent = filtered.length
          ? `Showing ${{filtered.length.toLocaleString()}} sites and ${{chargers.toLocaleString()}} chargers across ${{uniqueRoutes.toLocaleString()}} corridors.${{summaryRoute}}`
          : 'No stations match the current filter combination.';
      }}
    }}

    function renderMetrics() {{
      if (metricSites) metricSites.textContent = payload.metrics.proposed_sites.toLocaleString();
      if (metricChargers) metricChargers.textContent = payload.metrics.total_chargers.toLocaleString();
      if (metricFriction) metricFriction.textContent = payload.metrics.friction_points.toLocaleString();
      if (metricBaseline) metricBaseline.textContent = payload.metrics.existing_baseline.toLocaleString();
      if (metricEV) metricEV.textContent = payload.metrics.ev_2027.toLocaleString();
      if (metricRoutes) metricRoutes.textContent = payload.metrics.route_count.toLocaleString();
      if (impactCoverage) impactCoverage.textContent = payload.metrics.route_count.toLocaleString();
      if (impactBaseline) impactBaseline.textContent = payload.metrics.existing_baseline.toLocaleString();
      if (impactFriction) impactFriction.textContent = `${{payload.metrics.friction_share.toFixed(1)}}%`;
      if (spotlightFriction) spotlightFriction.textContent = `${{payload.metrics.friction_share.toFixed(1)}}%`;
    }}

    function renderStatusPills() {{
      const statuses = ['All', 'Sufficient', 'Moderate', 'Congested', 'Friction only'];
      const container = document.getElementById('statusPills');
      container.innerHTML = '';
      statuses.forEach((status) => {{
        const button = document.createElement('button');
        button.className = `pill${{state.status === status ? ' active' : ''}}`;
        button.textContent = status;
        button.addEventListener('click', () => {{
          state.status = status;
          renderStatusPills();
          refreshMap();
        }});
        container.appendChild(button);
      }});
    }}

    function renderDistributorFilter() {{
      if (!distributorFilter) return;
      const options = ['All', ...new Set(payload.stations.map((row) => row.distributor_network).filter(Boolean))];
      distributorFilter.innerHTML = options
        .map((option) => `<option value="${{option}}">${{option === 'All' ? 'All distributors' : option}}</option>`)
        .join('');
      distributorFilter.value = state.distributor;
      distributorFilter.addEventListener('change', (event) => {{
        state.distributor = event.target.value;
        refreshMap();
      }});
    }}

    function renderStationDetail(station) {{
      if (!stationAction || !stationTitle || !stationNarrative || !stationDetails || !stationWhy) return;
      stationAction.innerHTML = `<span class="action-chip" style="background:${{station.color}}">${{station.action_label}}</span>`;
      stationTitle.textContent = `${{station.location_id}} · ${{station.route_segment}}`;
      stationNarrative.textContent =
        station.grid_status === 'Sufficient'
          ? 'Immediate-build candidate in the current grid matching logic.'
          : station.grid_status === 'Moderate'
            ? 'Strong candidate for phased rollout once local reinforcement or sequencing is agreed.'
            : 'Important corridor need, but the dashboard frames this site as reinforcement-led.';
      const details = [
        ['Route segment', station.route_segment],
        ['Proposed chargers', fmt(station.n_chargers_proposed)],
        ['Grid status', station.grid_status],
        ['Recommended action', station.action_label],
        ['Distributor', station.distributor_network || '—'],
        ['Estimated demand', station.estimated_demand_kw ? `${{fmt(station.estimated_demand_kw)}} kW` : '—'],
        ['Available capacity', station.available_capacity_kw ? `${{fmt(station.available_capacity_kw)}} kW` : '—'],
        ['Grid node distance', station.distance_to_grid_node_km !== null ? `${{fmtMaybe(station.distance_to_grid_node_km, 2)}} km` : '—'],
        ['Business fit score', station.business_score !== null ? fmtMaybe(station.business_score, 2) : '—'],
        ['Coordinates', `${{fmtMaybe(station.latitude, 4)}}, ${{fmtMaybe(station.longitude, 4)}}`],
      ];
      stationDetails.innerHTML = details.map(([key, value]) => `
        <div class="detail-row">
          <div class="key">${{key}}</div>
          <div>${{value}}</div>
        </div>
      `).join('');

      const reasons = [];
      if (station.service_need_score !== null && station.service_need_score >= 0.45) {{
        reasons.push(`This route scores strongly on service need (${{station.service_need_score.toFixed(3)}}), which means it stands out once corridor span, strategic relevance, and baseline scarcity are considered together.`);
      }}
      if (station.traffic_imd_total !== null && station.traffic_imd_total >= 20000) {{
        reasons.push(`Traffic intensity on this corridor is high (${{fmt(station.traffic_imd_total)}} annual map units in the matched MITMA layer), reinforcing the case for a meaningful interurban stop.`);
      }}
      if (station.business_support_score !== null && station.business_support_score >= 300) {{
        reasons.push(`Nearby stop-quality proxies are strong, suggesting this location is more than a geometric waypoint and could support a credible charging stop environment.`);
      }}
      if (station.market_access_population !== null && station.market_access_population >= 500000) {{
        reasons.push(`Market-access signals are material here, with a large nearby population base reinforcing likely usage and service relevance.`);
      }} else if (station.tourism_overnight_stays !== null && station.tourism_overnight_stays >= 10000000) {{
        reasons.push(`Tourism pressure is substantial around this corridor, so the site is reinforced by seasonal and visitor-driven demand rather than only local traffic.`);
      }}
      if (station.grid_status !== 'Sufficient') {{
        reasons.push(`The nearest published grid node does not comfortably absorb the proposed load, which is why this location appears in the phased or reinforcement-led part of the rollout.`);
      }}
      if (!reasons.length) {{
        reasons.push('This site survives the full siting logic as a balanced candidate across corridor relevance, current coverage, and grid feasibility.');
      }}
      stationWhy.innerHTML = reasons.slice(0, 4).map((reason) => `<div class="reason-item">${{reason}}</div>`).join('');
    }}

    function renderStatusStack() {{
      const container = document.getElementById('statusStack');
      if (!container) return;
      container.innerHTML = payload.statusSummary.map((row) => `
        <div class="status-row">
          <div class="status-head">
            <div>
              <div style="display:flex; align-items:center; gap:10px;">
                <span class="action-chip" style="background:${{row.color}}">${{row.action}}</span>
                <strong>${{row.status}}</strong>
              </div>
              <p class="muted" style="margin-top:8px;">${{row.description}}</p>
            </div>
            <div style="text-align:right;">
              <div style="font-size:1.6rem;">${{fmt(row.sites)}}</div>
              <div class="muted">sites</div>
            </div>
          </div>
          <div class="status-meta">
            <span>${{fmt(row.chargers)}} chargers</span>
            <span>${{fmt(row.demand_kw)}} kW planned demand</span>
            <span>${{fmtMaybe(row.average_chargers, 1)}} chargers / site</span>
            <span>${{fmtMaybe(row.share_pct, 1)}}% of proposal</span>
          </div>
          <div class="progress"><span style="width:${{Math.min(row.share_pct, 100)}}%; background:${{row.color}};"></span></div>
        </div>
      `).join('');
    }}

    function renderRouteFocus(routeName = null) {{
      const selected = routeName
        ? payload.routes.find((row) => row.route === routeName)
        : state.selectedRoute
          ? payload.routes.find((row) => row.route === state.selectedRoute)
          : payload.routes[0];
      if (!selected) return;

      state.selectedRoute = selected.route;
      if (routeTitle && routeNarrative) {{
        routeTitle.textContent = `${{selected.route}} corridor spotlight`;
        routeNarrative.textContent =
          `${{fmt(selected.sites)}} proposed sites and ${{fmt(selected.chargers)}} chargers along approximately ` +
          `${{selected.length_km ? fmtMaybe(selected.length_km, 1) + ' km' : 'the mapped corridor'}}. ` +
          `${{selected.friction_points ? fmt(selected.friction_points) + ' of those locations fall into the friction-point conversation.' : 'This corridor is relatively cleaner on the current grid view.'}}`;
      }}

      document.querySelectorAll('.route-row').forEach((row) => {{
        row.classList.toggle('active', row.dataset.route === selected.route);
      }});
      refreshMap();
    }}

    function renderRouteTable() {{
      if (!corridorCallouts || !routeTableBody || !spotlightChips) return;
      const maxChargers = Math.max(...payload.routes.map((row) => Number(row.chargers || 0)), 1);
      corridorCallouts.innerHTML = payload.routes.slice(0, 3).map((row) => `
        <article class="corridor-card" data-route="${{row.route}}">
          <div class="route-name"><strong>${{row.route}}</strong></div>
          <p>${{row.friction_points ? 'High-priority corridor with visible grid tension.' : 'High-priority corridor with relatively cleaner immediate deployability.'}}</p>
          <div class="corridor-meta">
            <span>${{fmt(row.sites)}} sites</span>
            <span>${{fmt(row.chargers)}} chargers</span>
            <span>${{row.service_need_score ? 'score ' + fmtMaybe(row.service_need_score, 3) : 'priority corridor'}}</span>
          </div>
        </article>
      `).join('');
      corridorCallouts.querySelectorAll('.corridor-card').forEach((card) => {{
        card.addEventListener('click', () => renderRouteFocus(card.dataset.route));
      }});

      routeTableBody.innerHTML = payload.routes.map((row) => `
        <tr class="route-row" data-route="${{row.route}}">
          <td>
            <strong>${{row.route}}</strong>
            <div class="muted" style="margin-top:4px;">${{row.length_km ? fmtMaybe(row.length_km, 1) + ' km' : 'Mapped corridor'}}${{row.service_need_score ? ' · score ' + fmtMaybe(row.service_need_score, 3) : ''}}</div>
            <div class="mini-bar"><span style="width:${{Math.max(8, (Number(row.chargers || 0) / maxChargers) * 100)}}%;"></span></div>
          </td>
          <td>${{fmt(row.sites)}}</td>
          <td>${{fmt(row.chargers)}}</td>
          <td>${{fmt(row.friction_points)}}</td>
        </tr>
      `).join('');

      routeTableBody.querySelectorAll('.route-row').forEach((row) => {{
        row.addEventListener('click', () => {{
          renderRouteFocus(row.dataset.route);
        }});
      }});

      spotlightChips.innerHTML =
        `<button class="route-chip" data-route="">Full network</button>` +
        payload.routes.slice(0, 6).map((row) =>
          `<button class="route-chip" data-route="${{row.route}}">${{row.route}}</button>`
        ).join('');
      spotlightChips.querySelectorAll('.route-chip').forEach((chip) => {{
        chip.addEventListener('click', () => {{
          if (!chip.dataset.route) {{
            state.selectedRoute = null;
            routeTitle.textContent = 'Highest-priority corridors';
            routeNarrative.textContent = 'Rows below combine planned station density with route-level service need, friction exposure, and corridor relevance.';
            document.querySelectorAll('.route-row').forEach((row) => row.classList.remove('active'));
            refreshMap();
            return;
          }}
          renderRouteFocus(chip.dataset.route);
        }});
      }});
    }}

    function renderFrictionTable() {{
      if (!frictionTableBody) return;
      frictionTableBody.innerHTML = payload.frictionSummary.map((row) => `
        <tr>
          <td><strong>${{row.distributor}}</strong></td>
          <td>${{fmt(row.friction_points)}}</td>
          <td>${{fmt(row.demand_kw)}}</td>
        </tr>
      `).join('');
    }}

    function wireSearch() {{
      if (!routeSearch) return;
      routeSearch.addEventListener('input', (event) => {{
        state.routeQuery = event.target.value;
        if (state.routeQuery.trim()) {{
          state.selectedRoute = null;
        }}
        refreshMap();
      }});
    }}

    buildBaseMap();
    renderMetrics();
    renderStatusPills();
    renderDistributorFilter();
    renderStatusStack();
    renderRouteTable();
    renderFrictionTable();
    wireSearch();

    const defaultStation = payload.stations[0];
    if (defaultStation) {{
      state.selectedStationId = defaultStation.location_id;
      renderStationDetail(defaultStation);
    }}
    refreshMap();
  </script>
</body>
</html>
"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
