"""Generate the polished offline datathon dashboard from current submission assets."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from scripts.generate_submission_package import (
    DEFAULT_INPUT,
    enrich_route_summary_for_planning,
    filter_interurban_routes,
    load_business_context,
    load_external_route_baseline,
    load_roads_dataset,
    summarize_routes,
)
from src.data.external_sources import enrich_route_summary_with_baseline, enrich_route_summary_with_traffic
from src.visualization.offline_dashboard import build_offline_dashboard


def main() -> None:
    roads_gdf = filter_interurban_routes(load_roads_dataset(DEFAULT_INPUT))
    external_dir = ROOT / "data" / "external"

    route_summary = summarize_routes(roads_gdf)
    baseline_by_route, _, _ = load_external_route_baseline(external_dir)
    business_context, _ = load_business_context(external_dir)
    traffic_by_route_path = external_dir / "mitma_traffic_by_route.csv"
    if traffic_by_route_path.exists():
        route_summary = enrich_route_summary_with_traffic(route_summary, pd.read_csv(traffic_by_route_path))
    route_summary = enrich_route_summary_with_baseline(route_summary, baseline_by_route)
    route_summary = route_summary.copy()
    if business_context is not None:
        from scripts.generate_submission_package import enrich_route_summary_with_business

        route_summary = enrich_route_summary_with_business(route_summary, business_context)
    route_summary = enrich_route_summary_for_planning(route_summary)

    file_1 = pd.read_csv(ROOT / "data" / "submission" / "File 1.csv")
    file_2 = pd.read_csv(ROOT / "data" / "submission" / "File 2.csv")
    file_3 = pd.read_csv(ROOT / "data" / "submission" / "File 3.csv")

    friction_cols = [
        "latitude",
        "longitude",
        "route_segment",
        "distributor_network",
        "estimated_demand_kw",
        "grid_status",
    ]
    merged = file_2.merge(file_3[friction_cols], on=["latitude", "longitude", "route_segment", "grid_status"], how="left")

    build_offline_dashboard(
        file_1=file_1,
        stations_df=merged,
        file_3=file_3,
        roads_gdf=roads_gdf,
        route_summary=route_summary,
        output_path=ROOT / "maps" / "dashboard.html",
        title="Iberdrola Interurban Charging Network Dashboard",
        embed_explorer=True,
        explorer_path="offline_scenario_explorer.html",
    )
    print(f"Saved: {ROOT / 'maps' / 'dashboard.html'}")


if __name__ == "__main__":  # pragma: no cover
    main()
