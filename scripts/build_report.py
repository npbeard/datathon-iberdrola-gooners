"""Generate datathon-facing analytical report artifacts from project outputs."""

from pathlib import Path
from datetime import datetime

import geopandas as gpd
import pandas as pd


def generate_report(output_dir: str = 'docs') -> str:
    """Generate a concise analytical report from current project outputs."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    data_processed = Path('data/processed')

    try:
        gdf = gpd.read_parquet(data_processed / 'roads_processed_gdf.parquet')
    except FileNotFoundError:
        return "Error: Processed data not found. Run pipeline first."

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    file_1_path = Path("data/submission/File 1.csv")
    file_2_path = Path("data/submission/File 2.csv")
    file_3_path = Path("data/submission/File 3.csv")

    file_1 = pd.read_csv(file_1_path) if file_1_path.exists() else pd.DataFrame()
    file_2 = pd.read_csv(file_2_path) if file_2_path.exists() else pd.DataFrame()
    file_3 = pd.read_csv(file_3_path) if file_3_path.exists() else pd.DataFrame()

    total_sites = int(file_1["total_proposed_stations"].iloc[0]) if not file_1.empty else 0
    total_baseline = int(file_1["total_existing_stations_baseline"].iloc[0]) if not file_1.empty else 0
    total_friction = int(file_1["total_friction_points"].iloc[0]) if not file_1.empty else 0
    total_evs = int(file_1["total_ev_projected_2027"].iloc[0]) if not file_1.empty else 0
    top_routes = ""
    if not file_2.empty:
        route_table = (
            file_2.groupby("route_segment", as_index=False)
            .agg(proposed_sites=("location_id", "count"), chargers=("n_chargers_proposed", "sum"))
            .sort_values(["proposed_sites", "chargers"], ascending=[False, False])
            .head(8)
        )
        top_routes = "\n".join(
            f"  • {row.route_segment}: {int(row.proposed_sites)} sites / {int(row.chargers)} chargers"
            for row in route_table.itertuples(index=False)
        )

    report = f"""
================================================================================
                    RTIG ROADS NETWORK ANALYSIS REPORT
                        IE Iberdrola Datathon 2026
                            Generated: {timestamp}
================================================================================

1. EXECUTIVE SUMMARY
────────────────────────────────────────────────────────────────────────────────

This project proposes a 2027 interurban charging network for Spain that combines
official transport geometry, existing public charging baseline, EV electrification
forecasting, and distributor grid-capacity constraints into one reproducible package.

Current package snapshot:
  • Proposed charging sites: {total_sites:,}
  • Existing interurban baseline sites: {total_baseline:,}
  • Friction points needing phased deployment: {total_friction:,}
  • Projected EV stock proxy for 2027: {total_evs:,}


2. DATASET OVERVIEW
────────────────────────────────────────────────────────────────────────────────

Network Statistics:
  • Number of RTIG segments analysed: {len(gdf):,}
  • Total network length: {gdf['length_km'].sum():,.0f} km
  • Average segment length: {gdf['length_km'].mean():.2f} km
  • TEN-T segments: {(gdf['is_tent'] == 1).sum()} ({(gdf['is_tent'] == 1).sum() / len(gdf) * 100:.1f}%)
  • Geometry validity: {(gdf.geometry.is_valid.sum() / len(gdf) * 100):.1f}%


3. METHODOLOGY
────────────────────────────────────────────────────────────────────────────────

The final proposal follows four planning steps:
  1. Filter the RTIG network to interurban A-, AP-, and N- corridors aligned with the brief.
  2. Measure each corridor's strategic need using route span, total length, TEN-T share,
     geometric complexity, and scarcity of existing matched charging supply.
  3. Convert corridor need into dynamic station spacing instead of using one national
     fixed gap. High-need corridors receive denser spacing; routes with stronger
     baseline coverage receive more spacing credit.
  4. Classify grid viability by matching each proposed site to the nearest published
     distributor node and comparing available capacity against charger demand
     (`n_chargers_proposed × 150 kW`).


4. OUTPUTS FOR THE JURY
────────────────────────────────────────────────────────────────────────────────

Required CSV package:
  ✓ File 1.csv - global network KPI scorecard
  ✓ File 2.csv - proposed charging locations
  ✓ File 3.csv - friction points

Recommended visual deliverables:
  ✓ maps/proposed_charging_network.html - self-contained offline map
  ✓ maps/offline_scenario_explorer.html - scenario-testing companion for the pitch

Top routes in the current proposal:
{top_routes or '  • Route aggregation will appear once File 2.csv is available.'}


5. STRATEGIC INTERPRETATION
────────────────────────────────────────────────────────────────────────────────

The proposal is intentionally built as a phased investment plan:
  • Sufficient nodes are "build now" candidates.
  • Moderate nodes are "build with limited reinforcement / staged rollout" candidates.
  • Congested nodes are "protect the site, solve the grid first" candidates.

This framing matters for Iberdrola because it separates mobility opportunity from
electrical feasibility instead of forcing every route into one go/no-go decision.


6. LIMITATIONS TO DISCLOSE
────────────────────────────────────────────────────────────────────────────────

  • Corridor demand is still estimated from public proxies rather than proprietary traffic traces.
  • The 2027 EV figure is a stock proxy built from official registrations and forecasted additions.
  • Nearest-node grid matching is a planning approximation, not a formal access study.


7. RECOMMENDED SUBMISSION STORY
────────────────────────────────────────────────────────────────────────────────

Lead the jury through three messages:
  1. We minimise new sites by giving credit to existing interurban infrastructure.
  2. We do not confuse "good mobility location" with "grid-feasible location".
  3. We give Iberdrola a rollout queue, not just a map.


================================================================================
                              END OF REPORT
                    Report generated: {timestamp}
================================================================================
"""

    report_path = output_dir / 'analysis_report.txt'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"Report saved to {report_path}")
    return str(report_path)


if __name__ == '__main__':  # pragma: no cover
    report_path = generate_report()
    print(f"\n✓ Report generated: {report_path}")
