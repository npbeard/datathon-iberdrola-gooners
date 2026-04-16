"""Fetch and cache the official external datasets used by the datathon package."""

from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from scripts.generate_submission_package import DEFAULT_INPUT
from src.data.external_sources import load_grid_capacity_bundle, try_build_official_external_inputs


def main() -> None:
    roads_gdf = gpd.read_parquet(DEFAULT_INPUT)
    if roads_gdf.crs is None:
        roads_gdf = roads_gdf.set_crs("EPSG:4326")
    elif roads_gdf.crs.to_string() != "EPSG:4326":
        roads_gdf = roads_gdf.to_crs("EPSG:4326")

    result = try_build_official_external_inputs(
        roads_gdf=roads_gdf,
        external_dir=ROOT / "data" / "external",
        target_year=2027,
    )
    grid_nodes = load_grid_capacity_bundle(ROOT / "data" / "external")

    print("Official external inputs cached.")
    print(f"Baseline summary: {result['baseline_summary_path']}")
    print(f"EV projection: {result['ev_projection_path']}")
    print(f"Traffic summary: {result.get('traffic_summary_path', 'not generated')}")
    print(f"Gas stations raw: {result.get('gasolineras_json_path', 'not generated')}")
    print(f"Gas stations matched: {result.get('gasolineras_matched_path', 'not generated')}")
    print(f"INE municipal population: {result.get('ine_population_csv_path', 'not generated')}")
    print(f"INE hotel overnight stays: {result.get('ine_hotel_overnight_json_path', 'not generated')}")
    print(f"INE provincial overnight stays: {result.get('ine_provincial_overnight_csv_path', 'not generated')}")
    print(f"Grid files detected: {len(result['grid_capacity_paths'])}")
    print(f"Standardized grid nodes available: {len(grid_nodes)}")


if __name__ == "__main__":  # pragma: no cover
    main()
