import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point

import scripts.generate_submission_package as gsp
import scripts.run_pipeline as run_pipeline
from scripts.build_offline_scenario_explorer import build_scenarios, geometry_to_lines, render_html
from scripts.build_report import generate_report
from scripts.fetch_external_data import main as fetch_external_data_main
from scripts.scrub_notebook_paths import main as scrub_notebook_paths_main, scrub_text
from scripts.validate_submission import (
    FILE_1_COLUMNS,
    FILE_2_COLUMNS,
    FILE_3_COLUMNS,
    load_csv,
    validate_columns,
    validate_submission,
)


def sample_roads_gdf():
    roads = gpd.GeoDataFrame(
        {
            "id": [1, 2, 3],
            "carretera": ["A-1", "A-1", "AP-7"],
            "pk_inicio": [0.0, 90.0, 0.0],
            "pk_fin": [90.0, 220.0, 240.0],
            "length_km": [90.0, 130.0, 240.0],
            "num_vertices": [2, 2, 2],
            "curve_complexity": [2.0, 3.0, 4.0],
            "is_tent": [1, 0, 1],
            "geometry": [
                LineString([(-3.9, 40.4), (-3.0, 41.0)]),
                LineString([(-3.0, 41.0), (-2.0, 42.0)]),
                LineString([(0.5, 41.5), (2.5, 42.5)]),
            ],
        },
        crs="EPSG:4326",
    )
    return roads


def write_submission_files(submission_dir: Path, file_1: pd.DataFrame, file_2: pd.DataFrame, file_3: pd.DataFrame):
    submission_dir.mkdir(parents=True, exist_ok=True)
    file_1.to_csv(submission_dir / "File 1.csv", index=False)
    file_2.to_csv(submission_dir / "File 2.csv", index=False)
    file_3.to_csv(submission_dir / "File 3.csv", index=False)


def test_generate_submission_helpers_and_main(tmp_path, monkeypatch):
    roads = sample_roads_gdf()
    route_summary = gsp.summarize_routes(roads)
    assert route_summary.iloc[0]["carretera"] == "AP-7"

    assert gsp.assign_proxy_grid_status(0, "relaxed") == "Moderate"
    assert gsp.assign_proxy_grid_status(9, "balanced") == "Moderate"
    assert gsp.assign_proxy_grid_status(21, "cautious") == "Congested"
    assert gsp.assign_proxy_distributor(43.2, -4.2) == "Viesgo"
    assert gsp.assign_proxy_distributor(37.5, -3.0) == "Endesa"
    assert gsp.assign_proxy_distributor(40.4, -3.7) == "i-DE"

    positions = gsp.station_positions(route_summary.iloc[0], spacing_km=100, min_route_span_km=50)
    assert positions
    assert gsp.station_positions(route_summary.iloc[0], spacing_km=100, min_route_span_km=1000) == []
    short_route = route_summary.copy()
    short_route.loc[0, ["min_pk", "max_pk"]] = [10.0, 10.0]
    assert gsp.station_positions(short_route.iloc[0], spacing_km=50, min_route_span_km=0) == [10.0]

    point = gsp.interpolate_station_point(roads[roads["carretera"] == "A-1"], 120.0)
    assert round(point.y, 3) > 40
    point_fallback = gsp.interpolate_station_point(roads[roads["carretera"] == "A-1"], 999.0)
    assert round(point_fallback.y, 3) > 40
    assert gsp.chargers_for_route(route_summary.iloc[0], "aggressive") >= 8
    assert gsp.chargers_for_route(route_summary.iloc[1], "conservative") >= 2
    medium_row = pd.Series({"tent_share": 0.0, "total_length_km": 250.0})
    low_row = pd.Series({"tent_share": 0.0, "total_length_km": 100.0})
    assert gsp.chargers_for_route(medium_row, "balanced") == 6
    assert gsp.chargers_for_route(low_row, "balanced") == 4
    assert gsp.assign_proxy_grid_status(50, "balanced") == "Sufficient"

    zero_span_segments = roads.iloc[[0]].copy()
    zero_span_segments["pk_inicio"] = 10.0
    zero_span_segments["pk_fin"] = 10.0
    zero_point = gsp.interpolate_station_point(zero_span_segments, 10.0)
    assert round(zero_point.x, 3) < 0

    file_2 = gsp.build_file_2(roads, route_summary, spacing_km=100, min_route_span_km=50, charger_policy="balanced", grid_policy="balanced")
    assert FILE_2_COLUMNS == file_2.columns.tolist()
    assert not file_2.empty

    skipped_summary = route_summary.copy()
    skipped_summary.loc[:, "min_pk"] = 0.0
    skipped_summary.loc[:, "max_pk"] = 10.0
    skipped = gsp.build_file_2(roads, skipped_summary, spacing_km=100, min_route_span_km=50)
    assert skipped.empty

    file_3 = gsp.build_file_3(file_2, charger_power_kw=150)
    assert FILE_3_COLUMNS == file_3.columns.tolist()
    empty_file_3 = gsp.build_file_3(file_2[file_2["grid_status"] == "Sufficient"], charger_power_kw=150)
    assert empty_file_3.empty

    file_1 = gsp.build_file_1(file_2, file_3, baseline_existing_stations=5, total_ev_projected_2027=123456)
    assert FILE_1_COLUMNS == file_1.columns.tolist()

    map_path = tmp_path / "map.html"
    gsp.save_map(file_2, map_path)
    assert map_path.exists()
    empty_map_path = tmp_path / "empty_map.html"
    gsp.save_map(pd.DataFrame(columns=FILE_2_COLUMNS), empty_map_path)
    assert empty_map_path.exists()

    gsp.save_assumptions_note(tmp_path, "loaded:x.csv", "missing:y.csv", "loaded:grid.csv", 120)
    assert "120" in (tmp_path / "ASSUMPTIONS.md").read_text(encoding="utf-8")

    root = tmp_path / "repo"
    (root / "config").mkdir(parents=True)
    (root / "data" / "external").mkdir(parents=True)
    (root / "maps").mkdir(parents=True)
    (root / "data" / "submission").mkdir(parents=True)
    (root / "config" / "settings.yaml").write_text(
        """
datathon:
  target_year: 2027
  charger_power_kw: 150
  station_spacing_km: 120
  min_route_span_km: 50
  baseline_existing_stations_default: 0
  total_ev_projected_2027_default: 400000
  output_dir: "data/submission"
  map_output: "maps/proposed_charging_network.html"
datasets: {}
""".strip(),
        encoding="utf-8",
    )
    pd.DataFrame([{"total_existing_stations_baseline": 7}]).to_csv(
        root / "data" / "external" / "existing_interurban_stations.csv", index=False
    )
    pd.DataFrame([{"total_ev_projected_2027": 654321}]).to_csv(
        root / "data" / "external" / "ev_projection_2027.csv", index=False
    )
    pd.DataFrame([{"wrong_column": 1}]).to_csv(root / "data" / "external" / "invalid.csv", index=False)
    assert gsp.load_optional_scalar(root / "data" / "external" / "invalid.csv", "missing", 3) == (3, "invalid:invalid.csv")
    assert gsp.load_optional_scalar(root / "data" / "external" / "missing.csv", "missing", 4) == (4, "missing:missing.csv")
    assert gsp.load_config()["datathon"]["station_spacing_km"] == 120

    monkeypatch.setattr(gsp, "ROOT", root)
    monkeypatch.setattr(gsp, "DEFAULT_INPUT", root / "data" / "processed" / "roads_processed_gdf.parquet")
    monkeypatch.setattr(gsp, "load_roads_dataset", lambda _: roads)
    monkeypatch.setattr(gsp, "try_build_official_external_inputs", lambda *args, **kwargs: {})
    monkeypatch.setattr(gsp, "load_grid_capacity_bundle", lambda *_: pd.DataFrame())

    gsp.main()

    assert (root / "data" / "submission" / "File 1.csv").exists()
    assert (root / "maps" / "proposed_charging_network.html").exists()


def test_validate_submission_and_run_pipeline(tmp_path, monkeypatch):
    submission_dir = tmp_path / "submission"
    file_2 = pd.DataFrame(
        [
            {
                "location_id": "IBE_001",
                "latitude": 40.5,
                "longitude": -3.7,
                "route_segment": "A-1",
                "n_chargers_proposed": 4,
                "grid_status": "Moderate",
            }
        ]
    )
    file_3 = pd.DataFrame(
        [
            {
                "bottleneck_id": "FRIC_001",
                "latitude": 40.5,
                "longitude": -3.7,
                "route_segment": "A-1",
                "distributor_network": "i-DE",
                "estimated_demand_kw": 600,
                "grid_status": "Moderate",
            }
        ]
    )
    file_1 = pd.DataFrame(
        [
            {
                "total_proposed_stations": 1,
                "total_existing_stations_baseline": 0,
                "total_friction_points": 1,
                "total_ev_projected_2027": 1000,
            }
        ]
    )
    write_submission_files(submission_dir, file_1, file_2, file_3)

    import scripts.validate_submission as vs

    monkeypatch.setattr(vs, "SUBMISSION_DIR", submission_dir)
    assert load_csv(submission_dir / "File 1.csv").equals(file_1)
    assert validate_columns(file_1, FILE_1_COLUMNS, "File 1.csv") == []
    assert validate_columns(file_1, ["bad"], "File 1.csv")
    assert validate_submission() == []

    broken = file_3.copy()
    broken.loc[0, "estimated_demand_kw"] = 1
    write_submission_files(submission_dir, file_1, file_2, broken)
    problems = validate_submission()
    assert any("expected 600" in problem for problem in problems)

    write_submission_files(submission_dir, file_1, file_2, file_3.assign(grid_status="Sufficient"))
    assert any("invalid grid_status values" in problem for problem in validate_submission())

    bad_file_2 = file_2.assign(grid_status="Bad")
    write_submission_files(submission_dir, file_1, bad_file_2, file_3)
    issues = validate_submission()
    assert any("File 2.csv: invalid grid_status values" in problem for problem in issues)

    write_submission_files(submission_dir, file_1.assign(total_proposed_stations=99), file_2, file_3)
    assert any("total_proposed_stations" in problem for problem in validate_submission())

    write_submission_files(submission_dir, file_1.assign(total_friction_points=99), file_2, file_3)
    assert any("total_friction_points" in problem for problem in validate_submission())

    write_submission_files(submission_dir, pd.concat([file_1, file_1], ignore_index=True), file_2, file_3)
    assert any("expected exactly 1 row" in problem for problem in validate_submission())

    bad_file_3 = file_3.assign(distributor_network="BadGrid")
    write_submission_files(submission_dir, file_1, file_2, bad_file_3)
    assert any("invalid distributor_network" in problem for problem in validate_submission())

    unmatched_file_3 = file_3.assign(latitude=99.0)
    write_submission_files(submission_dir, file_1, file_2, unmatched_file_3)
    issues = validate_submission()
    assert any("does not match any File 2 station" in problem for problem in issues)

    empty_file_3 = pd.DataFrame(columns=FILE_3_COLUMNS)
    write_submission_files(submission_dir, file_1.assign(total_friction_points=0), file_2, empty_file_3)
    assert validate_submission() == []

    (submission_dir / "File 1.csv").unlink()
    try:
        load_csv(submission_dir / "File 1.csv")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Expected FileNotFoundError for missing CSV")

    sample_gdf = sample_roads_gdf()
    sample_pdf = pd.DataFrame({"length_km": [1]}).pipe(lambda df: df)

    monkeypatch.setattr(run_pipeline, "run_download", lambda **kwargs: ("dummy.geojson", 3))
    monkeypatch.setattr(run_pipeline, "run_preprocessing", lambda **kwargs: (sample_gdf, sample_pdf))
    gdf, pdf = run_pipeline.main()
    assert len(gdf) == 3
    assert pdf.equals(sample_pdf)


def test_load_roads_dataset_branches(tmp_path, monkeypatch):
    import scripts.generate_submission_package as gsp_local

    missing = tmp_path / "missing.parquet"
    try:
        gsp_local.load_roads_dataset(missing)
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Expected FileNotFoundError for missing parquet")

    roads = sample_roads_gdf().to_crs("EPSG:3857")
    dummy = tmp_path / "dummy.parquet"
    dummy.write_bytes(b"PAR1")
    monkeypatch.setattr(gpd, "read_parquet", lambda _: roads)
    loaded = gsp_local.load_roads_dataset(dummy)
    assert str(loaded.crs).endswith("4326")

    roads_no_crs = sample_roads_gdf().copy()
    roads_no_crs = roads_no_crs.set_crs(None, allow_override=True)
    monkeypatch.setattr(gpd, "read_parquet", lambda _: roads_no_crs)
    dummy2 = tmp_path / "dummy2.parquet"
    dummy2.write_bytes(b"PAR1")
    loaded_no_crs = gsp_local.load_roads_dataset(dummy2)
    assert str(loaded_no_crs.crs).endswith("4326")

    roads_4326 = sample_roads_gdf()
    monkeypatch.setattr(gpd, "read_parquet", lambda _: roads_4326)
    dummy3 = tmp_path / "dummy3.parquet"
    dummy3.write_bytes(b"PAR1")
    loaded_native = gsp_local.load_roads_dataset(dummy3)
    assert str(loaded_native.crs).endswith("4326")


def test_offline_geometry_to_lines_extra_branches():
    import scripts.build_offline_scenario_explorer as boe

    roads = gpd.GeoDataFrame(
        {
            "geometry": [
                None,
                MultiLineString([[(0, 0), (1, 1)], [(1, 1), (2, 2)]]),
                LineString([(0, 0), (0, 0)]),
                Point(0, 0),
            ]
        },
        crs="EPSG:4326",
    )
    lines = boe.geometry_to_lines(roads)
    assert len(lines) >= 2


def test_build_report_and_offline_explorer(tmp_path, monkeypatch):
    report_dir = tmp_path / "docs"
    data_processed = tmp_path / "data" / "processed"
    data_raw = tmp_path / "data" / "raw"
    data_processed.mkdir(parents=True)
    data_raw.mkdir(parents=True)
    raw_file = data_raw / "carreteras_RTIG.geojson"
    raw_file.write_text("{}", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    roads = sample_roads_gdf()
    report_path = generate_report(str(report_dir))
    assert report_path == "Error: Processed data not found. Run pipeline first."

    monkeypatch.setattr(gpd, "read_parquet", lambda _: roads)
    report_path = generate_report(str(report_dir))
    assert Path(report_path).exists()
    assert "RTIG ROADS NETWORK ANALYSIS REPORT" in Path(report_path).read_text(encoding="utf-8")

    import scripts.build_offline_scenario_explorer as boe

    lines = geometry_to_lines(roads)
    assert lines

    root = tmp_path / "repo"
    (root / "config").mkdir(parents=True)
    (root / "data" / "external").mkdir(parents=True)
    (root / "maps").mkdir(parents=True)
    (root / "config" / "settings.yaml").write_text(
        """
datathon:
  charger_power_kw: 150
  min_route_span_km: 50
  baseline_existing_stations_default: 0
  total_ev_projected_2027_default: 400000
datasets: {}
""".strip(),
        encoding="utf-8",
    )

    scenarios = build_scenarios(roads, gsp.summarize_routes(roads), {"charger_power_kw": 150, "min_route_span_km": 50, "baseline_existing_stations_default": 0, "total_ev_projected_2027_default": 100})
    assert len(scenarios) == 27
    html = render_html(lines, scenarios, {"minLon": -4, "minLat": 40, "maxLon": 3, "maxLat": 43})
    assert "Offline Scenario Explorer" in html
    assert "Download File 2" in html

    monkeypatch.setattr(boe, "ROOT", root)
    monkeypatch.setattr(boe, "OUTPUT_PATH", root / "maps" / "offline_scenario_explorer.html")
    monkeypatch.setattr(boe, "DEFAULT_INPUT", root / "data" / "processed" / "roads_processed_gdf.parquet")
    monkeypatch.setattr(boe, "load_roads_dataset", lambda _: roads)
    boe.main()
    assert (root / "maps" / "offline_scenario_explorer.html").exists()


def test_fetch_external_data_script(tmp_path, monkeypatch):
    roads = sample_roads_gdf()

    import scripts.fetch_external_data as fed

    monkeypatch.setattr(fed, "ROOT", tmp_path)
    monkeypatch.setattr(gpd, "read_parquet", lambda _: roads)
    monkeypatch.setattr(
        fed,
        "try_build_official_external_inputs",
        lambda roads_gdf, external_dir, target_year: {
            "baseline_summary_path": external_dir / "existing_interurban_stations_by_route.csv",
            "ev_projection_path": external_dir / "ev_projection_2027.csv",
            "grid_capacity_paths": [external_dir / "edistribucion_capacity_2026_03.csv"],
        },
    )
    monkeypatch.setattr(
        fed,
        "load_grid_capacity_bundle",
        lambda external_dir: pd.DataFrame([{"node_name": "Node A"}]),
    )

    fetch_external_data_main()

    roads_no_crs = roads.set_crs(None, allow_override=True)
    monkeypatch.setattr(gpd, "read_parquet", lambda _: roads_no_crs)
    fetch_external_data_main()

    roads_3857 = roads.to_crs("EPSG:3857")
    monkeypatch.setattr(gpd, "read_parquet", lambda _: roads_3857)
    fetch_external_data_main()


def test_generate_submission_additional_branches(tmp_path, monkeypatch):
    roads = sample_roads_gdf()
    root = tmp_path / "repo"
    (root / "config").mkdir(parents=True)
    (root / "data" / "external").mkdir(parents=True)
    (root / "maps").mkdir(parents=True)
    (root / "data" / "submission").mkdir(parents=True)
    (root / "config" / "settings.yaml").write_text(
        """
datathon:
  target_year: 2027
  charger_power_kw: 150
  station_spacing_km: 120
  min_route_span_km: 50
  baseline_existing_stations_default: 0
  total_ev_projected_2027_default: 400000
  output_dir: "data/submission"
  map_output: "maps/proposed_charging_network.html"
datasets: {}
""".strip(),
        encoding="utf-8",
    )
    pd.DataFrame([{"carretera": "A-1", "existing_station_count": 2}]).to_csv(
        root / "data" / "external" / "existing_interurban_stations_by_route.csv", index=False
    )
    pd.DataFrame([{"total_existing_stations_baseline": 9}]).to_csv(
        root / "data" / "external" / "existing_interurban_stations.csv", index=False
    )
    pd.DataFrame([{"total_ev_projected_2027": 777777}]).to_csv(
        root / "data" / "external" / "ev_projection_2027.csv", index=False
    )

    monkeypatch.setattr(gsp, "ROOT", root)
    monkeypatch.setattr(gsp, "DEFAULT_INPUT", root / "data" / "processed" / "roads_processed_gdf.parquet")
    monkeypatch.setattr(gsp, "load_roads_dataset", lambda _: roads)
    monkeypatch.setattr(gsp, "try_build_official_external_inputs", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("offline")))
    monkeypatch.setattr(
        gsp,
        "load_grid_capacity_bundle",
        lambda *_: pd.DataFrame(
            [
                {
                    "node_name": "Node A",
                    "latitude": 40.5,
                    "longitude": -3.5,
                    "capacity_available_mw": 2.0,
                    "voltage_kv": 66,
                    "distributor_network": "i-DE",
                }
            ]
        ),
    )

    gsp.main()
    assumptions = (root / "data" / "submission" / "ASSUMPTIONS.md").read_text(encoding="utf-8")
    assert "loaded:grid_capacity_files" in assumptions
    missing_baseline = gsp.load_external_route_baseline(root / "missing-external")
    assert missing_baseline[0].empty
    assert missing_baseline[1] == 0
    invalid_external = root / "invalid-external"
    invalid_external.mkdir()
    pd.DataFrame([{"carretera": "A-1", "existing_station_count": 1}]).to_csv(
        invalid_external / "existing_interurban_stations_by_route.csv", index=False
    )
    pd.DataFrame([{"wrong": 1}]).to_csv(invalid_external / "existing_interurban_stations.csv", index=False)
    invalid_baseline = gsp.load_external_route_baseline(invalid_external)
    assert invalid_baseline[1] == 0


def test_offline_explorer_additional_branches(tmp_path, monkeypatch):
    roads = sample_roads_gdf()
    import scripts.build_offline_scenario_explorer as boe

    root = tmp_path / "repo"
    (root / "data" / "external").mkdir(parents=True)
    pd.DataFrame([{"total_existing_stations_baseline": 5}]).to_csv(
        root / "data" / "external" / "existing_interurban_stations.csv", index=False
    )
    pd.DataFrame([{"total_ev_projected_2027": 111111}]).to_csv(
        root / "data" / "external" / "ev_projection_2027.csv", index=False
    )
    pd.DataFrame([{"carretera": "A-1", "existing_station_count": 1}]).to_csv(
        root / "data" / "external" / "existing_interurban_stations_by_route.csv", index=False
    )

    monkeypatch.setattr(boe, "ROOT", root)
    monkeypatch.setattr(
        boe,
        "load_grid_capacity_bundle",
        lambda *_: pd.DataFrame(
            [
                {
                    "node_name": "Node A",
                    "latitude": 40.5,
                    "longitude": -3.5,
                    "capacity_available_mw": 2.0,
                    "voltage_kv": 66,
                    "distributor_network": "i-DE",
                }
            ]
        ),
    )

    scenarios = boe.build_scenarios(
        roads,
        gsp.summarize_routes(roads),
        {
            "charger_power_kw": 150,
            "min_route_span_km": 50,
            "baseline_existing_stations_default": 0,
            "total_ev_projected_2027_default": 100,
        },
    )
    assert scenarios

    pd.DataFrame([{"wrong": 1}]).to_csv(root / "data" / "external" / "existing_interurban_stations.csv", index=False)
    pd.DataFrame([{"wrong": 1}]).to_csv(root / "data" / "external" / "ev_projection_2027.csv", index=False)
    scenarios_with_invalid_scalars = boe.build_scenarios(
        roads,
        gsp.summarize_routes(roads),
        {
            "charger_power_kw": 150,
            "min_route_span_km": 50,
            "baseline_existing_stations_default": 0,
            "total_ev_projected_2027_default": 100,
        },
    )
    assert scenarios_with_invalid_scalars

    monkeypatch.setattr(boe, "OUTPUT_PATH", root / "maps" / "offline_scenario_explorer.html")
    monkeypatch.setattr(boe, "DEFAULT_INPUT", root / "data" / "processed" / "roads_processed_gdf.parquet")
    monkeypatch.setattr(boe, "load_roads_dataset", lambda _: roads)
    monkeypatch.setattr(boe, "load_config", lambda: {"datathon": {"charger_power_kw": 150, "min_route_span_km": 50, "baseline_existing_stations_default": 0, "total_ev_projected_2027_default": 100}})
    boe.main()
    assert (root / "maps" / "offline_scenario_explorer.html").exists()


def test_scrub_notebook_paths_script(tmp_path, monkeypatch):
    import scripts.scrub_notebook_paths as snp

    notebooks_dir = tmp_path / "notebooks"
    notebooks_dir.mkdir()
    nb_path = notebooks_dir / "demo.ipynb"
    repo_like_root = tmp_path
    nb_path.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "cell_type": "code",
                        "metadata": {},
                        "source": [f"open('{repo_like_root}/maps/file.html')"],
                        "outputs": [
                            {
                                "output_type": "stream",
                                "name": "stdout",
                                "text": [
                                    f"{repo_like_root}/data/raw/carreteras_RTIG.geojson\n",
                                    f"/opt/anaconda3/bin/python {repo_like_root}/scripts/run_pipeline.py\n",
                                ],
                            },
                            {
                                "output_type": "display_data",
                                "data": {
                                    "text/plain": [f"{repo_like_root}/maps/file.html", 1],
                                    "text/html": f"<a href='{repo_like_root}/maps/file.html'>map</a>",
                                },
                                "metadata": {},
                            }
                        ],
                    }
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(snp, "ROOT", repo_like_root)
    monkeypatch.setattr(snp, "NOTEBOOKS_DIR", notebooks_dir)
    assert scrub_text(f"{repo_like_root}/maps/test.html").startswith("maps/")
    scrub_notebook_paths_main()
    cleaned = nb_path.read_text(encoding="utf-8")
    assert str(repo_like_root) not in cleaned
    assert "python scripts/run_pipeline.py" in cleaned
    assert snp.scrub_notebook(nb_path) is False
