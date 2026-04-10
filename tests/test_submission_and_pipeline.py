import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point

import scripts.generate_submission_package as gsp
import scripts.run_pipeline as run_pipeline
from scripts.build_offline_scenario_explorer import build_scenarios, geometry_to_lines, render_html
from scripts.build_offline_reference_maps import (
    backdrop_features,
    base_styles,
    bounds_from_features as ref_bounds_from_features,
    build_summary as ref_build_summary,
    centroid_points as ref_centroid_points,
    geometry_to_features as ref_geometry_to_features,
    render_heatmap as ref_render_heatmap,
    render_line_map as ref_render_line_map,
)
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
    planning_summary = gsp.enrich_route_summary_for_planning(route_summary)
    assert "service_need_score" in planning_summary.columns
    assert planning_summary["route_hierarchy_score"].between(0.6, 1.0).all()
    assert gsp._minmax_normalize(pd.Series(dtype=float)).empty
    assert gsp._minmax_normalize(pd.Series([5.0, 5.0])).tolist() == [1.0, 1.0]

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
    assert gsp._dynamic_spacing_km(planning_summary.iloc[0], 120) <= 160
    assert gsp._route_target_station_count(planning_summary.iloc[0], 120, 50) >= 1
    assert gsp._target_positions_by_count(planning_summary.iloc[0], 0) == []

    zero_span_segments = roads.iloc[[0]].copy()
    zero_span_segments["pk_inicio"] = 10.0
    zero_span_segments["pk_fin"] = 10.0
    zero_point = gsp.interpolate_station_point(zero_span_segments, 10.0)
    assert round(zero_point.x, 3) < 0
    zero_span_row = pd.Series({"min_pk": 10.0, "max_pk": 10.0})
    assert gsp._target_positions_by_count(zero_span_row, 1) == [10.0]

    file_2 = gsp.build_file_2(roads, planning_summary, spacing_km=100, min_route_span_km=50, charger_policy="balanced", grid_policy="balanced")
    assert FILE_2_COLUMNS == file_2.columns.tolist()
    assert not file_2.empty

    duplicate_rows = pd.DataFrame(
        [
            {"location_id": "X", "latitude": 40.5, "longitude": -3.5, "route_segment": "A-1", "n_chargers_proposed": 4, "grid_status": "Moderate"},
            {"location_id": "Y", "latitude": 40.5, "longitude": -3.5, "route_segment": "A-1", "n_chargers_proposed": 6, "grid_status": "Congested"},
        ]
    )
    deduped = gsp.deduplicate_station_rows(duplicate_rows)
    assert len(deduped) == 1
    assert deduped.loc[0, "n_chargers_proposed"] == 10
    assert deduped.loc[0, "grid_status"] == "Congested"

    skipped_summary = planning_summary.copy()
    skipped_summary.loc[:, "min_pk"] = 0.0
    skipped_summary.loc[:, "max_pk"] = 10.0
    skipped_summary.loc[:, "pk_span_km"] = 10.0
    skipped = gsp.build_file_2(roads, skipped_summary, spacing_km=100, min_route_span_km=50)
    assert skipped.empty

    file_3 = gsp.build_file_3(file_2, charger_power_kw=150)
    assert FILE_3_COLUMNS == file_3.columns.tolist()
    empty_file_3 = gsp.build_file_3(file_2[file_2["grid_status"] == "Sufficient"], charger_power_kw=150)
    assert empty_file_3.empty

    file_1 = gsp.build_file_1(file_2, file_3, baseline_existing_stations=5, total_ev_projected_2027=123456)
    assert FILE_1_COLUMNS == file_1.columns.tolist()

    map_path = tmp_path / "map.html"
    gsp.save_map(file_2, map_path, roads_gdf=roads)
    assert map_path.exists()
    assert "Select a station" in map_path.read_text(encoding="utf-8")
    empty_map_path = tmp_path / "empty_map.html"
    gsp.save_map(pd.DataFrame(columns=FILE_2_COLUMNS), empty_map_path)
    assert empty_map_path.exists()
    assert gsp._geometry_to_lines(None) == []
    assert gsp._map_bounds(pd.DataFrame(columns=FILE_2_COLUMNS), [])["minLon"] == -9.5

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


def test_generate_submission_branch_helpers():
    route_summary = pd.DataFrame(
        [
            {"carretera": "N-2", "total_length_km": 100.0, "pk_span_km": 100.0, "mean_complexity": 1.0, "tent_share": 0.0, "existing_station_count": 0, "coverage_gap_score": 10.0},
            {"carretera": "AP-7N", "total_length_km": 110.0, "pk_span_km": 110.0, "mean_complexity": 1.5, "tent_share": 0.0, "existing_station_count": 1, "coverage_gap_score": 9.0},
            {"carretera": "A-66R", "total_length_km": 120.0, "pk_span_km": 120.0, "mean_complexity": 2.0, "tent_share": 0.0, "existing_station_count": 2, "coverage_gap_score": 8.0},
            {"carretera": "N-340A", "total_length_km": 130.0, "pk_span_km": 130.0, "mean_complexity": 2.5, "tent_share": 0.0, "existing_station_count": 3, "coverage_gap_score": 7.0},
            {"carretera": "XX-1", "total_length_km": 140.0, "pk_span_km": 140.0, "mean_complexity": 3.0, "tent_share": 0.0, "existing_station_count": 4, "coverage_gap_score": 6.0},
        ]
    )
    enriched = gsp.enrich_route_summary_for_planning(route_summary)
    hierarchy = dict(zip(enriched["carretera"], enriched["route_hierarchy_score"]))
    assert hierarchy["N-2"] == 0.88
    assert hierarchy["AP-7N"] == 0.90
    assert hierarchy["A-66R"] == 0.76
    assert hierarchy["N-340A"] == 0.66
    assert hierarchy["XX-1"] == 0.60

    low_need_row = pd.Series(
        {
            "service_need_score": 0.1,
            "existing_station_count": 20.0,
            "total_length_km": 500.0,
            "pk_span_km": 500.0,
            "route_hierarchy_score": 0.65,
        }
    )
    assert gsp._dynamic_spacing_km(low_need_row, 120) > 120

    medium_need_row = pd.Series(
        {
            "service_need_score": 0.72,
            "existing_station_count": 10.0,
            "total_length_km": 300.0,
            "pk_span_km": 300.0,
            "route_hierarchy_score": 0.96,
        }
    )
    assert gsp._route_target_station_count(medium_need_row, 120, 50) >= 1
    service_need_row = pd.Series(
        {
            "service_need_score": 0.72,
            "existing_station_count": 10.0,
            "total_length_km": 300.0,
            "pk_span_km": 300.0,
            "route_hierarchy_score": 0.80,
        }
    )
    assert gsp._route_target_station_count(service_need_row, 120, 50) >= 1

    roads = gpd.GeoDataFrame(
        {"geometry": [None, MultiLineString([[(0, 0), (1, 1)], [(1, 1), (2, 2)]])]},
        crs="EPSG:4326",
    )
    lines = gsp._geometry_to_lines(roads)
    assert len(lines) == 2


def test_offline_reference_map_helpers_and_main(tmp_path, monkeypatch):
    roads = sample_roads_gdf().copy()
    roads["center_lat"] = [40.7, 41.5, 42.0]
    roads["center_lon"] = [-3.45, -2.5, 1.5]
    roads["priority_score"] = [12.0, 68.0, 91.0]
    roads["priority_level"] = ["Low", "Medium", "High"]

    base_features = ref_geometry_to_features(roads, "base")
    priority_features = ref_geometry_to_features(roads, "priority")
    assert base_features
    assert priority_features[0]["color"]
    bounds = ref_bounds_from_features(base_features)
    assert bounds["maxLon"] > bounds["minLon"]
    points = ref_centroid_points(roads)
    assert points[0]["weight"] > 0
    roads_without_centers = roads.copy()
    roads_without_centers["center_lat"] = [None, None, None]
    roads_without_centers["center_lon"] = [None, None, None]
    assert ref_centroid_points(roads_without_centers)
    backdrops = backdrop_features(base_features)
    assert backdrops
    summary = ref_build_summary(roads)
    assert summary["segments"] == 3
    assert "--bg" in base_styles()

    line_html = ref_render_line_map("Base", "Subtitle", base_features, bounds, backdrops, "base", summary)
    assert "Offline Reference Map" in line_html
    priority_html = ref_render_line_map("Priority", "Subtitle", priority_features, bounds, backdrops, "priority", summary)
    assert "High" in priority_html
    heat_html = ref_render_heatmap("Heat", "Subtitle", points, bounds, backdrops, summary)
    assert "Higher density" in heat_html

    import scripts.build_offline_reference_maps as borm

    root = tmp_path / "repo"
    maps_dir = root / "maps"
    maps_dir.mkdir(parents=True)
    monkeypatch.setattr(borm, "MAPS_DIR", maps_dir)
    monkeypatch.setattr(borm, "load_datasets", lambda: roads)
    borm.main()
    assert (maps_dir / "base_map.html").exists()
    assert (maps_dir / "priority_map.html").exists()
    assert (maps_dir / "density_heatmap.html").exists()


def test_offline_reference_map_load_datasets_and_empty_geometry(monkeypatch):
    import scripts.build_offline_reference_maps as borm

    roads = sample_roads_gdf().copy()
    roads = roads.set_crs(None, allow_override=True)
    scored = pd.DataFrame({"priority_score": [11.0, 22.0, 33.0]})
    calls = {"count": 0}

    def fake_read_parquet(path):
        calls["count"] += 1
        return roads if calls["count"] == 1 else scored

    monkeypatch.setattr(borm.gpd, "read_parquet", fake_read_parquet)
    merged = borm.load_datasets()
    assert merged.crs.to_string() == "EPSG:4326"
    assert (merged["priority_level"] == "Low").all()
    assert merged["priority_score"].tolist() == [11.0, 22.0, 33.0]

    roads_3857 = sample_roads_gdf().to_crs("EPSG:3857")
    scored_full = pd.DataFrame({"priority_score": [1.0, 2.0, 3.0], "priority_level": ["Low", "Medium", "High"]})
    calls = {"count": 0}

    def fake_read_parquet_3857(path):
        calls["count"] += 1
        return roads_3857 if calls["count"] == 1 else scored_full

    monkeypatch.setattr(borm.gpd, "read_parquet", fake_read_parquet_3857)
    merged_3857 = borm.load_datasets()
    assert merged_3857.crs.to_string() == "EPSG:4326"
    assert merged_3857["priority_level"].tolist() == ["Low", "Medium", "High"]

    roads_default_score = sample_roads_gdf().copy()
    calls = {"count": 0}

    def fake_read_parquet_missing_score(path):
        calls["count"] += 1
        return roads_default_score if calls["count"] == 1 else pd.DataFrame({"priority_level": ["Low", "Low", "Low"]})

    monkeypatch.setattr(borm.gpd, "read_parquet", fake_read_parquet_missing_score)
    merged_default_score = borm.load_datasets()
    assert (merged_default_score["priority_score"] == 0.0).all()

    empty_geom = sample_roads_gdf().copy()
    empty_geom.loc[0, "geometry"] = None
    features = borm.geometry_to_features(empty_geom, "base")
    assert len(features) >= 2

    empty_centroids = sample_roads_gdf().copy()
    empty_centroids["center_lat"] = [None, None, None]
    empty_centroids["center_lon"] = [None, None, None]
    empty_centroids.loc[0, "geometry"] = None
    points = borm.centroid_points(empty_centroids)
    assert len(points) == 2


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
    submission_dir = tmp_path / "data" / "submission"
    submission_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "total_proposed_stations": 2,
                "total_existing_stations_baseline": 7,
                "total_friction_points": 1,
                "total_ev_projected_2027": 123456,
            }
        ]
    ).to_csv(submission_dir / "File 1.csv", index=False)
    pd.DataFrame(
        [
            {
                "location_id": "IBE_001",
                "latitude": 40.5,
                "longitude": -3.5,
                "route_segment": "A-1",
                "n_chargers_proposed": 4,
                "grid_status": "Moderate",
            },
            {
                "location_id": "IBE_002",
                "latitude": 41.5,
                "longitude": 1.2,
                "route_segment": "AP-7",
                "n_chargers_proposed": 8,
                "grid_status": "Sufficient",
            },
        ]
    ).to_csv(submission_dir / "File 2.csv", index=False)
    pd.DataFrame(
        [
            {
                "bottleneck_id": "FRIC_001",
                "latitude": 40.5,
                "longitude": -3.5,
                "route_segment": "A-1",
                "distributor_network": "i-DE",
                "estimated_demand_kw": 600,
                "grid_status": "Moderate",
            }
        ]
    ).to_csv(submission_dir / "File 3.csv", index=False)
    report_path = generate_report(str(report_dir))
    assert Path(report_path).exists()
    assert "RTIG ROADS NETWORK ANALYSIS REPORT" in Path(report_path).read_text(encoding="utf-8")
    assert "A-1: 1 sites / 4 chargers" in Path(report_path).read_text(encoding="utf-8")

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
