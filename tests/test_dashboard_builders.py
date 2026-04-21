from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString

import scripts.build_competition_dashboard as bcd
import src.visualization.offline_dashboard as od


def _sample_stations(include_business: bool = True) -> pd.DataFrame:
    rows = [
        {
            "location_id": "IBE_001",
            "latitude": 40.5,
            "longitude": -3.7,
            "route_segment": "A-1",
            "n_chargers_proposed": 4,
            "grid_status": "Moderate",
            "estimated_demand_kw": 600,
            "distributor_network": "i-DE",
        },
        {
            "location_id": "IBE_002",
            "latitude": 41.4,
            "longitude": 2.1,
            "route_segment": "AP-7",
            "n_chargers_proposed": 8,
            "grid_status": "Sufficient",
            "estimated_demand_kw": 1200,
            "distributor_network": "Endesa",
        },
    ]
    if include_business:
        rows[0]["business_score"] = 2.5
        rows[1]["business_score"] = 5.0
    return pd.DataFrame(rows)


def test_offline_dashboard_helper_branches():
    empty_roads = gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs="EPSG:4326")
    assert od._geometry_to_lines(empty_roads) == []
    assert od._route_lines(empty_roads) == {}
    assert od._map_bounds(pd.DataFrame(columns=["latitude", "longitude"]), []) == od.DEFAULT_BOUNDS
    assert od._friction_summary(pd.DataFrame()) == []

    mixed_roads = gpd.GeoDataFrame(
        {
            "carretera": ["", "A-1", "AP-7", "A-2"],
            "geometry": [
                LineString(),
                None,
                MultiLineString([[(0, 0), (1, 1)], [(1, 1), (2, 2)]]),
                LineString([(-3.9, 40.4), (-3.0, 41.0)]),
            ],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    line_payload = od._geometry_to_lines(mixed_roads)
    assert len(line_payload) == 3

    route_payload = od._route_lines(mixed_roads)
    assert "AP-7" in route_payload
    assert len(route_payload["AP-7"]) == 2
    assert "A-2" in route_payload
    assert od._route_lines(mixed_roads[["geometry"]]) == {}

    stations_without_business = _sample_stations(include_business=False)
    empty_file_3 = pd.DataFrame(columns=["route_segment", "bottleneck_id", "estimated_demand_kw"])
    route_rows = od._route_summary_payload(pd.DataFrame(), stations_without_business, empty_file_3)
    assert route_rows[0]["avg_business_score"] is None

    enriched_same = od._stations_with_route_context(stations_without_business, None)
    assert enriched_same.equals(stations_without_business)

    route_summary_missing_key = pd.DataFrame([{"route_name": "A-1", "service_need_score": 0.7}])
    enriched_missing_key = od._stations_with_route_context(stations_without_business, route_summary_missing_key)
    assert enriched_missing_key.equals(stations_without_business)


def test_build_offline_dashboard_map_and_full_views(tmp_path):
    file_1 = pd.DataFrame(
        [
            {
                "total_proposed_stations": 2,
                "total_existing_stations_baseline": 9,
                "total_friction_points": 1,
                "total_ev_projected_2027": 123456,
            }
        ]
    )
    stations_df = _sample_stations(include_business=True)
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
    roads = gpd.GeoDataFrame(
        {
            "carretera": ["A-1", "AP-7"],
            "geometry": [
                LineString([(-3.9, 40.4), (-3.0, 41.0)]),
                MultiLineString([[(0.5, 41.5), (1.5, 42.0)], [(1.5, 42.0), (2.5, 42.5)]]),
            ],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )
    route_summary = pd.DataFrame(
        [
            {
                "carretera": "A-1",
                "service_need_score": 0.7,
                "strategic_corridor_score": 0.9,
                "existing_station_count": 1,
                "business_support_score": 4.2,
                "traffic_imd_total": 15000,
                "market_access_population": 100000,
                "tourism_overnight_stays": 2000000,
                "coverage_gap_score": 8.0,
                "total_length_km": 120.0,
                "pk_span_km": 120.0,
            },
            {
                "carretera": "AP-7",
                "service_need_score": 0.9,
                "strategic_corridor_score": 1.0,
                "existing_station_count": 0,
                "business_support_score": 6.5,
                "traffic_imd_total": 23000,
                "market_access_population": 200000,
                "tourism_overnight_stays": 5000000,
                "coverage_gap_score": 9.0,
                "total_length_km": 200.0,
                "pk_span_km": 200.0,
            },
        ]
    )

    full_output = tmp_path / "dashboard.html"
    od.build_offline_dashboard(
        file_1=file_1,
        stations_df=stations_df,
        file_3=file_3,
        roads_gdf=roads,
        route_summary=route_summary,
        output_path=full_output,
        embed_explorer=True,
        explorer_path="offline_scenario_explorer.html",
    )
    full_html = full_output.read_text(encoding="utf-8")
    assert "Scenario Explorer" in full_html
    assert "Business-Fit Logic" in full_html
    assert "How To Read The Map" in full_html

    map_only_output = tmp_path / "map_only.html"
    od.build_offline_dashboard(
        file_1=file_1,
        stations_df=stations_df,
        file_3=file_3,
        roads_gdf=roads,
        route_summary=route_summary,
        output_path=map_only_output,
        map_only=True,
    )
    map_only_html = map_only_output.read_text(encoding="utf-8")
    assert "Scenario Explorer" not in map_only_html
    assert "How To Read The Map" not in map_only_html
    assert "Use the filters to inspect the proposed network by status, distributor, or corridor." in map_only_html


def test_build_competition_dashboard_main(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    (root / "data" / "external").mkdir(parents=True)
    (root / "data" / "submission").mkdir(parents=True)
    (root / "maps").mkdir(parents=True)
    traffic_path = root / "data" / "external" / "mitma_traffic_by_route.csv"
    pd.DataFrame([{"carretera": "A-1", "traffic_imd_total": 15000}]).to_csv(traffic_path, index=False)
    pd.DataFrame(
        [
            {
                "total_proposed_stations": 1,
                "total_existing_stations_baseline": 9,
                "total_friction_points": 1,
                "total_ev_projected_2027": 123456,
            }
        ]
    ).to_csv(root / "data" / "submission" / "File 1.csv", index=False)
    pd.DataFrame(
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
    ).to_csv(root / "data" / "submission" / "File 2.csv", index=False)
    pd.DataFrame(
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
    ).to_csv(root / "data" / "submission" / "File 3.csv", index=False)

    roads = gpd.GeoDataFrame(
        {"carretera": ["A-1"], "geometry": [LineString([(-3.9, 40.4), (-3.0, 41.0)])]},
        geometry="geometry",
        crs="EPSG:4326",
    )
    route_summary = pd.DataFrame([{"carretera": "A-1", "service_need_score": 0.7}])
    captured = {}

    monkeypatch.setattr(bcd, "ROOT", root)
    monkeypatch.setattr(bcd, "DEFAULT_INPUT", root / "data" / "processed" / "roads_processed_gdf.parquet")
    monkeypatch.setattr(bcd, "load_roads_dataset", lambda _: roads)
    monkeypatch.setattr(bcd, "filter_interurban_routes", lambda gdf: gdf)
    monkeypatch.setattr(bcd, "summarize_routes", lambda roads_gdf: route_summary.copy())
    monkeypatch.setattr(
        bcd,
        "load_external_route_baseline",
        lambda external_dir: (pd.DataFrame([{"carretera": "A-1", "existing_station_count": 2}]), 9, "loaded"),
    )
    monkeypatch.setattr(
        bcd,
        "load_business_context",
        lambda external_dir: (pd.DataFrame([{"route_segment": "A-1", "business_score": 3.0}]), "loaded"),
    )
    monkeypatch.setattr(
        bcd,
        "enrich_route_summary_with_traffic",
        lambda summary, traffic: summary.assign(traffic_imd_total=traffic["traffic_imd_total"].iloc[0]),
    )
    monkeypatch.setattr(
        bcd,
        "enrich_route_summary_with_baseline",
        lambda summary, baseline: summary.assign(existing_station_count=baseline["existing_station_count"].iloc[0]),
    )
    monkeypatch.setattr(
        bcd,
        "enrich_route_summary_for_planning",
        lambda summary: summary.assign(planning_ready=True),
    )

    def fake_enrich_route_summary_with_business(summary, business_context):
        return summary.assign(business_support_score=business_context["business_score"].iloc[0])

    monkeypatch.setattr(gpd, "read_file", gpd.read_file)
    monkeypatch.setattr(
        __import__("scripts.generate_submission_package", fromlist=["x"]),
        "enrich_route_summary_with_business",
        fake_enrich_route_summary_with_business,
    )

    def fake_build_offline_dashboard(**kwargs):
        captured.update(kwargs)
        kwargs["output_path"].write_text("ok", encoding="utf-8")

    monkeypatch.setattr(bcd, "build_offline_dashboard", fake_build_offline_dashboard)

    bcd.main()

    assert captured["embed_explorer"] is True
    assert captured["explorer_path"] == "offline_scenario_explorer.html"
    assert captured["output_path"] == root / "maps" / "dashboard.html"
    assert captured["stations_df"].loc[0, "distributor_network"] == "i-DE"
    assert captured["route_summary"].loc[0, "business_support_score"] == 3.0
    assert captured["route_summary"].loc[0, "traffic_imd_total"] == 15000
    assert (root / "maps" / "dashboard.html").exists()
