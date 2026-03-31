import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString

from src.data.eda import inspect_geojson
from src.data.preprocess import (
    convert_to_polars,
    engineer_features,
    esri_to_geojson_geometry,
    load_and_clean_geojson,
    run_preprocessing,
    save_processed_data,
    validate_and_standardize,
)


def sample_geodataframe():
    return gpd.GeoDataFrame(
        {
            "id": [1, 2],
            "tent": ["SI", "NO"],
            "value": [1.5, None],
            "geometry": [
                LineString([(-3.7, 40.4), (-3.6, 40.5)]),
                MultiLineString(
                    [
                        [(-3.5, 40.5), (-3.4, 40.6)],
                        [(-3.4, 40.6), (-3.3, 40.7)],
                    ]
                ),
            ],
        },
        crs="EPSG:4326",
    )


def test_esri_to_geojson_geometry_handles_variants():
    assert esri_to_geojson_geometry(None) is None
    assert esri_to_geojson_geometry({"paths": []}) is None
    assert isinstance(esri_to_geojson_geometry({"paths": [[[0, 0], [1, 1]]]}), LineString)
    assert isinstance(
        esri_to_geojson_geometry({"paths": [[[0, 0], [1, 1]], [[2, 2], [3, 3]]]}),
        MultiLineString,
    )
    assert isinstance(
        esri_to_geojson_geometry({"coordinates": [[0, 0], [1, 1]], "type": "LineString"}),
        LineString,
    )
    assert esri_to_geojson_geometry({"foo": "bar"}) is None


def test_load_and_clean_geojson_and_inspect(tmp_path, capsys):
    payload = {
        "features": [
            {
                "attributes": {"id": 1, "TENT": "YES"},
                "geometry": {"paths": [[[-412305.1, 4926709.9], [-412100.0, 4926800.0]]]},
            },
            {
                "properties": {"id": 2, "tent": "NO"},
                "geometry": {"type": "LineString", "coordinates": [[-3.7, 40.4], [-3.6, 40.5]]},
            },
            {"attributes": {"id": 3}, "geometry": None},
        ]
    }
    path = tmp_path / "sample.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    gdf, lengths = load_and_clean_geojson(str(path), source_crs="EPSG:3857", target_crs="EPSG:4326")

    assert len(gdf) == 2
    assert len(lengths) == 2
    assert str(gdf.crs).endswith("4326")

    empty_path = tmp_path / "empty.json"
    empty_path.write_text(json.dumps({"features": [{"attributes": {"id": 1}, "geometry": None}]}), encoding="utf-8")
    empty_gdf, empty_lengths = load_and_clean_geojson(str(empty_path))
    assert empty_gdf.empty
    assert empty_lengths.empty

    geojson_path = tmp_path / "inspect.geojson"
    sample_geodataframe().to_file(geojson_path, driver="GeoJSON")
    inspected = inspect_geojson(str(geojson_path))
    assert len(inspected) == 2
    assert "Rows:" in capsys.readouterr().out


def test_engineer_validate_convert_and_save(tmp_path):
    gdf = sample_geodataframe()
    engineered = engineer_features(gdf)
    assert {"length_km", "num_vertices", "curve_complexity", "is_tent", "center_lat", "center_lon"} <= set(
        engineered.columns
    )
    assert engineered["is_tent"].tolist() == [1, 0]

    fallback_gdf = gpd.GeoDataFrame(
        {
            "TENT_red_basica": ["x", None],
            "geometry": [LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 2)])],
        },
        crs="EPSG:4326",
    )
    fallback = engineer_features(fallback_gdf)
    assert fallback["is_tent"].tolist() == [1, 0]

    no_tent_gdf = gpd.GeoDataFrame(
        {"geometry": [LineString([(0, 0), (1, 1)])]},
        crs="EPSG:4326",
    )
    no_tent = engineer_features(no_tent_gdf)
    assert no_tent["is_tent"].tolist() == [0]

    standardized = validate_and_standardize(engineered.rename(columns={"value": "VALUE"}))
    assert "value" in standardized.columns
    assert standardized["value"].iloc[1] == 0

    polars_df = convert_to_polars(standardized)
    assert polars_df.shape[0] == 2
    assert "geometry" not in polars_df.columns

    save_processed_data(standardized, tmp_path, basename="roads_processed")
    assert (tmp_path / "roads_processed_gdf.parquet").exists()
    assert (tmp_path / "roads_processed.parquet").exists()
    assert (tmp_path / "roads_processed.csv").exists()
    assert (tmp_path / "roads_processed.geojson").exists()


def test_run_preprocessing_end_to_end(tmp_path):
    payload = {
        "features": [
            {
                "attributes": {"id": 1, "TENT": "SI"},
                "geometry": {"paths": [[[-412305.1, 4926709.9], [-412100.0, 4926800.0]]]},
            }
        ]
    }
    input_path = tmp_path / "roads.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")

    gdf, pdf = run_preprocessing(str(input_path), str(tmp_path / "out"))

    assert len(gdf) == 1
    assert pdf.shape[0] == 1
    assert (tmp_path / "out" / "roads_processed.csv").exists()
