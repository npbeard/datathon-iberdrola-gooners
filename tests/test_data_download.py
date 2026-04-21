import json
from pathlib import Path

import pytest
import requests

import src.data.download as download_module
from src.data.download import count_cached_features, fetch_arcgis_layer, load_config, run_download


def test_fetch_arcgis_layer(tmp_path, requests_mock):
    sample_data = {
        "features": [{"type": "Feature", "geometry": None, "properties": {"id": 1}}],
    }
    url = "https://example.com/query"
    requests_mock.get(url, json=sample_data)

    config = {
        "base_url": url,
        "where": "GEOM is not null",
        "out_fields": "*",
        "f": "json",
        "result_record_count": 500,
    }
    output_file = tmp_path / "out.geojson"
    path, count = fetch_arcgis_layer(config, str(output_file), show_progress=False)

    assert path == str(output_file)
    assert count == 1

    loaded = json.loads(output_file.read_text(encoding="utf-8"))
    assert loaded["type"] == "FeatureCollection"
    assert len(loaded["features"]) == 1


def test_fetch_arcgis_layer_falls_back_to_cache(tmp_path, requests_mock):
    output_file = tmp_path / "cached.geojson"
    output_file.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [{"attributes": {"id": 99}, "geometry": {"paths": []}}],
            }
        ),
        encoding="utf-8",
    )

    url = "https://example.com/query"
    requests_mock.get(url, exc=requests.exceptions.ConnectionError)

    config = {
        "base_url": url,
        "where": "GEOM is not null",
        "out_fields": "*",
        "f": "json",
        "result_record_count": 500,
    }

    path, count = fetch_arcgis_layer(config, str(output_file), show_progress=True, fallback_to_cache=True)

    assert path == str(output_file)
    assert count == 1
    assert count_cached_features(output_file) == 1


def test_fetch_arcgis_layer_uses_cache_and_run_download(tmp_path, requests_mock, monkeypatch):
    output_file = tmp_path / "cached.geojson"
    output_file.write_text(
        json.dumps({"type": "FeatureCollection", "features": [{"attributes": {"id": 1}, "geometry": None}]}),
        encoding="utf-8",
    )
    path, count = fetch_arcgis_layer({"base_url": "https://unused.example.com"}, str(output_file), show_progress=True, use_cache=True)
    assert path == str(output_file)
    assert count == 1
    path_silent, count_silent = fetch_arcgis_layer(
        {"base_url": "https://unused.example.com"},
        str(output_file),
        show_progress=False,
        use_cache=True,
    )
    assert path_silent == str(output_file)
    assert count_silent == 1

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    temp_config_path = config_dir / "settings.yaml"
    temp_config_path.write_text(
        """
datasets:
  roads_rtig:
    base_url: "https://example.com/query"
    where: "1=1"
    out_fields: "*"
    f: "json"
    result_record_count: 500
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(download_module, "load_config", lambda config_path="config/settings.yaml": load_config(str(temp_config_path)))
    requests_mock.get("https://example.com/query", json={"features": []})
    downloaded_path, downloaded_count = run_download("roads_rtig", tmp_path / "out.geojson", use_cache=False, fallback_to_cache=True)
    assert Path(downloaded_path).exists()
    assert downloaded_count == 0

    fallback_cache = tmp_path / "fallback.json"
    fallback_cache.write_text(json.dumps({"type": "FeatureCollection", "features": []}), encoding="utf-8")
    requests_mock.get("https://example.com/offline", exc=requests.exceptions.ConnectionError)
    path, count = fetch_arcgis_layer({"base_url": "https://example.com/offline"}, str(fallback_cache), show_progress=False, fallback_to_cache=True)
    assert path == str(fallback_cache)
    assert count == 0


def test_run_download_invalid_dataset_key(monkeypatch):
    monkeypatch.setattr(download_module, "load_config", lambda config_path="config/settings.yaml": {"datasets": {}})
    with pytest.raises(ValueError):
        run_download("missing")


def test_fetch_arcgis_layer_multi_batch_and_error_paths(tmp_path, requests_mock, capsys):
    url = "https://example.com/paged"
    requests_mock.get(
        url,
        [
            {"json": {"features": [{"id": 1}, {"id": 2}], "total": 3}},
            {"json": {"features": [{"id": 3}]}},
        ],
    )
    path, count = fetch_arcgis_layer(
        {"base_url": url, "result_record_count": 2},
        str(tmp_path / "paged.json"),
        show_progress=True,
    )
    assert count == 3
    assert "Downloading records 0 to 2" in capsys.readouterr().out

    assert count_cached_features(tmp_path / "does_not_exist.json") == 0

    requests_mock.get("https://example.com/fail", exc=requests.exceptions.ConnectionError)
    with pytest.raises(requests.exceptions.ConnectionError):
        fetch_arcgis_layer({"base_url": "https://example.com/fail"}, str(tmp_path / "no-cache.json"), show_progress=False, fallback_to_cache=False)
