import json
import time
from pathlib import Path

import requests
import yaml


def load_config(config_path="config/settings.yaml"):
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def count_cached_features(output_path):
    output_file = Path(output_path)
    if not output_file.exists():
        return 0

    with output_file.open("r", encoding="utf-8") as fin:
        payload = json.load(fin)

    return len(payload.get("features", []))


def fetch_arcgis_layer(
    config,
    output_path,
    show_progress=True,
    use_cache=False,
    fallback_to_cache=True,
):
    base_url = config["base_url"]
    output_file = Path(output_path)

    if use_cache and output_file.exists():
        cached_count = count_cached_features(output_file)
        if show_progress:
            print(f"Using cached file: {output_file}")
            print(f"Cached features available: {cached_count}")
        return str(output_file), cached_count

    params = {
        "where": config.get("where", "1=1"),
        "outFields": config.get("out_fields", "*"),
        "f": config.get("f", "json"),
        "resultRecordCount": config.get("result_record_count", 500),
    }
    result_offset = 0
    all_features = []
    total = None

    while True:
        params["resultOffset"] = result_offset

        try:
            response = requests.get(base_url, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException:
            if fallback_to_cache and output_file.exists():
                cached_count = count_cached_features(output_file)
                if show_progress:
                    print(f"Remote download unavailable. Falling back to cached data: {output_file}")
                    print(f"Cached features available: {cached_count}")
                return str(output_file), cached_count
            raise

        if total is None:
            total = payload.get("exceededTransferLimit", False) or payload.get("total", None)

        features = payload.get("features", [])
        batch_size = len(features)

        if show_progress:
            print(f"Downloading records {result_offset} to {result_offset + batch_size} (got {batch_size})")

        all_features.extend(features)

        if batch_size < params["resultRecordCount"]:
            break

        result_offset += batch_size
        time.sleep(0.1)

    if show_progress:
        print(f"Total downloaded features: {len(all_features)}")

    # Create output directory
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as fout:
        json.dump({"type": "FeatureCollection", "features": all_features}, fout, ensure_ascii=False, indent=2)

    return str(output_file), len(all_features)


def run_download(
    dataset_key="roads_rtig",
    output_file="data/raw/carreteras_RTIG.geojson",
    use_cache=False,
    fallback_to_cache=True,
):
    config = load_config()
    dataset_conf = config["datasets"].get(dataset_key)
    if dataset_conf is None:
        raise ValueError(f"Dataset key '{dataset_key}' not found in config")

    output_file = Path(output_file)
    downloaded_path, count = fetch_arcgis_layer(
        dataset_conf,
        str(output_file),
        use_cache=use_cache,
        fallback_to_cache=fallback_to_cache,
    )
    return downloaded_path, count


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Download RTIG roads dataset via ArcGIS REST API")
    parser.add_argument("--dataset", default="roads_rtig", help="Config key of the dataset")
    parser.add_argument("--output", default="data/raw/carreteras_RTIG.geojson", help="Output file path")
    parser.add_argument("--force-download", action="store_true", help="Ignore cached files and redownload")
    args = parser.parse_args()

    path, n = run_download(
        args.dataset,
        args.output,
        use_cache=not args.force_download,
        fallback_to_cache=True,
    )
    print("Saved:", path)
    print("Downloaded records:", n)
