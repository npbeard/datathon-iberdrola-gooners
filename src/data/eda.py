import geopandas as gpd


def inspect_geojson(path):
    gdf = gpd.read_file(path)
    print("Rows:", len(gdf))
    print("Columns:", list(gdf.columns))
    print(gdf.describe(include='all'))
    return gdf


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Inspect GeoJSON dataset")
    parser.add_argument("--path", default="data/raw/carreteras_RTIG.geojson")
    args = parser.parse_args()

    inspect_geojson(args.path)
