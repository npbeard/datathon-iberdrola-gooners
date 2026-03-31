"""
Data preprocessing and feature engineering for RTIG roads dataset.
Converts raw GeoJSON to clean, feature-rich datasets using polars and geopandas.
"""

import json
from pathlib import Path
from typing import Optional, Tuple

import geopandas as gpd
import pandas as pd
import polars as pl
from shapely.geometry import shape, LineString, MultiLineString


def esri_to_geojson_geometry(esri_geom):
    """Convert Esri JSON geometry format to standard GeoJSON geometry.
    
    Esri uses 'paths' for LineString/MultiLineString geometries,
    while standard GeoJSON uses 'coordinates'.
    """
    if not esri_geom:
        return None
    
    if 'paths' in esri_geom:
        paths = esri_geom['paths']
        if not paths:
            return None
        
        # Single path -> LineString, multiple paths -> MultiLineString
        if len(paths) == 1:
            return LineString(paths[0])
        else:
            return MultiLineString(paths)
    
    elif 'coordinates' in esri_geom:
        # Already in GeoJSON format
        return shape(esri_geom)
    
    return None


def load_and_clean_geojson(
    geojson_path: str,
    source_crs: str = "EPSG:3857",
    target_crs: str = "EPSG:4326",
) -> Tuple[gpd.GeoDataFrame, pd.Series]:
    """Load GeoJSON (Esri or standard format), clean it, and reproject to WGS84."""
    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    features_data = []
    for feature in data.get('features', []):
        # Convert Esri geometry format if needed
        geometry = esri_to_geojson_geometry(feature.get('geometry'))
        
        if geometry is None:
            continue
        
        # Get attributes (for Esri format) or properties (for standard GeoJSON)
        props = feature.get('attributes') or feature.get('properties', {})
        props['geometry'] = geometry
        features_data.append(props)
    
    if not features_data:
        empty = gpd.GeoDataFrame(columns=['geometry'], crs=target_crs)
        return empty, pd.Series(dtype="float64")

    # Create GeoDataFrame from features
    gdf = gpd.GeoDataFrame(features_data, crs=source_crs)

    # Drop rows with invalid geometry
    gdf = gdf[gdf.geometry.is_valid & ~gdf.geometry.is_empty].copy()

    # Ensure consistent geometry type (mainly LineString for roads)
    gdf = gdf[gdf.geometry.geom_type.isin(['LineString', 'MultiLineString'])].copy()

    length_km = gdf.geometry.length / 1000
    gdf = gdf.to_crs(target_crs)

    return gdf, length_km.reset_index(drop=True)


def engineer_features(
    gdf: gpd.GeoDataFrame,
    length_km: Optional[pd.Series] = None,
) -> gpd.GeoDataFrame:
    """Engineer new features from raw road data."""
    gdf = gdf.copy()

    # Geometry-based features
    if length_km is None:
        metric_gdf = gdf.to_crs("EPSG:3857")
        gdf['length_km'] = metric_gdf.geometry.length / 1000
    else:
        gdf['length_km'] = length_km.values

    # Bounds as separate columns
    bounds = gdf.geometry.bounds
    gdf['min_lon'] = bounds['minx']
    gdf['max_lon'] = bounds['maxx']
    gdf['min_lat'] = bounds['miny']
    gdf['max_lat'] = bounds['maxy']
    
    # Center coordinates
    gdf['center_lon'] = (gdf['min_lon'] + gdf['max_lon']) / 2
    gdf['center_lat'] = (gdf['min_lat'] + gdf['max_lat']) / 2
    
    # Road complexity heuristic (number of vertices)
    gdf['num_vertices'] = gdf.geometry.apply(
        lambda g: len(g.coords) if g.geom_type == 'LineString' else sum(len(g_part.coords) for g_part in g.geoms)
    )
    
    # Curve complexity = vertices per km
    gdf['curve_complexity'] = gdf['num_vertices'] / (gdf['length_km'] + 0.1)
    
    # Identify features that may indicate TEN-T (if fields exist)
    # Check if TENT column has "SI" or "YES" (Spanish yes/English yes) values
    # Try both uppercase and lowercase column names 
    tent_col = None
    if 'tent' in gdf.columns:
        tent_col = 'tent'
    elif 'TENT' in gdf.columns:
        tent_col = 'TENT'
    
    if tent_col:
        gdf['is_tent'] = gdf[tent_col].fillna('').astype(str).str.upper().isin(['SI', 'YES']).astype(int)
    else:
        # Fallback: check other TENT-related columns
        tent_indicators = [col for col in gdf.columns if 'TENT' in str(col).upper() and col != 'is_tent']
        if tent_indicators:
            gdf['is_tent'] = gdf[tent_indicators].notna().any(axis=1).astype(int)
        else:
            gdf['is_tent'] = 0
    
    return gdf


def validate_and_standardize(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Standardize column names and data types."""
    gdf = gdf.copy()
    
    # Standardize column names to lowercase
    gdf.columns = [col.lower() for col in gdf.columns]
    
    # Fill NaN in numeric columns with 0
    numeric_cols = gdf.select_dtypes(include=['float64', 'int64']).columns
    gdf[numeric_cols] = gdf[numeric_cols].fillna(0)
    
    return gdf


def convert_to_polars(gdf: gpd.GeoDataFrame, exclude_geometry: bool = True) -> pl.DataFrame:
    """Convert GeoDataFrame to Polars DataFrame for efficient processing."""
    # Convert to regular pandas first, drop geometry if needed
    df = gdf.drop(columns=['geometry']) if exclude_geometry else gdf
    
    # Polars can read pandas DataFrames
    pdf = pl.from_pandas(df)
    
    return pdf


def save_processed_data(
    gdf: gpd.GeoDataFrame,
    output_dir: Path,
    basename: str = 'roads_processed'
) -> None:
    """Save processed data in multiple formats."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # GeoParquet (keep geometry)
    geof_path = output_dir / f'{basename}_gdf.parquet'
    gdf.to_parquet(geof_path)
    print(f'Saved GeoDataFrame: {geof_path}')
    
    # Parquet without geometry (for polars)
    df_no_geom = gdf.drop(columns=['geometry'])
    data_path = output_dir / f'{basename}.parquet'
    df_no_geom.to_parquet(data_path)
    print(f'Saved cleaned data: {data_path}')
    
    # CSV for reference
    csv_path = output_dir / f'{basename}.csv'
    df_no_geom.to_csv(csv_path, index=False)
    print(f'Saved CSV: {csv_path}')
    
    # GeoJSON (subset) for mapping - keep only essential columns for display
    coordinate_cols = ['min_lon', 'max_lon', 'min_lat', 'max_lat', 'center_lon', 'center_lat']
    gdf_geojson = gdf.drop(columns=[col for col in coordinate_cols if col in gdf.columns])
    geojson_output = output_dir / f'{basename}.geojson'
    gdf_geojson.to_file(geojson_output, driver='GeoJSON')
    print(f'Saved GeoJSON: {geojson_output}')


def run_preprocessing(
    input_path: str = 'data/raw/carreteras_RTIG.geojson',
    output_dir: str = 'data/processed'
) -> Tuple[gpd.GeoDataFrame, pl.DataFrame]:
    """Run complete preprocessing pipeline."""
    
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    
    print(f'Loading data from {input_path}...')
    gdf, length_km = load_and_clean_geojson(str(input_path))
    print(f'Loaded {len(gdf)} valid features')

    print('\nEngineering features...')
    gdf = engineer_features(gdf, length_km=length_km)
    
    print('\nStandardizing data...')
    gdf = validate_and_standardize(gdf)
    
    print('\nSaving processed data...')
    save_processed_data(gdf, output_dir, basename='roads_processed')
    
    print('\nConverting to Polars...')
    pdf = convert_to_polars(gdf, exclude_geometry=True)
    print(f'Polars DataFrame shape: {pdf.shape}')
    
    # Save polars version
    polars_path = output_dir / 'roads_processed.parquet'
    # Already saved above, so just print confirmation
    print(f'Polars version available at {polars_path}')
    
    return gdf, pdf


if __name__ == '__main__':  # pragma: no cover
    import argparse
    
    parser = argparse.ArgumentParser(description='Preprocess RTIG roads data')
    parser.add_argument(
        '--input',
        default='data/raw/carreteras_RTIG.geojson',
        help='Input GeoJSON file'
    )
    parser.add_argument(
        '--output',
        default='data/processed',
        help='Output directory'
    )
    
    args = parser.parse_args()
    
    gdf, pdf = run_preprocessing(args.input, args.output)
    print('\n✓ Preprocessing complete!')
