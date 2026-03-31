"""
Complete data pipeline runner: download → preprocess → analyze → score.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.append(str(ROOT))

from src.data.download import run_download
from src.data.preprocess import run_preprocessing


def main():
    """Run full pipeline."""
    root = ROOT
    
    print("=" * 70)
    print("RTIG ROADS DATATHON PIPELINE - FULL RUN")
    print("=" * 70)
    
    # Step 1: Download
    print("\n[1/3] Downloading raw data from ArcGIS REST API...")
    raw_path, count = run_download(
        dataset_key='roads_rtig',
        output_file=str(root / 'data' / 'raw' / 'carreteras_RTIG.geojson'),
        use_cache=True,
        fallback_to_cache=True,
    )
    print(f"✓ Downloaded {count} road segments to {raw_path}")
    
    # Step 2: Preprocess
    print("\n[2/3] Preprocessing and feature engineering...")
    gdf, pdf = run_preprocessing(
        input_path=str(raw_path),
        output_dir=str(root / 'data' / 'processed')
    )
    print(f"✓ Processed {len(gdf)} features")
    print(f"✓ Generated {len(gdf.columns)} columns with engineered features")
    
    # Step 3: Summary
    print("\n[3/3] Pipeline Summary:")
    print(f"  - Total road segments: {len(gdf)}")
    print(f"  - Total network length: {gdf['length_km'].sum():.0f} km")
    print(f"  - TEN-T corridors: {(gdf['is_tent'] == 1).sum()}")
    print(f"  - Data quality: {(gdf.geometry.is_valid.sum() / len(gdf) * 100):.1f}% valid geometries")
    
    print("\n" + "=" * 70)
    print("✓ PIPELINE COMPLETE")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. Run Jupyter notebooks for analysis:")
    print("     - notebooks/01_eda.ipynb")
    print("     - notebooks/02_feature_engineering.ipynb")
    print("     - notebooks/03_modeling.ipynb")
    print("     - notebooks/04_dashboards.ipynb")
    print("  2. Review outputs in data/processed/ and maps/")
    print("  3. Check score distribution in visualizations")
    
    return gdf, pdf


if __name__ == "__main__":  # pragma: no cover
    try:
        gdf, pdf = main()
    except Exception as e:
        print(f"\n❌ Error during pipeline execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
