# Notebook Execution Report
**Date:** March 26, 2026  
**Status:** ✅ **UPDATED FOR SUBMISSION**

---

## Summary

Executed the core analytical notebooks and the final submission notebook for the datathon package. The notebook flow now connects the road-network analysis to the real external inputs used in the final CSV deliverables, including the official charging baseline, the official EV forecast workflow, and published grid-capacity files from i-DE, Endesa, and Viesgo.

---

## Notebook 01: Exploratory Data Analysis (EDA)

**Status:** ✅ Fixed and rechecked

**Issues Fixed:**
1. **Geometry bounds assignment error** - Removed problematic `gdf['geom_bounds'] = gdf.geometry.bounds` line (bounds returns DataFrame, not Series)
2. **Data loading from raw Esri JSON** - Updated to load from processed parquet instead
3. **Missing maps directory** - Auto-create directory before saving visualizations

**Key Outputs:**
- Loaded 1,602 road segments 
- 100% geometry validity
- 23 total columns created
- Base map saved: `maps/base_map.html`
- Dataset summary statistics

---

## Notebook 02: Feature Engineering & Analysis

**Status:** ✅ Fixed and rechecked

**Issues Fixed:**
1. **is_tent flag calculation error** - TEN-T detection was checking for non-null instead of "SI"/"YES" values
   - **Root cause:** Column name case mismatch (TENT vs tent)  
   - **Solution:** Check both uppercase and lowercase column names, compare to "SI"/"YES"
2. **Pie chart data mismatch** - All roads showing as TEN-T due to incorrect logic

**Corrections Applied to `src/data/preprocess.py`:**
```python
# Before: gdf['is_tent'] = gdf[tent_indicators].notna().any(axis=1).astype(int)
# After: Properly check for TENT='SI' or TENT='YES'
```

**Key Results:**
- TEN-T Corridors: 535 segments (33.4%)
- Regular RTIG: 1,067 segments (66.6%)
- Total Network: 38,529 km
- TEN-T corridors longer on average: 30.8 km vs 20.7 km (regular)
- Risk scores generated (0-100 scale)
- Saved: `data/processed/roads_scored_gdf.parquet`

---

## Notebook 03: Predictive Modeling

**Status:** ✅ Executed successfully

**Models Generated:**

### Model 1: Length Prediction (RandomForest)
- **R² Score:** 0.979 (excellent)
- **RMSE:** 5.39 km
- **MAE:** 1.27 km
- **Feature Importance:** num_vertices (77.5%), curve_complexity (22.5%)

### Model 2: Priority Scoring
- **Weights:** Length 35% + Complexity 25% + TEN-T 40%
- **Distribution:**
  - High Priority: 185 segments (11.5%)
  - Medium Priority: 367 segments (22.9%)
  - Low Priority: 1,050 segments (65.5%)

**Outputs:**
- Saved models: `models/length_prediction_model.pkl`, `models/feature_scaler.pkl`
- Scored dataset: `data/processed/roads_scored_final.parquet`

---

## Notebook 04: Dashboards & Visualizations

**Status:** ✅ Fixed and rechecked

**Issues Fixed:**
1. **Subplot incompatibility** - Pie chart incompatible with xy subplots
   - **Solution:** Replaced pie with bar chart for consistent subplot types
2. **Random fallback logic removed** - Dashboard now fails loudly if prerequisite scoring is missing instead of creating synthetic priorities
3. **Heatmap coordinate order corrected** - Folium heat layer now uses `[lat, lon]` correctly

**Visualizations Created:**

| Map | Size | Features |
|-----|------|----------|
| `priority_map.html` | 34 MB | Road segments color-coded by priority (High=red, Medium=orange, Low=gray) |
| `dashboard.html` | 4.5 MB | 4-panel analytical dashboard (length, priority, complexity, TEN-T analysis) |
| `density_heatmap.html` | 77 MB | Heatmap showing road network density across Spain |
| `base_map.html` | 23 MB | Base map with sample road segments |

**KPI Summary:**
- 1,602 total segments
- 38,529 km total network
- 535 TEN-T corridors (33.4%)
- 185 high-priority segments (11.5%)

---

## Notebook 05: Final Submission Package

**Status:** ✅ Added & Executed

**Purpose:**
- Rebuild the local submission package
- Validate the exact CSV outputs
- Preview `File 1.csv`, `File 2.csv`, and `File 3.csv`
- Surface source provenance and assumptions for judge review

**Outputs:**
- Final notebook with visible outputs: `notebooks/05_final_submission_package.ipynb`
- Submission files previewed from `data/submission/`
- Judge-friendly map linked: `maps/proposed_charging_network.html`
- Offline scenario explorer linked: `maps/offline_scenario_explorer.html`
- Final package summary reflected in notebook outputs:
  - 269 proposed charging locations
  - 9,699 existing baseline stations
  - 145 friction points
  - 549,226 projected EV stock proxy in 2027

---

## Data Outputs

| File | Size | Purpose |
|------|------|---------|
| `roads_processed.csv` | 371 KB | Clean tabular format for Excel/analysts |
| `roads_processed.parquet` | 176 KB | Efficient columnar format |
| `roads_processed_gdf.parquet` | 27 MB | GeoDataFrame with geometry for GIS analysis |
| `roads_scored_final.parquet` | 27 MB | Final scored dataset with priority levels |
| `roads_scored.csv` | 154 KB | Scored data in CSV format |
| `roads_processed.geojson` | 90 MB | GeoJSON format for mapping/GIS |

---

## Key Fixes & Improvements

### 1. Esri JSON Geometry Parsing
- Added custom converter for Esri "paths" format → standard GeoJSON coordinates
- Handles both single path (LineString) and multi-path (MultiLineString) geometries

### 2. TEN-T Flag Calculation (Critical Fix)
- **Problem:** All roads marked as TEN-T despite field having "SI"/"NO" values
- **Root Cause:** Checking for non-null vs checking for specific values
- **Fix:** Direct string comparison to "SI"/"YES" after case normalization

### 3. Preprocessing Pipeline Robustness
- Column name case-insensitivity 
- Proper handling of missing values
- Geometry validation (100% valid geometries)

### 4. Notebook Visualization Compatibility
- Fixed subplot type mismatches
- Proper directory creation for outputs
- Fallback data loading strategies

---

## Validation Checklist

✅ Final submission notebook executes and validates the package  
✅ Data pipeline: 1,602 segments processed end-to-end  
✅ Feature engineering: 23 columns created  
✅ Models trained: Length prediction (R²=0.979)  
✅ Priority scoring: 185 high, 367 medium, 1,050 low  
✅ Visualizations: 4 interactive maps + dashboard  
✅ Data exports: CSV, Parquet, GeoJSON formats  
✅ Model artifacts: .pkl files saved for inference  

---

## Performance Metrics

- **Data Processing:** ~10 minutes (download + preprocess + feature engineer)
- **Model Training:** ~30 seconds (RandomForest on 1,602 samples)
- **Visualization Generation:** ~30 seconds (4 interactive maps)
- **Total Pipeline Runtime:** ~12-15 minutes

---

## Datasets Ready for Use

1. **Analysis:** `roads_processed.csv` + `roads_scored.csv`
2. **GIS Work:** `roads_processed_gdf.parquet` + `roads_processed.geojson`
3. **Presentations:** HTML maps + `dashboard.html`
4. **Production Inference:** Saved models + `roads_scored_final.parquet`

---

## Recommendations

1. **Share the final package first:** Start with `05_final_submission_package.ipynb`, `File 1/2/3`, and the offline scenario explorer.
2. **Use the lighter maps during demos:** Prefer `proposed_charging_network.html` and `offline_scenario_explorer.html` over the heavier exploratory HTML files.
3. **Explain the friction points clearly:** The strongest business insight is where corridor need collides with distributor capacity limits.
4. **Keep assumptions honest:** The repo is much more real now, but remaining thresholds and spacing rules should still be explained clearly in the presentation.

---

**Status:** Project is **submission-ready and grounded in real external EV, charging, and grid inputs** ✅

The notebook layer is now safer for judge review and much closer to the datathon brief than the earlier road-only version. The remaining upside is mainly presentation quality and, if time allows, further refinement of siting thresholds rather than major missing data sources.
