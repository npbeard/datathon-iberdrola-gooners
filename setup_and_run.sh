#!/usr/bin/env bash
# 
# RTIG Roads Datathon - Automated Setup & Execution
# This script sets up the environment and runs the complete pipeline
#

set -e  # Exit on any error

echo "════════════════════════════════════════════════════════════════"
echo "   RTIG ROADS ANALYSIS - AUTOMATED SETUP & PIPELINE"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Determine the script location
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "📍 Working directory: $SCRIPT_DIR"
echo ""

# Step 1: Check Python version
echo "🐍 Checking Python version..."
PYTHON_VERSION=$(python3 --version 2>&1 | grep -oP '\d+\.\d+')
echo "   Python version: $PYTHON_VERSION"
if ! python3 -c 'import sys; exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; then
    echo "   ⚠️  Warning: Python 3.11+ recommended (you have $PYTHON_VERSION)"
fi
echo ""

# Step 2: Create conda environment if needed
if command -v conda >/dev/null 2>&1; then
    echo "🔧 Preparing conda environment..."
    if ! conda env list | grep -q "^iberdrola-datathon "; then
        conda env create -f environment.yml >/dev/null 2>&1 || conda env create -f environment.yml
        echo "   ✓ Conda environment created"
    else
        echo "   ✓ Conda environment already exists"
    fi

    echo ""
    echo "🚀 Activating conda environment..."
    eval "$(conda shell.bash hook)"
    conda activate iberdrola-datathon
    echo "   ✓ Environment activated"
else
    echo "⚠️  Conda not found. Falling back to local virtual environment."
    if [ ! -d ".venv" ]; then
        echo "🔧 Creating virtual environment..."
        python3 -m venv .venv
        echo "   ✓ Virtual environment created"
    else
        echo "   ✓ Virtual environment already exists"
    fi
    echo ""
    echo "🚀 Activating environment..."
    source .venv/bin/activate
    echo "   ✓ Environment activated"
    echo ""
    echo "📦 Installing dependencies..."
    pip install -q -r requirements.txt 2>/dev/null || pip install -r requirements.txt
    echo "   ✓ Dependencies installed"
fi
echo ""

# Step 5: Run pipeline
echo "⚙️  Running data pipeline..."
export PYTHONPATH="."
python scripts/run_pipeline.py
echo ""

# Step 6: Generate report
echo "📄 Generating analysis report..."
python scripts/build_report.py
echo ""

# Step 7: Summary
echo "════════════════════════════════════════════════════════════════"
echo "✅ SETUP & PIPELINE COMPLETE"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📊 Next steps:"
echo "   1. Open Jupyter notebooks:"
echo "      jupyter notebook notebooks/"
echo ""
echo "   2. View interactive visualizations:"
echo "      • maps/priority_map.html"
echo "      • maps/dashboard.html"
echo "      • maps/density_heatmap.html"
echo ""
echo "   3. Review documentation:"
echo "      • docs/executive_summary.md"
echo "      • docs/analysis_report.txt (full findings)"
echo ""
echo "   4. Explore processed data:"
echo "      • data/processed/roads_processed.parquet (main dataset)"
echo "      • data/processed/roads_scored_final.parquet (scoring results)"
echo ""
echo "════════════════════════════════════════════════════════════════"
