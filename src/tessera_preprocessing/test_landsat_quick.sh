#!/usr/bin/env bash
#
# test_landsat_quick.sh — Quick test for fixed Landsat integration
#

set -euo pipefail

# Test with a very small window to verify fixes
export YEAR=2024
export INPUT_TIFF="/scratch/zf281/tessera/data/cambridge/geoinfo/CB.tiff"  
export OUT_DIR="/tmp/test_landsat_$(date +%Y%m%d_%H%M%S)"
export TEMP_DIR="/tmp/landsat_test_tmp"

# Create directories
mkdir -p "$OUT_DIR" "$TEMP_DIR"

# Configure for minimal test
export S1_ENABLED=false
export S2_ENABLED=false  
export LANDSAT_ENABLED=true
export PARALLEL_PROCESSING=false

# Test with just one recent week
export START_TIME="2024-01-01"
export END_TIME="2024-01-07"

export LANDSAT_WINDOW_DAYS=7
export LANDSAT_MAX_PROCESSES=1
export LANDSAT_WORKER_MEMORY=4
export LANDSAT_MAX_CLOUD=90
export DEBUG=true

echo "Testing Landsat integration with minimal setup..."
echo "Output directory: $OUT_DIR"
echo "Test period: $START_TIME to $END_TIME"

# Update the script to use new variables
sed -i.bak \
    -e "s|^YEAR=.*|YEAR=$YEAR|" \
    -e "s|^INPUT_TIFF=.*|INPUT_TIFF=\"$INPUT_TIFF\"|" \
    -e "s|^OUT_DIR=.*|OUT_DIR=\"$OUT_DIR\"|" \
    -e "s|^export TEMP_DIR=.*|export TEMP_DIR=\"$TEMP_DIR\"|" \
    -e "s|^S1_ENABLED=.*|S1_ENABLED=$S1_ENABLED|" \
    -e "s|^S2_ENABLED=.*|S2_ENABLED=$S2_ENABLED|" \
    -e "s|^LANDSAT_ENABLED=.*|LANDSAT_ENABLED=$LANDSAT_ENABLED|" \
    -e "s|^START_TIME=.*|START_TIME=\"$START_TIME\"|" \
    -e "s|^END_TIME=.*|END_TIME=\"$END_TIME\"|" \
    -e "s|^LANDSAT_WINDOW_DAYS=.*|LANDSAT_WINDOW_DAYS=$LANDSAT_WINDOW_DAYS|" \
    -e "s|^LANDSAT_MAX_PROCESSES=.*|LANDSAT_MAX_PROCESSES=$LANDSAT_MAX_PROCESSES|" \
    -e "s|^LANDSAT_WORKER_MEMORY=.*|LANDSAT_WORKER_MEMORY=$LANDSAT_WORKER_MEMORY|" \
    -e "s|^LANDSAT_MAX_CLOUD=.*|LANDSAT_MAX_CLOUD=$LANDSAT_MAX_CLOUD|" \
    -e "s|^DEBUG=.*|DEBUG=$DEBUG|" \
    s1_s2_downloader_test.sh

echo "Running test..."
bash s1_s2_downloader_test.sh

echo "Test completed. Check output in: $OUT_DIR"

# Restore original
mv s1_s2_downloader_test.sh.bak s1_s2_downloader_test.sh