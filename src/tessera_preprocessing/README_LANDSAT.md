# Landsat 8/9 Integration for TESSERA Processing Pipeline

## Overview

This document describes the integration of Landsat 8/9 Collection 2 Level-2 surface reflectance data into the existing TESSERA processing pipeline, extending it from Sentinel-1/Sentinel-2 to include Landsat satellite data.

## Features

### ✅ Landsat 8/9 Support
- **Collection**: Landsat Collection 2 Level-2 Surface Reflectance
- **Platforms**: Landsat 8 and Landsat 9
- **Time Range**: 2017-2024 (prioritized period)
- **Data Source**: Microsoft Planetary Computer STAC API

### ✅ Band Processing
- **Selected Bands**: 6 multispectral bands + 1 QA band
  - `SR_B2`: Blue (450-515nm) → 10m
  - `SR_B3`: Green (525-600nm) → 10m
  - `SR_B4`: Red (630-680nm) → 10m
  - `SR_B5`: NIR (845-885nm) → 10m
  - `SR_B6`: SWIR1 (1560-1660nm) → 10m
  - `SR_B7`: SWIR2 (2100-2300nm) → 10m
  - `QA_PIXEL`: Quality Assessment → 10m

### ✅ 10m Resolution Interpolation
- **Method**: Bilinear interpolation from native 30m to 10m
- **Quality**: Preserves spectral characteristics while enhancing spatial detail
- **Consistency**: Matches Sentinel-2 10m resolution for multi-sensor analysis

### ✅ Quality Assessment
- **QA_PIXEL Analysis**: Comprehensive bit flag interpretation
- **Cloud Detection**: Multi-level confidence assessment
- **Shadow Detection**: Cloud shadow identification and masking
- **Coverage Thresholds**: Configurable minimum valid pixel requirements

### ✅ Dynamic Window Processing
- **Window-based Processing**: Configurable time windows (default: 7 days)
- **Parallel Processing**: Multiple concurrent workers per window
- **Retry Logic**: Automatic retry for failed windows with exponential backoff
- **Timeout Control**: Configurable timeouts for robust processing

## Files Added/Modified

### New Files
```
landsat_fast_processor.py          # Main Landsat processing engine
landsat_quality_assessment.py      # QA_PIXEL quality assessment utilities
requirements_landsat.txt           # Python dependencies for Landsat processing
test_landsat_integration.sh        # Integration test script
README_LANDSAT.md                   # This documentation file
```

### Modified Files
```
s1_s2_downloader_test.sh           # Updated to s1_s2_landsat_downloader.sh
                                   # Added Landsat configuration and processing
```

## Configuration

### Landsat-Specific Parameters

```bash
# === Landsat 8/9 Configuration ===
LANDSAT_ENABLED=true                # Enable Landsat processing
LANDSAT_MAX_PROCESSES=8             # Maximum number of Landsat worker processes
LANDSAT_WINDOW_DAYS=7               # Days per window for Landsat processing
LANDSAT_WORKER_MEMORY=16            # Memory per Landsat worker (GB)
LANDSAT_CHUNKSIZE=512               # Landsat stackstac chunk size
LANDSAT_MAX_CLOUD=80                # Maximum cloud coverage for Landsat (%)
LANDSAT_RESOLUTION=10.0             # Landsat output resolution (meters)
LANDSAT_MIN_COVERAGE=10.0           # Minimum valid pixel coverage for Landsat (%)
LANDSAT_OVERWRITE=false             # Overwrite existing Landsat files
LANDSAT_WINDOW_TIMEOUT=600          # Landsat window processing timeout(s)
LANDSAT_ENABLE_RETRY=true           # Enable retry for failed Landsat windows
```

### Output Directory Structure

```
output_dir/
├── data_landsat_raw/
│   ├── blue/
│   │   └── YYYY-MM-DD_mosaic.tiff
│   ├── green/
│   │   └── YYYY-MM-DD_mosaic.tiff
│   ├── red/
│   │   └── YYYY-MM-DD_mosaic.tiff
│   ├── nir/
│   │   └── YYYY-MM-DD_mosaic.tiff
│   ├── swir16/
│   │   └── YYYY-MM-DD_mosaic.tiff
│   ├── swir22/
│   │   └── YYYY-MM-DD_mosaic.tiff
│   └── qa/
│       └── YYYY-MM-DD_mosaic.tiff
└── landsat_window_queues/
    ├── pending/
    ├── processing/
    ├── done/
    └── failed/
```

## Usage

### Basic Usage

```bash
# Process only Landsat data
LANDSAT_ENABLED=true S1_ENABLED=false S2_ENABLED=false \
bash s1_s2_downloader_test.sh

# Process all three satellite systems in parallel
PARALLEL_PROCESSING=true \
bash s1_s2_downloader_test.sh

# Process with custom parameters
LANDSAT_WINDOW_DAYS=14 LANDSAT_MAX_CLOUD=70 \
bash s1_s2_downloader_test.sh
```

### Advanced Configuration

```bash
# High-performance setup
LANDSAT_MAX_PROCESSES=16 \
LANDSAT_WORKER_MEMORY=32 \
LANDSAT_CHUNKSIZE=1024 \
bash s1_s2_downloader_test.sh

# Conservative setup for limited resources
LANDSAT_MAX_PROCESSES=4 \
LANDSAT_WORKER_MEMORY=8 \
LANDSAT_CHUNKSIZE=256 \
bash s1_s2_downloader_test.sh
```

## Quality Assessment Details

### QA_PIXEL Bit Flags

| Bit | Flag | Description |
|-----|------|-------------|
| 0   | Fill | Fill pixel |
| 1   | Dilated Cloud | Dilated cloud pixel |
| 2   | Cirrus | Cirrus cloud (high confidence) |
| 3   | Cloud | Cloud pixel |
| 4   | Cloud Shadow | Cloud shadow pixel |
| 5   | Snow | Snow pixel |
| 6   | Clear | Clear pixel |
| 7   | Water | Water pixel |
| 8-9 | Cloud Confidence | Cloud confidence (2-bit) |
| 10-11 | Cloud Shadow Confidence | Cloud shadow confidence (2-bit) |
| 12-13 | Snow/Ice Confidence | Snow/ice confidence (2-bit) |
| 14-15 | Cirrus Confidence | Cirrus confidence (2-bit) |

### Validity Criteria

- **Valid Observations**: Clear, Water, Snow pixels
- **Invalid Observations**: Fill, Cloud, Cloud Shadow, Dilated Cloud, Cirrus
- **Confidence Thresholds**: Configurable for cloud, shadow, snow, and cirrus detection
- **Coverage Requirements**: Minimum 10% valid pixels per window (configurable)

## Performance Optimization

### Memory Management
- **Conservative Mode**: Enabled by default for window processing
- **Chunk Size**: Optimized at 512 for balance of memory and performance
- **Memory Limits**: Configurable per-worker memory allocation
- **Garbage Collection**: Automatic cleanup between processing windows

### Network Resilience
- **Retry Logic**: Exponential backoff with jitter
- **Timeout Control**: Per-operation timeout configuration
- **Error Handling**: Comprehensive network error detection and recovery
- **Connection Pooling**: Efficient STAC API connection management

### Processing Efficiency
- **Window-based Processing**: Minimizes memory footprint
- **Parallel Workers**: Configurable concurrent processing
- **Smart Mosaicking**: Quality-based tile selection
- **Vectorized Operations**: Optimized numpy operations for QA processing

## Troubleshooting

### Common Issues

1. **Missing Dependencies**
   ```bash
   # Install required Python packages
   pip install -r requirements_landsat.txt
   ```

2. **Memory Issues**
   ```bash
   # Reduce worker memory and processes
   LANDSAT_WORKER_MEMORY=8 LANDSAT_MAX_PROCESSES=4
   ```

3. **Network Timeouts**
   ```bash
   # Increase timeout settings
   LANDSAT_WINDOW_TIMEOUT=1200
   ```

4. **Insufficient Data**
   ```bash
   # Lower cloud coverage threshold
   LANDSAT_MAX_CLOUD=90
   ```

### Log Files

- **Main Log**: `logs/tessera_processing_YYYYMMDD_HHMMSS.log`
- **Window Logs**: `logs/LANDSAT_YYYY-MM-DD_*_worker*_retry*.log`
- **Dask Reports**: `dask-report-WINDOW_ID.html`

## Data Quality

### Spatial Resolution
- **Original**: 30m (Landsat native resolution)
- **Interpolated**: 10m (bilinear interpolation)
- **Accuracy**: Maintains spectral integrity while enhancing spatial detail

### Temporal Coverage
- **Landsat 8**: February 2013 - present
- **Landsat 9**: October 2021 - present
- **Combined**: Improved temporal resolution with both satellites
- **Revisit**: 8-day combined revisit time

### Spectral Quality
- **Atmospheric Correction**: Collection 2 Level-2 surface reflectance
- **Radiometric Quality**: 14-bit quantization (Landsat 9), 12-bit (Landsat 8)
- **Geometric Quality**: Improved geolocation accuracy in Collection 2

## Integration Testing

Run the comprehensive integration test:

```bash
bash test_landsat_integration.sh
```

Expected output:
```
========================================
Testing Landsat Integration Components
========================================
Testing Landsat processor exists... PASS
Testing Quality assessment module exists... PASS
Testing Main script contains Landsat config... PASS
...
All tests passed! ✅
```

## Future Enhancements

### Potential Improvements
1. **Additional Bands**: Support for thermal bands (10-11)
2. **Cloud Masking**: Advanced cloud masking algorithms
3. **Atmospheric Correction**: Custom atmospheric correction parameters
4. **Multi-Resolution**: Support for different output resolutions
5. **Time Series**: Temporal compositing and gap-filling

### Compatibility
- **Sentinel Integration**: Seamless integration with existing S1/S2 processing
- **Output Format**: Compatible with existing dpixel format expectations
- **Coordinate Systems**: Automatic CRS handling and reprojection
- **Metadata**: Comprehensive metadata preservation

## Support

For issues, questions, or contributions related to the Landsat integration:

1. Check the troubleshooting section above
2. Review log files for detailed error information
3. Run integration tests to verify setup
4. Ensure all dependencies are properly installed

## Version History

- **v1.0.0** (2025-08-10): Initial Landsat 8/9 integration
  - Complete Landsat Collection 2 Level-2 support
  - 6-band processing with 10m interpolation
  - QA_PIXEL quality assessment
  - Dynamic window processing with retry logic
  - Full integration with existing S1/S2 pipeline