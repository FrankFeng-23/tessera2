#!/usr/bin/env python3
"""
Direct test of landsat_fast_processor.py with a single small time window
"""

import subprocess
import sys
from pathlib import Path

def main():
    # Test parameters
    input_tiff = "/scratch/zf281/tessera/data/cambridge/geoinfo/CB.tiff"
    start_date = "2024-01-01T00:00:00"
    end_date = "2024-01-07T23:59:59"
    output_dir = "/tmp/test_landsat_direct"
    
    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print("Testing Landsat processor directly...")
    print(f"Input: {input_tiff}")
    print(f"Period: {start_date} to {end_date}")
    print(f"Output: {output_dir}")
    
    # Construct command
    cmd = [
        "python3", "landsat_fast_processor.py",
        "--input_tiff", input_tiff,
        "--start_date", start_date,
        "--end_date", end_date,
        "--output", output_dir,
        "--max_cloud", "90",
        "--dask_workers", "1",
        "--worker_memory", "4",
        "--chunksize", "256",
        "--resolution", "10.0",
        "--min_coverage", "5.0",
        "--partition_id", "test_single",
        "--debug"
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    
    # Run the command
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        print("\n" + "="*50)
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("\n" + "="*50)
            print("STDERR:")
            print(result.stderr)
        
        print(f"\nExit code: {result.returncode}")
        
        if result.returncode == 0:
            print("✅ Test completed successfully!")
            
            # Check output files
            output_path = Path(output_dir)
            if output_path.exists():
                print(f"\nOutput directory contents:")
                for item in output_path.rglob("*"):
                    if item.is_file():
                        size_mb = item.stat().st_size / (1024*1024)
                        print(f"  {item.relative_to(output_path)}: {size_mb:.2f} MB")
        else:
            print("❌ Test failed!")
            
    except subprocess.TimeoutExpired:
        print("❌ Test timed out after 5 minutes")
        return 1
    except Exception as e:
        print(f"❌ Error running test: {e}")
        return 1
    
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())