#!/usr/bin/env bash
#
# s1_s2_landsat_downloader.sh — Sentinel-1, Sentinel-2 & Landsat 8/9 Parallel/Sequential Processing Pipeline
# Dependencies: bash ≥4, GNU coreutils, Python ≥3.7
# Usage: bash s1_s2_landsat_downloader.sh

# set -euo pipefail
set -u

#######################################
# USER CONFIGURABLE PARAMETERS
#######################################

# === Basic Configuration ===
YEAR=2024 # Range [2017-2024]

INPUT_TIFF="/scratch/zf281/tessera2/data/uk_tile_test/geoinfo/grid_0.45_50.85.tiff"
OUT_DIR="/scratch/zf281/tessera2/data/uk_tile_test/output"
OUT_DIR="${OUT_DIR}/${YEAR}"

export TEMP_DIR="/scratch/zf281/global_d_pixel_generation/tmp_data"     # Temporary file directory

mkdir -p "$OUT_DIR"

# Python environment path
PYTHON_ENV="/maps/zf281/miniconda3/envs/detectree-env/bin/python"

# === Processing Control ===
PARALLEL_PROCESSING=false          # Enable parallel S1/S2/Landsat processing (default: false for sequential)

# === Sentinel-1, Sentinel-2 & Landsat Processing Configuration ===
RESOLUTION=10.0  # Resolution of the input TIFF, also the default output resolution (meters)

# LANDSAT_RESOLUTION=30.0  # Landsat output resolution (meters)

# === Sentinel-1 Configuration (UNCHANGED - Original Logic) ===
S1_ENABLED=false                    # Enable S1 processing
S1_PARTITIONS=16                   # Number of S1 parallel partitions
S1_TOTAL_WORKERS=16                # Total number of S1 Dask workers
S1_WORKER_MEMORY=16                 # Memory per S1 worker (GB)
S1_CHUNKSIZE=1024                  # S1 stackstac chunk size
S1_ORBIT_STATE="both"              # Orbit state: ascending/descending/both
S1_MIN_COVERAGE=10.0               # Minimum valid pixel coverage for S1 (%)
S1_RESOLUTION=$RESOLUTION          # S1 output resolution (meters)
S1_OVERWRITE=false                  # Overwrite existing S1 files

# === Sentinel-2 Configuration (NEW - Dynamic Window Logic) ===
S2_ENABLED=false                     # Enable S2 processing
S2_MAX_PROCESSES=8                  # Maximum number of S2 worker processes
S2_WINDOW_DAYS=7                   # Days per window for S2 processing (changed from 15 to 20)
S2_WORKER_MEMORY=16                  # Memory per S2 worker (GB)
S2_CHUNKSIZE=1024                   # S2 stackstac chunk size
S2_MAX_CLOUD=90                     # Maximum cloud coverage for S2 (%)
S2_RESOLUTION=$RESOLUTION           # S2 output resolution (meters)
S2_MIN_COVERAGE=10.0                # Minimum valid pixel coverage for S2 (%)
S2_OVERWRITE=false                  # Overwrite existing S2 files
S2_WINDOW_TIMEOUT=480               # S2 window processing timeout(s)
S2_ENABLE_RETRY=true                # Enable retry for failed S2 windows

# === Landsat 8/9 Configuration (NEW - Dynamic Window Logic) ===
LANDSAT_ENABLED=true                # Enable Landsat processing
LANDSAT_MAX_PROCESSES=8             # Maximum number of Landsat worker processes
LANDSAT_WINDOW_DAYS=7               # Days per window for Landsat processing
LANDSAT_WORKER_MEMORY=16            # Memory per Landsat worker (GB)
LANDSAT_CHUNKSIZE=512               # Landsat stackstac chunk size
LANDSAT_MAX_CLOUD=90                # Maximum cloud coverage for Landsat (%)
LANDSAT_RESOLUTION=$RESOLUTION      # Landsat output resolution (meters)
LANDSAT_MIN_COVERAGE=10.0           # Minimum valid pixel coverage for Landsat (%)
LANDSAT_OVERWRITE=false             # Overwrite existing Landsat files
LANDSAT_WINDOW_TIMEOUT=600          # Landsat window processing timeout(s)
LANDSAT_ENABLE_RETRY=true           # Enable retry for failed Landsat windows

# === System Configuration ===
DEBUG=false                        # Enable debug mode
LOG_INTERVAL=10                    # Progress update interval (seconds)

#######################################
# Internal Variables (Be Careful to Modify)
#######################################
START_TIME="${YEAR}-06-01"
END_TIME="${YEAR}-07-01" # only for debugging

SCRIPT_START_TIME=$(date +%s)
SCRIPT_NAME=$(basename "$0")
LOG_DIR="${OUT_DIR}/logs"
MAIN_LOG="${LOG_DIR}/tessera_processing_$(date +%Y%m%d_%H%M%S).log"
S1_OUTPUT="${OUT_DIR}/data_sar_raw"
S2_OUTPUT="${OUT_DIR}/data_raw"
LANDSAT_OUTPUT="${OUT_DIR}/data_landsat_raw"

# S2 Window queue directories (NEW)
S2_WINDOW_QUEUE_BASE="${OUT_DIR}/s2_window_queues"
S2_WINDOW_PENDING="${S2_WINDOW_QUEUE_BASE}/pending"
S2_WINDOW_PROCESSING="${S2_WINDOW_QUEUE_BASE}/processing"
S2_WINDOW_DONE="${S2_WINDOW_QUEUE_BASE}/done"
S2_WINDOW_FAILED="${S2_WINDOW_QUEUE_BASE}/failed"

# Landsat Window queue directories (NEW)
LANDSAT_WINDOW_QUEUE_BASE="${OUT_DIR}/landsat_window_queues"
LANDSAT_WINDOW_PENDING="${LANDSAT_WINDOW_QUEUE_BASE}/pending"
LANDSAT_WINDOW_PROCESSING="${LANDSAT_WINDOW_QUEUE_BASE}/processing"
LANDSAT_WINDOW_DONE="${LANDSAT_WINDOW_QUEUE_BASE}/done"
LANDSAT_WINDOW_FAILED="${LANDSAT_WINDOW_QUEUE_BASE}/failed"

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

#######################################
# Logging Function
#######################################
log() {
    local level=$1
    shift
    local message="$@"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case $level in
        INFO)
            echo -e "${timestamp} ${BLUE}[INFO]${NC} $message" | tee -a "$MAIN_LOG"
            ;;
        SUCCESS)
            echo -e "${timestamp} ${GREEN}[SUCCESS]${NC} $message" | tee -a "$MAIN_LOG"
            ;;
        WARNING)
            echo -e "${timestamp} ${YELLOW}[WARNING]${NC} $message" | tee -a "$MAIN_LOG"
            ;;
        ERROR)
            echo -e "${timestamp} ${RED}[ERROR]${NC} $message" | tee -a "$MAIN_LOG"
            ;;
        HEADER)
            echo -e "${PURPLE}═══════════════════════════════════════════════════════════════════${NC}" | tee -a "$MAIN_LOG"
            echo -e "${timestamp} ${PURPLE}$message${NC}" | tee -a "$MAIN_LOG"
            echo -e "${PURPLE}═══════════════════════════════════════════════════════════════════${NC}" | tee -a "$MAIN_LOG"
            ;;
    esac
}

#######################################
# Utility Functions
#######################################
# Convert timestamp to seconds
time_to_seconds() {
    date -d "$1" +%s
}

# Convert seconds to date format
seconds_to_date() {
    date -d "@$1" +"%Y-%m-%d"
}

# Format duration
format_duration() {
    local duration=$1
    local hours=$((duration / 3600))
    local minutes=$(( (duration % 3600) / 60 ))
    local seconds=$((duration % 60))
    echo "${hours}h ${minutes}m ${seconds}s"
}

# Calculate time partitions (FOR S1 - UNCHANGED)
calculate_partitions() {
    local start_sec=$(time_to_seconds "$START_TIME")
    local end_sec=$(time_to_seconds "$END_TIME")
    local partitions=$1
    local total_seconds=$((end_sec - start_sec + 86400))
    local seconds_per_partition=$((total_seconds / partitions))
    
    for ((i=0; i<partitions; i++)); do
        local partition_start_sec
        local partition_end_sec
        
        if [[ $i -eq 0 ]]; then
            partition_start_sec=$start_sec
        else
            partition_start_sec=$((start_sec + i * seconds_per_partition))
        fi
        
        if [[ $i -eq $((partitions - 1)) ]]; then
            partition_end_sec=$end_sec
        else
            partition_end_sec=$((start_sec + (i + 1) * seconds_per_partition - 86400))
        fi
        
        local p_start=$(seconds_to_date $partition_start_sec)
        local p_end=$(seconds_to_date $partition_end_sec)
        
        echo "$p_start,$p_end"
    done
}

# Calculate workers per partition (FOR S1 - UNCHANGED)
calculate_workers_per_partition() {
    local total_workers=$1
    local partitions=$2
    local workers_per_partition=$((total_workers / partitions))
    local remaining_workers=$((total_workers % partitions))
    
    for ((i=0; i<partitions; i++)); do
        if [[ $i -lt $remaining_workers ]]; then
            echo $((workers_per_partition + 1))
        else
            echo $workers_per_partition
        fi
    done
}

# Generate partition ID (FOR S1 - UNCHANGED)
generate_partition_id() {
    local p_start=$1
    local p_end=$2
    local p_index=$3
    local prefix=$4
    
    local start_year=$(date -d "${p_start}" +%Y)
    local start_month=$(date -d "${p_start}" +%m)
    local start_day=$(date -d "${p_start}" +%d)
    local end_year=$(date -d "${p_end}" +%Y)
    local end_month=$(date -d "${p_end}" +%m)
    local end_day=$(date -d "${p_end}" +%d)
    
    if [[ "$start_year" == "$end_year" && "$start_month" == "$end_month" ]]; then
        if [[ "$start_day" == "$end_day" ]]; then
            echo "${prefix}_P${p_index}_${start_year}${start_month}${start_day}"
        else
            echo "${prefix}_P${p_index}_${start_year}${start_month}${start_day}-${end_day}"
        fi
    elif [[ "$start_year" == "$end_year" ]]; then
        echo "${prefix}_P${p_index}_${start_year}${start_month}${start_day}-${end_month}${end_day}"
    else
        echo "${prefix}_P${p_index}_${start_year}${start_month}${start_day}-${end_year}${end_month}${end_day}"
    fi
}

# Generate window ID (FOR S2 - NEW)
generate_window_id() {
    local p_start=$1
    local p_end=$2
    local p_index=$3
    local prefix=$4
    
    local start_year=$(date -d "${p_start}" +%Y)
    local start_month=$(date -d "${p_start}" +%m)
    local start_day=$(date -d "${p_start}" +%d)
    local end_year=$(date -d "${p_end}" +%Y)
    local end_month=$(date -d "${p_end}" +%m)
    local end_day=$(date -d "${p_end}" +%d)
    
    if [[ "$start_year" == "$end_year" && "$start_month" == "$end_month" ]]; then
        if [[ "$start_day" == "$end_day" ]]; then
            echo "${prefix}_W${p_index}_${start_year}${start_month}${start_day}"
        else
            echo "${prefix}_W${p_index}_${start_year}${start_month}${start_day}-${end_day}"
        fi
    elif [[ "$start_year" == "$end_year" ]]; then
        echo "${prefix}_W${p_index}_${start_year}${start_month}${start_day}-${end_month}${end_day}"
    else
        echo "${prefix}_W${p_index}_${start_year}${start_month}${start_day}-${end_year}${end_month}${end_day}"
    fi
}

#######################################
# S1 Monitoring Function (UNCHANGED)
#######################################
monitor_processes() {
    local -n pids=$1
    local -n partition_ids=$2
    local -n start_times=$3
    local -n completed=$4
    local -n failed=$5
    local prefix=$6
    
    declare -A finished_pids
    
    while true; do
        local all_done=true
        local running_count=0
        
        for i in "${!pids[@]}"; do
            local pid=${pids[i]}
            if [[ -n "${finished_pids[$pid]:-}" ]]; then
                continue
            fi
            
            if kill -0 $pid 2>/dev/null; then
                all_done=false
                running_count=$((running_count + 1))
            else
                local end_time=$(date +%s)
                local duration=$((end_time - ${start_times[i]}))
                local partition_id=${partition_ids[i]}
                
                wait $pid
                local exit_code=$?
                
                finished_pids[$pid]=1
                
                if [ $exit_code -eq 0 ]; then
                    log SUCCESS "$prefix partition $partition_id completed ($(format_duration $duration))"
                    completed+=("$partition_id")
                else
                    log ERROR "$prefix partition $partition_id failed with exit code $exit_code ($(format_duration $duration))"
                    failed+=("$partition_id")
                fi
            fi
        done
        
        if ! $all_done; then
            echo -ne "\r${CYAN}[$(date '+%H:%M:%S')]${NC} $prefix: ${running_count}/${#pids[@]} partitions running...${NC}"
        fi
        
        if $all_done; then
            echo -ne "\r\033[K"  # Clear the line
            break
        fi
        
        sleep $LOG_INTERVAL
    done
}

#######################################
# S2 Window Queue Management (NEW)
#######################################
# Generate time windows for S2 processing
generate_s2_windows() {
    local window_days=$1
    local pending_dir=$2
    
    local start_sec=$(time_to_seconds "$START_TIME")
    local end_sec=$(time_to_seconds "$END_TIME")
    local window_seconds=$((window_days * 86400))
    
    local window_index=0
    local current_start=$start_sec
    
    while [ $current_start -le $end_sec ]; do
        local current_end=$((current_start + window_seconds - 1))
        
        # Ensure we don't exceed the end date
        if [ $current_end -gt $end_sec ]; then
            current_end=$end_sec
        fi
        
        local w_start=$(seconds_to_date $current_start)
        local w_end=$(seconds_to_date $current_end)
        local window_id=$(generate_window_id "$w_start" "$w_end" "$window_index" "S2")
        
        # Create window file
        local window_file="${pending_dir}/${window_id}.window"
        echo "start_date=${w_start}" > "$window_file"
        echo "end_date=${w_end}" >> "$window_file"
        echo "window_id=${window_id}" >> "$window_file"
        echo "created=\"$(date '+%Y-%m-%d %H:%M:%S')\"" >> "$window_file"
        echo "retry_count=0" >> "$window_file"
        
        log INFO "Created S2 window: $window_id ($w_start to $w_end)"
        
        window_index=$((window_index + 1))
        current_start=$((current_end + 1))
    done
    
    log INFO "Generated $window_index windows for S2 processing (${window_days} days per window)"
}

# Get next available window from queue
get_next_s2_window() {
    local pending_dir=$1
    local processing_dir=$2
    local worker_id=$3
    
    # Find the oldest pending window
    local window_file=$(find "$pending_dir" -name "*.window" -type f 2>/dev/null | head -1)
    
    if [[ -z "$window_file" ]]; then
        return 1  # No windows available
    fi
    
    # Atomically move to processing
    local window_basename=$(basename "$window_file")
    local processing_file="${processing_dir}/${worker_id}_${window_basename}"
    
    if mv "$window_file" "$processing_file" 2>/dev/null; then
        echo "$processing_file"
        return 0
    else
        return 1  # Another worker got it first
    fi
}

# Mark window as completed
complete_s2_window() {
    local processing_file=$1
    local done_dir=$2
    
    local window_basename=$(basename "$processing_file")
    # Remove worker prefix from filename
    local clean_basename=${window_basename#*_}
    local done_file="${done_dir}/${clean_basename}"
    
    echo "completed=\"$(date '+%Y-%m-%d %H:%M:%S')\"" >> "$processing_file"
    mv "$processing_file" "$done_file"
}

# Mark window as failed
fail_s2_window() {
    local processing_file=$1
    local failed_dir=$2
    local error_msg=$3
    
    local window_basename=$(basename "$processing_file")
    # Remove worker prefix from filename
    local clean_basename=${window_basename#*_}
    local failed_file="${failed_dir}/${clean_basename}"
    
    # Update retry count
    local retry_count=0
    if grep -q "retry_count=" "$processing_file"; then
        retry_count=$(grep "retry_count=" "$processing_file" | cut -d'=' -f2)
    fi
    retry_count=$((retry_count + 1))
    
    # Update the file
    if grep -q "retry_count=" "$processing_file"; then
        sed -i "s/retry_count=.*/retry_count=${retry_count}/" "$processing_file"
    else
        echo "retry_count=${retry_count}" >> "$processing_file"
    fi
    
    echo "failed=\"$(date '+%Y-%m-%d %H:%M:%S')\"" >> "$processing_file"
    echo "error=${error_msg}" >> "$processing_file"
    mv "$processing_file" "$failed_file"
}

# Kill all S2 workers
kill_s2_workers() {
    local -n worker_pids=$1
    
    for pid in "${worker_pids[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            log INFO "Terminating S2 worker with PID $pid"
            kill -TERM $pid 2>/dev/null
        fi
    done
    
    # Give workers time to terminate gracefully
    sleep 3
    
    # Force kill if still running
    for pid in "${worker_pids[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            log WARNING "Force killing S2 worker with PID $pid"
            kill -KILL $pid 2>/dev/null
        fi
    done
    
    # Wait for all processes to finish
    for pid in "${worker_pids[@]}"; do
        wait $pid 2>/dev/null || true
    done
}

#######################################
# S2 Worker Function (NEW)
#######################################
s2_worker() {
    local worker_id=$1
    local worker_name="S2_Worker_${worker_id}"
    
    log INFO "Starting $worker_name"
    
    while true; do
        # Get next window
        local window_file=$(get_next_s2_window "$S2_WINDOW_PENDING" "$S2_WINDOW_PROCESSING" "$worker_id")
        
        if [[ $? -ne 0 ]]; then
            # No more windows, exit gracefully
            log INFO "$worker_name: No more windows available, exiting"
            break
        fi
        
        # Parse window information
        while IFS='=' read -r key value; do
            case "$key" in
                start_date) start_date="${value//\"/}" ;;
                end_date) end_date="${value//\"/}" ;;
                window_id) window_id="${value//\"/}" ;;
                retry_count) retry_count="${value//\"/}" ;;
            esac
        done < "$window_file"
        
        log INFO "$worker_name: Processing window $window_id ($start_date to $end_date) [Retry: ${retry_count:-0}]"
        
        # Set timeout for window processing
        local window_start_time=$(date +%s)
        local timeout_exceeded=false
        
        # Create worker-specific log file
        local worker_log="${LOG_DIR}/${window_id}_worker${worker_id}_retry${retry_count:-0}.log"
        
        # Add time to S2 (include full day)
        local s2_start="${start_date}T00:00:00"
        local s2_end="${end_date}T23:59:59"
        
        # Process with timeout
        (
            timeout $S2_WINDOW_TIMEOUT $PYTHON_ENV s2_fast_processor_test.py \
                --input_tiff "$INPUT_TIFF" \
                --start_date "$s2_start" \
                --end_date "$s2_end" \
                --output "$S2_OUTPUT" \
                --max_cloud "$S2_MAX_CLOUD" \
                --dask_workers 1 \
                --worker_memory "$S2_WORKER_MEMORY" \
                --chunksize "$S2_CHUNKSIZE" \
                --resolution "$S2_RESOLUTION" \
                --min_coverage "$S2_MIN_COVERAGE" \
                --partition_id "$window_id" \
                $([ "$S2_OVERWRITE" == "true" ] && echo "--overwrite") \
                $([ "$DEBUG" == "true" ] && echo "--debug") \
                > "$worker_log" 2>&1
        ) &
        
        local process_pid=$!
        
        # Monitor process with timeout
        while kill -0 $process_pid 2>/dev/null; do
            local current_time=$(date +%s)
            local elapsed=$((current_time - window_start_time))
            
            if [ $elapsed -ge $S2_WINDOW_TIMEOUT ]; then
                kill -TERM $process_pid 2>/dev/null
                sleep 5
                kill -KILL $process_pid 2>/dev/null
                timeout_exceeded=true
                break
            fi
            
            sleep 5
        done
        
        # Check results
        wait $process_pid 2>/dev/null
        local exit_code=$?
        local processing_time=$(($(date +%s) - window_start_time))
        
        if [ $timeout_exceeded == true ]; then
            log WARNING "$worker_name: Window $window_id processing timed out after ${S2_WINDOW_TIMEOUT}s"
            fail_s2_window "$window_file" "$S2_WINDOW_FAILED" "timeout_after_${S2_WINDOW_TIMEOUT}s"
        elif [ $exit_code -eq 0 ]; then
            log SUCCESS "$worker_name: Window $window_id completed successfully ($(format_duration $processing_time))"
            complete_s2_window "$window_file" "$S2_WINDOW_DONE"
        else
            log ERROR "$worker_name: Window $window_id failed with exit code $exit_code ($(format_duration $processing_time))"
            fail_s2_window "$window_file" "$S2_WINDOW_FAILED" "exit_code_${exit_code}"
        fi
    done
    
    log INFO "$worker_name: Worker finished"
}

#######################################
# S2 Processing Round Function (NEW)
#######################################
run_s2_processing_round() {
    local round_name="$1"
    log INFO "Starting S2 processing round: $round_name"
    
    # Count initial windows
    local initial_windows=$(find "$S2_WINDOW_PENDING" -name "*.window" -type f | wc -l)
    if [ $initial_windows -eq 0 ]; then
        log INFO "No windows to process in $round_name"
        return 0
    fi
    
    log INFO "$round_name: $initial_windows windows to process"
    
    # Start worker processes
    local s2_pids=()
    local actual_processes=$(( initial_windows < S2_MAX_PROCESSES ? initial_windows : S2_MAX_PROCESSES ))
    
    for ((i=0; i<actual_processes; i++)); do
        s2_worker $i &
        s2_pids+=($!)
        sleep 1  # Brief delay between starting workers
    done
    
    log INFO "$round_name: Started ${#s2_pids[@]} worker processes"
    
    # Monitor progress until all windows are processed
    local start_monitor_time=$(date +%s)
    while true; do
        local pending_count=$(find "$S2_WINDOW_PENDING" -name "*.window" -type f | wc -l)
        local processing_count=$(find "$S2_WINDOW_PROCESSING" -name "*.window" -type f | wc -l)
        local done_count=$(find "$S2_WINDOW_DONE" -name "*.window" -type f | wc -l)
        local failed_count=$(find "$S2_WINDOW_FAILED" -name "*.window" -type f | wc -l)
        
        # Check if all windows are processed (pending + processing = 0)
        if [ $pending_count -eq 0 ] && [ $processing_count -eq 0 ]; then
            log INFO "$round_name: All windows processed, stopping workers..."
            kill_s2_workers s2_pids
            break
        fi
        
        # Check if any workers are still running
        local running_workers=0
        for pid in "${s2_pids[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                running_workers=$((running_workers + 1))
            fi
        done
        
        if [ $running_workers -eq 0 ] && [ $pending_count -gt 0 ]; then
            log WARNING "$round_name: All workers exited but $pending_count windows remain. This may indicate a processing issue."
            break
        fi
        
        # Status update
        local elapsed=$(( $(date +%s) - start_monitor_time ))
        echo -ne "\r${CYAN}[$(date '+%H:%M:%S')]${NC} $round_name: ${running_workers}/${actual_processes} workers | Pending: ${pending_count} | Processing: ${processing_count} | Done: ${done_count} | Failed: ${failed_count} | Elapsed: $(format_duration $elapsed)"
        
        sleep $LOG_INTERVAL
    done
    
    echo  # New line after progress updates
    
    # Ensure all workers are stopped
    kill_s2_workers s2_pids
    
    # Final count for this round
    local final_done=$(find "$S2_WINDOW_DONE" -name "*.window" -type f | wc -l)
    local final_failed=$(find "$S2_WINDOW_FAILED" -name "*.window" -type f | wc -l)
    local round_elapsed=$(( $(date +%s) - start_monitor_time ))
    
    log INFO "$round_name completed in $(format_duration $round_elapsed)"
    log INFO "$round_name summary: Done: $final_done, Failed: $final_failed"
}

#######################################
# Landsat Window Queue Management (NEW)
#######################################
# Generate time windows for Landsat processing
generate_landsat_windows() {
    local window_days=$1
    local pending_dir=$2
    
    local start_sec=$(time_to_seconds "$START_TIME")
    local end_sec=$(time_to_seconds "$END_TIME")
    local window_seconds=$((window_days * 86400))
    
    local window_index=0
    local current_start=$start_sec
    
    while [ $current_start -le $end_sec ]; do
        local current_end=$((current_start + window_seconds - 1))
        
        # Ensure we don't exceed the end date
        if [ $current_end -gt $end_sec ]; then
            current_end=$end_sec
        fi
        
        local w_start=$(seconds_to_date $current_start)
        local w_end=$(seconds_to_date $current_end)
        local window_id=$(generate_window_id "$w_start" "$w_end" "$window_index" "LANDSAT")
        
        # Create window file
        local window_file="${pending_dir}/${window_id}.window"
        echo "start_date=${w_start}" > "$window_file"
        echo "end_date=${w_end}" >> "$window_file"
        echo "window_id=${window_id}" >> "$window_file"
        echo "created=\"$(date '+%Y-%m-%d %H:%M:%S')\"" >> "$window_file"
        echo "retry_count=0" >> "$window_file"
        
        log INFO "Created Landsat window: $window_id ($w_start to $w_end)"
        
        window_index=$((window_index + 1))
        current_start=$((current_end + 1))
    done
    
    log INFO "Generated $window_index windows for Landsat processing (${window_days} days per window)"
}

# Get next available window from queue
get_next_landsat_window() {
    local pending_dir=$1
    local processing_dir=$2
    local worker_id=$3
    
    # Find the oldest pending window
    local window_file=$(find "$pending_dir" -name "*.window" -type f 2>/dev/null | head -1)
    
    if [[ -z "$window_file" ]]; then
        return 1  # No windows available
    fi
    
    # Atomically move to processing
    local window_basename=$(basename "$window_file")
    local processing_file="${processing_dir}/${worker_id}_${window_basename}"
    
    if mv "$window_file" "$processing_file" 2>/dev/null; then
        echo "$processing_file"
        return 0
    else
        return 1  # Another worker got it first
    fi
}

# Mark window as completed
complete_landsat_window() {
    local processing_file=$1
    local done_dir=$2
    
    local window_basename=$(basename "$processing_file")
    # Remove worker prefix from filename
    local clean_basename=${window_basename#*_}
    local done_file="${done_dir}/${clean_basename}"
    
    echo "completed=\"$(date '+%Y-%m-%d %H:%M:%S')\"" >> "$processing_file"
    mv "$processing_file" "$done_file"
}

# Mark window as failed
fail_landsat_window() {
    local processing_file=$1
    local failed_dir=$2
    local error_msg=$3
    
    local window_basename=$(basename "$processing_file")
    # Remove worker prefix from filename
    local clean_basename=${window_basename#*_}
    local failed_file="${failed_dir}/${clean_basename}"
    
    # Update retry count
    local retry_count=0
    if grep -q "retry_count=" "$processing_file"; then
        retry_count=$(grep "retry_count=" "$processing_file" | cut -d'=' -f2)
    fi
    retry_count=$((retry_count + 1))
    
    # Update the file
    if grep -q "retry_count=" "$processing_file"; then
        sed -i "s/retry_count=.*/retry_count=${retry_count}/" "$processing_file"
    else
        echo "retry_count=${retry_count}" >> "$processing_file"
    fi
    
    echo "failed=\"$(date '+%Y-%m-%d %H:%M:%S')\"" >> "$processing_file"
    echo "error=${error_msg}" >> "$processing_file"
    mv "$processing_file" "$failed_file"
}

# Kill all Landsat workers
kill_landsat_workers() {
    local -n worker_pids=$1
    
    for pid in "${worker_pids[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            log INFO "Terminating Landsat worker with PID $pid"
            kill -TERM $pid 2>/dev/null
        fi
    done
    
    # Give workers time to terminate gracefully
    sleep 3
    
    # Force kill if still running
    for pid in "${worker_pids[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            log WARNING "Force killing Landsat worker with PID $pid"
            kill -KILL $pid 2>/dev/null
        fi
    done
    
    # Wait for all processes to finish
    for pid in "${worker_pids[@]}"; do
        wait $pid 2>/dev/null || true
    done
}

#######################################
# Landsat Worker Function (NEW)
#######################################
landsat_worker() {
    local worker_id=$1
    local worker_name="Landsat_Worker_${worker_id}"
    
    log INFO "Starting $worker_name"
    
    while true; do
        # Get next window
        local window_file=$(get_next_landsat_window "$LANDSAT_WINDOW_PENDING" "$LANDSAT_WINDOW_PROCESSING" "$worker_id")
        
        if [[ $? -ne 0 ]]; then
            # No more windows, exit gracefully
            log INFO "$worker_name: No more windows available, exiting"
            break
        fi
        
        # Parse window information
        while IFS='=' read -r key value; do
            case "$key" in
                start_date) start_date="${value//\"/}" ;;
                end_date) end_date="${value//\"/}" ;;
                window_id) window_id="${value//\"/}" ;;
                retry_count) retry_count="${value//\"/}" ;;
            esac
        done < "$window_file"
        
        log INFO "$worker_name: Processing window $window_id ($start_date to $end_date) [Retry: ${retry_count:-0}]"
        
        # Set timeout for window processing
        local window_start_time=$(date +%s)
        local timeout_exceeded=false
        
        # Create worker-specific log file
        local worker_log="${LOG_DIR}/${window_id}_worker${worker_id}_retry${retry_count:-0}.log"
        
        # Add time to Landsat (include full day)
        local landsat_start="${start_date}T00:00:00"
        local landsat_end="${end_date}T23:59:59"
        
        # Process with timeout
        (
            timeout $LANDSAT_WINDOW_TIMEOUT $PYTHON_ENV landsat_fast_processor.py \
                --input_tiff "$INPUT_TIFF" \
                --start_date "$landsat_start" \
                --end_date "$landsat_end" \
                --output "$LANDSAT_OUTPUT" \
                --max_cloud "$LANDSAT_MAX_CLOUD" \
                --dask_workers 1 \
                --worker_memory "$LANDSAT_WORKER_MEMORY" \
                --chunksize "$LANDSAT_CHUNKSIZE" \
                --resolution "$LANDSAT_RESOLUTION" \
                --min_coverage "$LANDSAT_MIN_COVERAGE" \
                --partition_id "$window_id" \
                $([ "$LANDSAT_OVERWRITE" == "true" ] && echo "--overwrite") \
                $([ "$DEBUG" == "true" ] && echo "--debug") \
                > "$worker_log" 2>&1
        ) &
        
        local process_pid=$!
        
        # Monitor process with timeout
        while kill -0 $process_pid 2>/dev/null; do
            local current_time=$(date +%s)
            local elapsed=$((current_time - window_start_time))
            
            if [ $elapsed -ge $LANDSAT_WINDOW_TIMEOUT ]; then
                kill -TERM $process_pid 2>/dev/null
                sleep 5
                kill -KILL $process_pid 2>/dev/null
                timeout_exceeded=true
                break
            fi
            
            sleep 5
        done
        
        # Check results
        wait $process_pid 2>/dev/null
        local exit_code=$?
        local processing_time=$(($(date +%s) - window_start_time))
        
        if [ $timeout_exceeded == true ]; then
            log WARNING "$worker_name: Window $window_id processing timed out after ${LANDSAT_WINDOW_TIMEOUT}s"
            fail_landsat_window "$window_file" "$LANDSAT_WINDOW_FAILED" "timeout_after_${LANDSAT_WINDOW_TIMEOUT}s"
        elif [ $exit_code -eq 0 ]; then
            log SUCCESS "$worker_name: Window $window_id completed successfully ($(format_duration $processing_time))"
            complete_landsat_window "$window_file" "$LANDSAT_WINDOW_DONE"
        else
            log ERROR "$worker_name: Window $window_id failed with exit code $exit_code ($(format_duration $processing_time))"
            fail_landsat_window "$window_file" "$LANDSAT_WINDOW_FAILED" "exit_code_${exit_code}"
        fi
    done
    
    log INFO "$worker_name: Worker finished"
}

#######################################
# Landsat Processing Round Function (NEW)
#######################################
run_landsat_processing_round() {
    local round_name="$1"
    log INFO "Starting Landsat processing round: $round_name"
    
    # Count initial windows
    local initial_windows=$(find "$LANDSAT_WINDOW_PENDING" -name "*.window" -type f | wc -l)
    if [ $initial_windows -eq 0 ]; then
        log INFO "No windows to process in $round_name"
        return 0
    fi
    
    log INFO "$round_name: $initial_windows windows to process"
    
    # Start worker processes
    local landsat_pids=()
    local actual_processes=$(( initial_windows < LANDSAT_MAX_PROCESSES ? initial_windows : LANDSAT_MAX_PROCESSES ))
    
    for ((i=0; i<actual_processes; i++)); do
        landsat_worker $i &
        landsat_pids+=($!)
        sleep 1  # Brief delay between starting workers
    done
    
    log INFO "$round_name: Started ${#landsat_pids[@]} worker processes"
    
    # Monitor progress until all windows are processed
    local start_monitor_time=$(date +%s)
    while true; do
        local pending_count=$(find "$LANDSAT_WINDOW_PENDING" -name "*.window" -type f | wc -l)
        local processing_count=$(find "$LANDSAT_WINDOW_PROCESSING" -name "*.window" -type f | wc -l)
        local done_count=$(find "$LANDSAT_WINDOW_DONE" -name "*.window" -type f | wc -l)
        local failed_count=$(find "$LANDSAT_WINDOW_FAILED" -name "*.window" -type f | wc -l)
        
        # Check if all windows are processed (pending + processing = 0)
        if [ $pending_count -eq 0 ] && [ $processing_count -eq 0 ]; then
            log INFO "$round_name: All windows processed, stopping workers..."
            kill_landsat_workers landsat_pids
            break
        fi
        
        # Check if any workers are still running
        local running_workers=0
        for pid in "${landsat_pids[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                running_workers=$((running_workers + 1))
            fi
        done
        
        if [ $running_workers -eq 0 ] && [ $pending_count -gt 0 ]; then
            log WARNING "$round_name: All workers exited but $pending_count windows remain. This may indicate a processing issue."
            break
        fi
        
        # Status update
        local elapsed=$(( $(date +%s) - start_monitor_time ))
        echo -ne "\r${CYAN}[$(date '+%H:%M:%S')]${NC} $round_name: ${running_workers}/${actual_processes} workers | Pending: ${pending_count} | Processing: ${processing_count} | Done: ${done_count} | Failed: ${failed_count} | Elapsed: $(format_duration $elapsed)"
        
        sleep $LOG_INTERVAL
    done
    
    echo  # New line after progress updates
    
    # Ensure all workers are stopped
    kill_landsat_workers landsat_pids
    
    # Final count for this round
    local final_done=$(find "$LANDSAT_WINDOW_DONE" -name "*.window" -type f | wc -l)
    local final_failed=$(find "$LANDSAT_WINDOW_FAILED" -name "*.window" -type f | wc -l)
    local round_elapsed=$(( $(date +%s) - start_monitor_time ))
    
    log INFO "$round_name completed in $(format_duration $round_elapsed)"
    log INFO "$round_name summary: Done: $final_done, Failed: $final_failed"
}

#######################################
# Processing Functions
#######################################
process_sentinel1() {
    log HEADER "Starting Sentinel-1 Processing (Original Logic)"
    
    mkdir -p "$S1_OUTPUT"
    
    # Generate partitions
    mapfile -t s1_partitions < <(calculate_partitions $S1_PARTITIONS)
    mapfile -t s1_workers_per_partition < <(calculate_workers_per_partition $S1_TOTAL_WORKERS $S1_PARTITIONS)
    
    log INFO "S1 Configuration:"
    log INFO "  - Partitions: $S1_PARTITIONS"
    log INFO "  - Total Workers: $S1_TOTAL_WORKERS"
    log INFO "  - Worker Memory: ${S1_WORKER_MEMORY}GB"
    log INFO "  - Orbit State: $S1_ORBIT_STATE"
    log INFO "  - Output: $S1_OUTPUT"
    
    # Start parallel processing
    local s1_pids=()
    local s1_partition_ids=()
    local s1_start_times=()
    local s1_completed=()
    local s1_failed=()
    
    for i in "${!s1_partitions[@]}"; do
        p_range=${s1_partitions[i]}
        p_start=${p_range%,*}
        p_end=${p_range#*,}
        workers=${s1_workers_per_partition[i]}
        partition_id=$(generate_partition_id "$p_start" "$p_end" "$i" "S1")
        
        s1_partition_ids+=("$partition_id")
        
        local overwrite_flag=""
        [[ "$S1_OVERWRITE" == "true" ]] && overwrite_flag="--overwrite"
        
        local debug_flag=""
        [[ "$DEBUG" == "true" ]] && debug_flag="--debug"
        
        $PYTHON_ENV s1_fast_processor.py \
            --input_tiff "$INPUT_TIFF" \
            --start_date "$p_start" \
            --end_date "$p_end" \
            --output "$S1_OUTPUT" \
            --orbit_state "$S1_ORBIT_STATE" \
            --dask_workers "$workers" \
            --worker_memory "$S1_WORKER_MEMORY" \
            --resolution "$S1_RESOLUTION" \
            --chunksize "$S1_CHUNKSIZE" \
            --min_coverage "$S1_MIN_COVERAGE" \
            --partition_id "$partition_id" \
            $overwrite_flag $debug_flag \
            > "$LOG_DIR/${partition_id}.log" 2>&1 &
        
        s1_pids+=($!)
        s1_start_times+=("$(date +%s)")
        
        # sleep 2
    done
    
    log INFO "S1: Launched ${#s1_pids[@]} partition processes"
    
    # Monitor processes
    monitor_processes s1_pids s1_partition_ids s1_start_times s1_completed s1_failed "S1"
    
    # Summarize results
    log INFO "S1 Processing Summary:"
    log INFO "  - Total: ${#s1_partitions[@]}"
    log INFO "  - Successful: ${#s1_completed[@]}"
    log INFO "  - Failed: ${#s1_failed[@]}"
    
    if [[ ${#s1_failed[@]} -gt 0 ]]; then
        log WARNING "S1 failed partitions: ${s1_failed[@]}"
        return 1
    fi
    
    return 0
}

process_sentinel2() {
    log HEADER "Starting Sentinel-2 Processing with Dynamic Windows and Retry"
    
    mkdir -p "$S2_OUTPUT"
    mkdir -p "$S2_WINDOW_PENDING" "$S2_WINDOW_PROCESSING" "$S2_WINDOW_DONE" "$S2_WINDOW_FAILED"
    
    log INFO "S2 Configuration:"
    log INFO "  - Max Processes: $S2_MAX_PROCESSES"
    log INFO "  - Window Size: $S2_WINDOW_DAYS days"
    log INFO "  - Window Timeout: $S2_WINDOW_TIMEOUT seconds"
    log INFO "  - Worker Memory: ${S2_WORKER_MEMORY}GB"
    log INFO "  - Max Cloud: ${S2_MAX_CLOUD}%"
    log INFO "  - Resolution: ${S2_RESOLUTION}m"
    log INFO "  - Retry Enabled: $S2_ENABLE_RETRY"
    log INFO "  - Output: $S2_OUTPUT"
    
    # Generate windows (only if pending directory is empty)
    local existing_windows=$(find "$S2_WINDOW_PENDING" -name "*.window" -type f | wc -l)
    if [ $existing_windows -eq 0 ]; then
        generate_s2_windows "$S2_WINDOW_DAYS" "$S2_WINDOW_PENDING"
    else
        log INFO "Found $existing_windows existing windows, skipping generation"
    fi
    
    # Count total windows
    local total_windows=$(find "$S2_WINDOW_PENDING" -name "*.window" -type f | wc -l)
    log INFO "Total S2 windows to process: $total_windows"
    
    if [ $total_windows -eq 0 ]; then
        log WARNING "No S2 windows to process"
        return 1
    fi
    
    # First round of processing
    run_s2_processing_round "First round"
    
    # Check for failed windows and retry if enabled
    local failed_count=$(find "$S2_WINDOW_FAILED" -name "*.window" -type f | wc -l)
    if [ $failed_count -gt 0 ] && [ "$S2_ENABLE_RETRY" == "true" ]; then
        log INFO "Found $failed_count failed windows, starting retry round..."
        
        # Move failed windows back to pending for retry
        find "$S2_WINDOW_FAILED" -name "*.window" -type f -exec mv {} "$S2_WINDOW_PENDING"/ \;
        
        # Second round (retry)
        run_s2_processing_round "Retry round"
    elif [ $failed_count -gt 0 ]; then
        log INFO "Found $failed_count failed windows, but retry is disabled"
    fi
    
    # Final summary
    local final_done=$(find "$S2_WINDOW_DONE" -name "*.window" -type f | wc -l)
    local final_failed=$(find "$S2_WINDOW_FAILED" -name "*.window" -type f | wc -l)
    
    log INFO "S2 Processing Final Summary:"
    log INFO "  - Total Windows: $total_windows"
    log INFO "  - Successful: $final_done"
    log INFO "  - Failed: $final_failed"
    
    if [ $final_failed -gt 0 ]; then
        log WARNING "S2: $final_failed windows failed after all attempts"
        # Return error if there are failed windows
        return 1
    fi
    
    # Return success only if all windows completed successfully
    return 0
}

#######################################
# Landsat Processing Function (NEW)
#######################################
process_landsat() {
    log HEADER "Starting Landsat 8/9 Processing with Dynamic Windows and Retry"
    
    mkdir -p "$LANDSAT_OUTPUT"
    mkdir -p "$LANDSAT_WINDOW_PENDING" "$LANDSAT_WINDOW_PROCESSING" "$LANDSAT_WINDOW_DONE" "$LANDSAT_WINDOW_FAILED"
    
    log INFO "Landsat Configuration:"
    log INFO "  - Max Processes: $LANDSAT_MAX_PROCESSES"
    log INFO "  - Window Size: $LANDSAT_WINDOW_DAYS days"
    log INFO "  - Window Timeout: $LANDSAT_WINDOW_TIMEOUT seconds"
    log INFO "  - Worker Memory: ${LANDSAT_WORKER_MEMORY}GB"
    log INFO "  - Max Cloud: ${LANDSAT_MAX_CLOUD}%"
    log INFO "  - Resolution: ${LANDSAT_RESOLUTION}m"
    log INFO "  - Retry Enabled: $LANDSAT_ENABLE_RETRY"
    log INFO "  - Output: $LANDSAT_OUTPUT"
    
    # Generate windows (only if pending directory is empty)
    local existing_windows=$(find "$LANDSAT_WINDOW_PENDING" -name "*.window" -type f | wc -l)
    if [ $existing_windows -eq 0 ]; then
        generate_landsat_windows "$LANDSAT_WINDOW_DAYS" "$LANDSAT_WINDOW_PENDING"
    else
        log INFO "Found $existing_windows existing windows, skipping generation"
    fi
    
    # Count total windows
    local total_windows=$(find "$LANDSAT_WINDOW_PENDING" -name "*.window" -type f | wc -l)
    log INFO "Total Landsat windows to process: $total_windows"
    
    if [ $total_windows -eq 0 ]; then
        log WARNING "No Landsat windows to process"
        return 0
    fi
    
    local s2_start_time=$(date +%s)
    
    # Initial processing round
    run_landsat_processing_round "Initial_Landsat_Round"
    
    # Handle retries if enabled
    if [ "$LANDSAT_ENABLE_RETRY" == "true" ]; then
        local retry_round=1
        
        while [ $retry_round -le 3 ]; do  # Maximum 3 retry rounds
            # Move failed windows back to pending for retry
            local failed_count=$(find "$LANDSAT_WINDOW_FAILED" -name "*.window" -type f | wc -l)
            
            if [ $failed_count -eq 0 ]; then
                log INFO "No failed Landsat windows to retry"
                break
            fi
            
            log INFO "Retry round $retry_round: Moving $failed_count failed windows back to pending"
            
            # Move failed windows back to pending (with retry limits)
            for failed_file in "$LANDSAT_WINDOW_FAILED"/*.window; do
                [[ ! -f "$failed_file" ]] && continue
                
                # Check retry count
                local retry_count=0
                if grep -q "retry_count=" "$failed_file"; then
                    retry_count=$(grep "retry_count=" "$failed_file" | cut -d'=' -f2)
                fi
                
                # Only retry if under limit (max 3 retries)
                if [ $retry_count -le 3 ]; then
                    local window_basename=$(basename "$failed_file")
                    local pending_file="$LANDSAT_WINDOW_PENDING/$window_basename"
                    
                    # Remove failure markers before moving back
                    grep -v -E "^(failed|error)=" "$failed_file" > "$pending_file"
                    rm "$failed_file"
                    
                    log INFO "Moved $(basename "$failed_file") to pending for retry (attempt $((retry_count + 1)))"
                else
                    log WARNING "$(basename "$failed_file") exceeded maximum retries ($retry_count), keeping in failed"
                fi
            done
            
            # Check if there are windows to retry
            local pending_retry_count=$(find "$LANDSAT_WINDOW_PENDING" -name "*.window" -type f | wc -l)
            if [ $pending_retry_count -eq 0 ]; then
                log INFO "No windows eligible for retry"
                break
            fi
            
            # Run retry round
            run_landsat_processing_round "Landsat_Retry_Round_${retry_round}"
            
            retry_round=$((retry_round + 1))
        done
    fi
    
    # Final summary
    local final_done=$(find "$LANDSAT_WINDOW_DONE" -name "*.window" -type f | wc -l)
    local final_failed=$(find "$LANDSAT_WINDOW_FAILED" -name "*.window" -type f | wc -l)
    local total_processed=$((final_done + final_failed))
    local s2_elapsed=$(( $(date +%s) - s2_start_time ))
    
    log HEADER "Landsat Processing Summary"
    log INFO "Total windows: $total_windows"
    log INFO "Successfully completed: $final_done"
    log INFO "Failed: $final_failed"
    log INFO "Success rate: $(( final_done * 100 / total_windows ))%"
    log INFO "Total processing time: $(format_duration $s2_elapsed)"
    
    # Return success only if all windows completed successfully
    return 0
}

#######################################
# Main Program
#######################################
main() {
    # Create necessary directories
    mkdir -p "$LOG_DIR" "$TEMP_DIR"
    
    # Initialize log
    echo "" > "$MAIN_LOG"
    
    log HEADER "TESSERA Processing Pipeline Started (S1 Original + S2 Dynamic Windows + Landsat Integration + Retry)"
    log INFO "Script: $SCRIPT_NAME"
    log INFO "Start time: $(date)"
    log INFO "Configuration:"
    log INFO "  - Input TIFF: $INPUT_TIFF"
    log INFO "  - Output Directory: $OUT_DIR"
    log INFO "  - Time Range: $START_TIME to $END_TIME"
    log INFO "  - S1 Enabled: $S1_ENABLED"
    log INFO "  - S2 Enabled: $S2_ENABLED"
    log INFO "  - Landsat Enabled: $LANDSAT_ENABLED"
    log INFO "  - Parallel Processing: $PARALLEL_PROCESSING"
    log INFO "  - S2 Window Size: $S2_WINDOW_DAYS days"
    log INFO "  - Landsat Window Size: $LANDSAT_WINDOW_DAYS days"
    
    # Check input file
    if [[ ! -f "$INPUT_TIFF" ]]; then
        log ERROR "Input TIFF file not found: $INPUT_TIFF"
        exit 1
    fi
    
    # Check Python environment
    if [[ ! -x "$PYTHON_ENV" ]]; then
        log ERROR "Python environment not found or not executable: $PYTHON_ENV"
        exit 1
    fi
    
    # Check Python scripts
    if [[ "$S1_ENABLED" == "true" && ! -f "s1_fast_processor.py" ]]; then
        log ERROR "S1 processor script not found: s1_fast_processor.py"
        exit 1
    fi
    
    if [[ "$S2_ENABLED" == "true" && ! -f "s2_fast_processor_test.py" ]]; then
        log ERROR "S2 processor script not found: s2_fast_processor_test.py"
        exit 1
    fi
    
    if [[ "$LANDSAT_ENABLED" == "true" && ! -f "landsat_fast_processor.py" ]]; then
        log ERROR "Landsat processor script not found: landsat_fast_processor.py"
        exit 1
    fi
    
    # Processing flags
    local s1_success=true
    local s2_success=true
    local landsat_success=true
    local s1_pid=""
    local s2_pid=""
    local landsat_pid=""
    
    # Processing logic based on parallel/sequential mode
    # Determine enabled processors
    local enabled_processors=()
    [[ "$S1_ENABLED" == "true" ]] && enabled_processors+=("S1")
    [[ "$S2_ENABLED" == "true" ]] && enabled_processors+=("S2")
    [[ "$LANDSAT_ENABLED" == "true" ]] && enabled_processors+=("LANDSAT")
    
    if [[ ${#enabled_processors[@]} -eq 0 ]]; then
        log ERROR "No processors are enabled! Please enable at least one of S1, S2, or Landsat processing."
        exit 1
    fi
    
    log INFO "Enabled processors: ${enabled_processors[*]}"
    
    if [[ "$PARALLEL_PROCESSING" == "true" ]]; then
        log INFO "Starting parallel processing of enabled satellites..."
        
        # Start enabled processors in the background
        if [[ "$S1_ENABLED" == "true" ]]; then
            log INFO "Starting Sentinel-1 processing in background..."
            ( process_sentinel1 ) &
            s1_pid=$!
        fi
        
        if [[ "$S2_ENABLED" == "true" ]]; then
            log INFO "Starting Sentinel-2 processing in background..."
            ( process_sentinel2 ) &
            s2_pid=$!
        fi
        
        if [[ "$LANDSAT_ENABLED" == "true" ]]; then
            log INFO "Starting Landsat processing in background..."
            ( process_landsat ) &
            landsat_pid=$!
        fi
        
        # Wait for all processes to complete
        log INFO "Waiting for all enabled processors to complete..."
        
        if [[ -n "$s1_pid" ]]; then
            log INFO "Waiting for Sentinel-1 processing (PID: $s1_pid)..."
            if ! wait $s1_pid; then
                s1_success=false
                log ERROR "Sentinel-1 processing failed"
            else
                log SUCCESS "Sentinel-1 processing completed successfully"
            fi
        fi
        
        if [[ -n "$s2_pid" ]]; then
            log INFO "Waiting for Sentinel-2 processing (PID: $s2_pid)..."
            if ! wait $s2_pid; then
                s2_success=false
                log ERROR "Sentinel-2 processing failed"
            else
                log SUCCESS "Sentinel-2 processing completed successfully"
            fi
        fi
        
        if [[ -n "$landsat_pid" ]]; then
            log INFO "Waiting for Landsat processing (PID: $landsat_pid)..."
            if ! wait $landsat_pid; then
                landsat_success=false
                log ERROR "Landsat processing failed"
            else
                log SUCCESS "Landsat processing completed successfully"
            fi
        fi
        
    else
        log INFO "Starting sequential processing of enabled satellites..."
        
        # Process sequentially in order: S1 -> S2 -> Landsat
        if [[ "$S1_ENABLED" == "true" ]]; then
            log INFO "Processing Sentinel-1..."
            if ! process_sentinel1; then
                s1_success=false
                log ERROR "Sentinel-1 processing failed"
            else
                log SUCCESS "Sentinel-1 processing completed successfully"
            fi
        fi
        
        if [[ "$S2_ENABLED" == "true" ]]; then
            if [[ "$S1_ENABLED" != "true" || "$s1_success" == "true" ]]; then
                log INFO "Processing Sentinel-2..."
                if ! process_sentinel2; then
                    s2_success=false
                    log ERROR "Sentinel-2 processing failed"
                else
                    log SUCCESS "Sentinel-2 processing completed successfully"
                fi
            else
                log WARNING "Skipping Sentinel-2 processing due to Sentinel-1 failure"
                s2_success=false
            fi
        fi
        
        if [[ "$LANDSAT_ENABLED" == "true" ]]; then
            # Only proceed with Landsat if no critical failures occurred
            local should_process_landsat=true
            if [[ "$S1_ENABLED" == "true" && "$s1_success" == "false" ]]; then
                should_process_landsat=false
            fi
            if [[ "$S2_ENABLED" == "true" && "$s2_success" == "false" ]]; then
                should_process_landsat=false
            fi
            
            if [[ "$should_process_landsat" == "true" ]]; then
                log INFO "Processing Landsat..."
                if ! process_landsat; then
                    landsat_success=false
                    log ERROR "Landsat processing failed"
                else
                    log SUCCESS "Landsat processing completed successfully"
                fi
            else
                log WARNING "Skipping Landsat processing due to previous failures"
                landsat_success=false
            fi
        fi
    fi
    
    # Check if processing was successful
    local processing_failed=false
    
    if [[ "$S1_ENABLED" == "true" && "$s1_success" == "false" ]]; then
        log ERROR "Sentinel-1 processing failed!"
        processing_failed=true
    fi
    
    if [[ "$S2_ENABLED" == "true" && "$s2_success" == "false" ]]; then
        log ERROR "Sentinel-2 processing failed!"
        processing_failed=true
    fi
    
    if [[ "$LANDSAT_ENABLED" == "true" && "$landsat_success" == "false" ]]; then
        log ERROR "Landsat processing failed!"
        processing_failed=true
    fi
    
    # If any processing failed, exit with error before proceeding to stacking
    if [ "$processing_failed" == "true" ]; then
        log ERROR "Data processing failed. Not proceeding to stacking phase."
        exit 1
    fi
    
    # Processing complete, now proceed to stacking
    log HEADER "Data Processing Complete - Starting Stacking Phase"
    
    local total_duration=$(($(date +%s) - SCRIPT_START_TIME))
    log INFO "Data processing time: $(format_duration $total_duration)"
    
    # Generate final report
    {
        echo ""
        echo "════════════════════════════════════════════════════════════════════"
        echo "     TESSERA PROCESSING REPORT (S1 Original + S2 Dynamic + Landsat)     "
        echo "════════════════════════════════════════════════════════════════════"
        echo ""
        echo "Processing Period: $START_TIME to $END_TIME"
        echo "Processing Mode: $([ "$PARALLEL_PROCESSING" == "true" ] && echo "Parallel" || echo "Sequential")"
        echo "Data Processing Duration: $(format_duration $total_duration)"
        echo ""
        
        if [[ "$S1_ENABLED" == "true" ]]; then
            echo "Sentinel-1 Processing (Original Logic):"
            echo "  Status: $([ "$s1_success" == "true" ] && echo "SUCCESS ✓" || echo "FAILED ✗")"
            echo "  Partitions: $S1_PARTITIONS"
            echo "  Output: $S1_OUTPUT"
            echo ""
        fi
        
        if [[ "$S2_ENABLED" == "true" ]]; then
            local s2_done=$(find "$S2_WINDOW_DONE" -name "*.window" -type f 2>/dev/null | wc -l)
            local s2_failed=$(find "$S2_WINDOW_FAILED" -name "*.window" -type f 2>/dev/null | wc -l)
            echo "Sentinel-2 Processing (Dynamic Windows):"
            echo "  Status: $([ "$s2_success" == "true" ] && echo "SUCCESS ✓" || echo "FAILED ✗")"
            echo "  Windows Completed: $s2_done"
            echo "  Windows Failed: $s2_failed"
            echo "  Window Size: $S2_WINDOW_DAYS days"
            echo "  Retry Enabled: $S2_ENABLE_RETRY"
            echo "  Output: $S2_OUTPUT"
            echo ""
        fi
        
        if [[ "$LANDSAT_ENABLED" == "true" ]]; then
            local landsat_done=$(find "$LANDSAT_WINDOW_DONE" -name "*.window" -type f 2>/dev/null | wc -l)
            local landsat_failed=$(find "$LANDSAT_WINDOW_FAILED" -name "*.window" -type f 2>/dev/null | wc -l)
            echo "Landsat 8/9 Processing (Dynamic Windows):"
            echo "  Status: $([ "$landsat_success" == "true" ] && echo "SUCCESS ✓" || echo "FAILED ✗")"
            echo "  Windows Completed: $landsat_done"
            echo "  Windows Failed: $landsat_failed"
            echo "  Window Size: $LANDSAT_WINDOW_DAYS days"
            echo "  Retry Enabled: $LANDSAT_ENABLE_RETRY"
            echo "  Max Cloud: $LANDSAT_MAX_CLOUD%"
            echo "  Output: $LANDSAT_OUTPUT"
            echo ""
        fi
        
        echo "Log Files:"
        echo "  Main Log: $MAIN_LOG"
        echo "  Detail Logs: $LOG_DIR/"
        echo ""
        echo "Proceeding to stacking phase..."
        echo "════════════════════════════════════════════════════════════════════"
    } | tee -a "$MAIN_LOG"
    
    # Combine logs
    if [[ "$S1_ENABLED" == "true" ]]; then
        cat "$LOG_DIR"/S1_*.log > "$LOG_DIR/s1_combined.log" 2>/dev/null || true
        log INFO "S1 combined log: $LOG_DIR/s1_combined.log"
    fi
    
    if [[ "$S2_ENABLED" == "true" ]]; then
        cat "$LOG_DIR"/S2_*.log > "$LOG_DIR/s2_combined.log" 2>/dev/null || true
        log INFO "S2 combined log: $LOG_DIR/s2_combined.log"
    fi
    
    # Proceed to stacking phase only if processing was successful
    log SUCCESS "Data processing phase completed successfully! Proceeding to stacking phase. 🎉"
    exit 0
}

# Cleanup function
cleanup() {
    log WARNING "Script interrupted. Cleaning up..."
    
    # Kill all background processes
    local all_pids=$(jobs -p)
    if [[ -n "$all_pids" ]]; then
        kill $all_pids 2>/dev/null || true
        sleep 2
        kill -9 $all_pids 2>/dev/null || true
    fi
    
    log WARNING "Cleanup completed. Exiting."
    exit 130
}

# Capture interrupt signal
trap cleanup INT TERM

# Run main program
main "$@"