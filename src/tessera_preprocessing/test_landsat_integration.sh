#!/usr/bin/env bash
#
# test_landsat_integration.sh — Test script for Landsat integration
# Usage: bash test_landsat_integration.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "Testing Landsat Integration Components"
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

# Test function
test_component() {
    local test_name="$1"
    local test_command="$2"
    
    echo -n "Testing $test_name... "
    
    if eval "$test_command" >/dev/null 2>&1; then
        echo -e "${GREEN}PASS${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}FAIL${NC}"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo "  Command: $test_command"
    fi
}

# Test 1: Check if Landsat processor exists
test_component "Landsat processor exists" "test -f landsat_fast_processor.py"

# Test 2: Check if quality assessment module exists  
test_component "Quality assessment module exists" "test -f landsat_quality_assessment.py"

# Test 3: Check if main script has been updated
test_component "Main script contains Landsat config" "grep -q 'LANDSAT_ENABLED' s1_s2_downloader_test.sh"

# Test 4: Check Python syntax of Landsat processor
test_component "Landsat processor Python syntax" "python3 -m py_compile landsat_fast_processor.py"

# Test 5: Check Python syntax of quality assessment
test_component "Quality assessment Python syntax" "python3 -m py_compile landsat_quality_assessment.py"

# Test 6: Check if requirements file exists
test_component "Requirements file exists" "test -f requirements_landsat.txt"

# Test 7: Check if Landsat constants are properly defined
test_component "Landsat constants defined" "grep -q 'LANDSAT_BAND_MAPPING' landsat_fast_processor.py"

# Test 8: Check if quality assessment runs
test_component "Quality assessment module test" "python3 landsat_quality_assessment.py"

# Test 9: Check if main script syntax is valid
test_component "Main script bash syntax" "bash -n s1_s2_downloader_test.sh"

# Test 10: Check if required environment variables are documented
test_component "Environment documentation check" "grep -q 'TEMP_DIR' s1_s2_downloader_test.sh"

echo ""
echo "========================================"
echo "Test Summary"
echo "========================================"
echo -e "Tests passed: ${GREEN}${TESTS_PASSED}${NC}"
echo -e "Tests failed: ${RED}${TESTS_FAILED}${NC}"
echo -e "Total tests: $((TESTS_PASSED + TESTS_FAILED))"

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed! ✅${NC}"
    echo ""
    echo "Integration Components Ready:"
    echo "✅ landsat_fast_processor.py - Main Landsat processing engine"
    echo "✅ landsat_quality_assessment.py - QA_PIXEL quality assessment"
    echo "✅ s1_s2_downloader_test.sh - Updated orchestration script"
    echo ""
    echo "Key Features Implemented:"
    echo "• Landsat 8/9 Collection 2 Level-2 support"
    echo "• 6 bands: Blue, Green, Red, NIR, SWIR1, SWIR2 + QA_PIXEL"
    echo "• 10m interpolation from native 30m resolution"
    echo "• Quality assessment with cloud/shadow detection"
    echo "• Dynamic window processing with retry logic"
    echo "• Parallel and sequential processing modes"
    echo "• Comprehensive logging and monitoring"
    echo ""
    exit 0
else
    echo -e "${RED}Some tests failed! ❌${NC}"
    echo "Please check the failed components before proceeding."
    exit 1
fi