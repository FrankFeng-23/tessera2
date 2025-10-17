#!/usr/bin/env python3
"""
landsat_quality_assessment.py — Landsat 8/9 QA_PIXEL Quality Assessment Utilities
Created: 2025-08-10
Provides comprehensive quality assessment tools for Landsat Collection 2 Level-2 QA_PIXEL band
Features: Bit flag parsing, cloud/shadow detection, statistical analysis
"""

import numpy as np
import logging
from typing import Dict, Tuple, Optional, List

# QA_PIXEL bit flag definitions for Landsat Collection 2
# Based on USGS Landsat Collection 2 Quality Assessment Band specification
QA_PIXEL_BIT_FLAGS = {
    0: {"name": "Fill", "description": "Fill pixel"},
    1: {"name": "Dilated Cloud", "description": "Dilated cloud pixel"},  
    2: {"name": "Cirrus", "description": "Cirrus cloud (high confidence)"},
    3: {"name": "Cloud", "description": "Cloud pixel"},
    4: {"name": "Cloud Shadow", "description": "Cloud shadow pixel"},
    5: {"name": "Snow", "description": "Snow pixel"},
    6: {"name": "Clear", "description": "Clear pixel"},
    7: {"name": "Water", "description": "Water pixel"},
    8: {"name": "Cloud Confidence Low", "description": "Cloud confidence low bit"},
    9: {"name": "Cloud Confidence High", "description": "Cloud confidence high bit"},
    10: {"name": "Cloud Shadow Confidence Low", "description": "Cloud shadow confidence low bit"},
    11: {"name": "Cloud Shadow Confidence High", "description": "Cloud shadow confidence high bit"},
    12: {"name": "Snow/Ice Confidence Low", "description": "Snow/ice confidence low bit"},
    13: {"name": "Snow/Ice Confidence High", "description": "Snow/ice confidence high bit"},
    14: {"name": "Cirrus Confidence Low", "description": "Cirrus confidence low bit"},
    15: {"name": "Cirrus Confidence High", "description": "Cirrus confidence high bit"}
}

# Invalid/problematic QA pixel categories (bits that indicate unusable pixels)
INVALID_QA_BITS = [0, 1, 2, 3, 4]  # Fill, Dilated Cloud, Cirrus, Cloud, Cloud Shadow

# Valid surface observation bits
VALID_QA_BITS = [5, 6, 7]  # Snow, Clear, Water (all can be valid observations)

# Confidence level mappings (2-bit combinations)
CONFIDENCE_LEVELS = {
    0: "Not Determined",  # 00
    1: "Low",            # 01  
    2: "Medium",         # 10
    3: "High"            # 11
}

def extract_bit_flag(qa_value: int, bit_position: int) -> bool:
    """Extract a single bit flag from QA_PIXEL value"""
    return bool((qa_value >> bit_position) & 1)

def extract_confidence_level(qa_value: int, low_bit: int, high_bit: int) -> int:
    """Extract 2-bit confidence level from QA_PIXEL value"""
    low = (qa_value >> low_bit) & 1
    high = (qa_value >> high_bit) & 1
    return (high << 1) | low

def parse_qa_pixel_flags(qa_value: int) -> Dict[str, bool]:
    """
    Parse all bit flags from a single QA_PIXEL value
    
    Args:
        qa_value: Integer QA_PIXEL value
        
    Returns:
        Dictionary with flag names as keys and boolean values
    """
    flags = {}
    
    # Extract basic bit flags (0-7)
    for bit_pos in range(8):
        if bit_pos in QA_PIXEL_BIT_FLAGS:
            flag_name = QA_PIXEL_BIT_FLAGS[bit_pos]["name"]
            flags[flag_name] = extract_bit_flag(qa_value, bit_pos)
    
    # Extract confidence levels (8-bit pairs)
    flags["Cloud_Confidence"] = extract_confidence_level(qa_value, 8, 9)
    flags["Cloud_Shadow_Confidence"] = extract_confidence_level(qa_value, 10, 11)
    flags["Snow_Ice_Confidence"] = extract_confidence_level(qa_value, 12, 13)
    flags["Cirrus_Confidence"] = extract_confidence_level(qa_value, 14, 15)
    
    return flags

def is_valid_observation(qa_value: int, 
                        exclude_water: bool = False,
                        exclude_snow: bool = False,
                        cloud_confidence_threshold: int = 1,
                        shadow_confidence_threshold: int = 1) -> bool:
    """
    Determine if QA_PIXEL value represents a valid surface observation
    
    Args:
        qa_value: Integer QA_PIXEL value
        exclude_water: Whether to exclude water pixels  
        exclude_snow: Whether to exclude snow pixels
        cloud_confidence_threshold: Minimum cloud confidence to exclude (0-3)
        shadow_confidence_threshold: Minimum shadow confidence to exclude (0-3)
        
    Returns:
        Boolean indicating if pixel is valid observation
    """
    # Check for invalid bits (fill, dilated cloud, cirrus, cloud, cloud shadow)
    for bit in INVALID_QA_BITS:
        if extract_bit_flag(qa_value, bit):
            return False
    
    # Check confidence levels
    cloud_conf = extract_confidence_level(qa_value, 8, 9)
    shadow_conf = extract_confidence_level(qa_value, 10, 11)
    
    if cloud_conf >= cloud_confidence_threshold:
        return False
        
    if shadow_conf >= shadow_confidence_threshold:
        return False
    
    # Check for water exclusion
    if exclude_water and extract_bit_flag(qa_value, 7):  # Water bit
        return False
        
    # Check for snow exclusion  
    if exclude_snow and extract_bit_flag(qa_value, 5):  # Snow bit
        return False
        
    return True

def analyze_qa_statistics(qa_array: np.ndarray, 
                         roi_mask: Optional[np.ndarray] = None,
                         partition_id: str = "unknown") -> Dict:
    """
    Perform comprehensive statistical analysis of QA_PIXEL array
    
    Args:
        qa_array: 2D numpy array of QA_PIXEL values
        roi_mask: Optional ROI mask to limit analysis area
        partition_id: Identifier for logging
        
    Returns:
        Dictionary with comprehensive QA statistics
    """
    logging.info(f"[{partition_id}] Analyzing QA statistics for array shape {qa_array.shape}")
    
    # Apply ROI mask if provided
    if roi_mask is not None:
        # Ensure shapes match
        if qa_array.shape != roi_mask.shape:
            min_height = min(qa_array.shape[0], roi_mask.shape[0])
            min_width = min(qa_array.shape[1], roi_mask.shape[1])
            qa_subset = qa_array[:min_height, :min_width]
            mask_subset = roi_mask[:min_height, :min_width]
            analysis_pixels = qa_subset[mask_subset > 0]
        else:
            analysis_pixels = qa_array[roi_mask > 0]
    else:
        analysis_pixels = qa_array.flatten()
    
    total_pixels = len(analysis_pixels)
    logging.info(f"[{partition_id}] Analyzing {total_pixels} pixels")
    
    if total_pixels == 0:
        logging.warning(f"[{partition_id}] No pixels to analyze")
        return {"total_pixels": 0}
    
    # Get unique values and counts
    unique_values, counts = np.unique(analysis_pixels, return_counts=True)
    value_stats = dict(zip(unique_values, counts))
    
    # Initialize statistics dictionary
    stats = {
        "total_pixels": total_pixels,
        "unique_values": len(unique_values),
        "value_distribution": value_stats,
        "bit_flag_statistics": {},
        "confidence_statistics": {},
        "validity_statistics": {}
    }
    
    # Analyze bit flags
    bit_flag_counts = {flag_info["name"]: 0 for flag_info in QA_PIXEL_BIT_FLAGS.values()}
    
    # Analyze confidence levels
    confidence_counts = {
        "Cloud_Confidence": {level: 0 for level in CONFIDENCE_LEVELS.values()},
        "Cloud_Shadow_Confidence": {level: 0 for level in CONFIDENCE_LEVELS.values()},
        "Snow_Ice_Confidence": {level: 0 for level in CONFIDENCE_LEVELS.values()},
        "Cirrus_Confidence": {level: 0 for level in CONFIDENCE_LEVELS.values()}
    }
    
    # Count validity categories
    valid_counts = {
        "valid_clear": 0,
        "valid_water": 0, 
        "valid_snow": 0,
        "invalid_cloud": 0,
        "invalid_shadow": 0,
        "invalid_fill": 0,
        "invalid_other": 0
    }
    
    # Process each pixel value
    for qa_val, count in value_stats.items():
        # Parse flags for this value
        flags = parse_qa_pixel_flags(qa_val)
        
        # Count bit flags
        for bit_pos in range(8):
            if bit_pos in QA_PIXEL_BIT_FLAGS:
                flag_name = QA_PIXEL_BIT_FLAGS[bit_pos]["name"] 
                if flags[flag_name]:
                    bit_flag_counts[flag_name] += count
        
        # Count confidence levels
        for conf_type in ["Cloud_Confidence", "Cloud_Shadow_Confidence", 
                         "Snow_Ice_Confidence", "Cirrus_Confidence"]:
            conf_level = flags[conf_type]
            conf_name = CONFIDENCE_LEVELS[conf_level]
            confidence_counts[conf_type][conf_name] += count
        
        # Categorize validity
        if flags["Fill"]:
            valid_counts["invalid_fill"] += count
        elif flags["Cloud"] or flags["Dilated Cloud"]:
            valid_counts["invalid_cloud"] += count
        elif flags["Cloud Shadow"]:
            valid_counts["invalid_shadow"] += count
        elif flags["Clear"]:
            valid_counts["valid_clear"] += count
        elif flags["Water"]:
            valid_counts["valid_water"] += count
        elif flags["Snow"]:
            valid_counts["valid_snow"] += count
        else:
            valid_counts["invalid_other"] += count
    
    # Store results
    stats["bit_flag_statistics"] = bit_flag_counts
    stats["confidence_statistics"] = confidence_counts
    stats["validity_statistics"] = valid_counts
    
    # Calculate percentages
    stats["validity_percentages"] = {
        key: 100.0 * count / total_pixels for key, count in valid_counts.items()
    }
    
    # Calculate overall validity
    total_valid = (valid_counts["valid_clear"] + 
                  valid_counts["valid_water"] + 
                  valid_counts["valid_snow"])
    stats["overall_valid_percentage"] = 100.0 * total_valid / total_pixels
    
    return stats

def create_validity_mask(qa_array: np.ndarray,
                        exclude_water: bool = False,
                        exclude_snow: bool = False, 
                        cloud_confidence_threshold: int = 1,
                        shadow_confidence_threshold: int = 1) -> np.ndarray:
    """
    Create boolean validity mask from QA_PIXEL array
    
    Args:
        qa_array: 2D numpy array of QA_PIXEL values
        exclude_water: Whether to exclude water pixels
        exclude_snow: Whether to exclude snow pixels  
        cloud_confidence_threshold: Minimum cloud confidence to exclude (0-3)
        shadow_confidence_threshold: Minimum shadow confidence to exclude (0-3)
        
    Returns:
        Boolean numpy array with same shape as qa_array
    """
    # Vectorized validity check
    vectorized_validity = np.vectorize(
        lambda x: is_valid_observation(
            x, exclude_water, exclude_snow, 
            cloud_confidence_threshold, shadow_confidence_threshold
        )
    )
    
    return vectorized_validity(qa_array)

def log_qa_summary(stats: Dict, partition_id: str = "unknown"):
    """
    Log comprehensive QA statistics summary
    
    Args:
        stats: Statistics dictionary from analyze_qa_statistics
        partition_id: Identifier for logging
    """
    logging.info(f"[{partition_id}] QA Analysis Summary:")
    logging.info(f"[{partition_id}]   Total pixels: {stats['total_pixels']:,}")
    logging.info(f"[{partition_id}]   Unique QA values: {stats['unique_values']}")
    logging.info(f"[{partition_id}]   Overall valid coverage: {stats['overall_valid_percentage']:.2f}%")
    
    # Log validity breakdown
    validity_stats = stats['validity_percentages']
    logging.info(f"[{partition_id}]   Validity breakdown:")
    logging.info(f"[{partition_id}]     Clear: {validity_stats['valid_clear']:.2f}%")
    logging.info(f"[{partition_id}]     Water: {validity_stats['valid_water']:.2f}%")  
    logging.info(f"[{partition_id}]     Snow: {validity_stats['valid_snow']:.2f}%")
    logging.info(f"[{partition_id}]     Cloud: {validity_stats['invalid_cloud']:.2f}%")
    logging.info(f"[{partition_id}]     Shadow: {validity_stats['invalid_shadow']:.2f}%")
    logging.info(f"[{partition_id}]     Fill: {validity_stats['invalid_fill']:.2f}%")
    
    # Log top 5 most common QA values
    value_dist = stats['value_distribution']
    sorted_values = sorted(value_dist.items(), key=lambda x: x[1], reverse=True)[:5]
    logging.info(f"[{partition_id}]   Most common QA values:")
    for qa_val, count in sorted_values:
        percentage = 100.0 * count / stats['total_pixels']
        flags = parse_qa_pixel_flags(qa_val)
        flag_desc = ", ".join([name for name, flag in flags.items() 
                              if isinstance(flag, bool) and flag])
        logging.info(f"[{partition_id}]     QA={qa_val}: {count:,} pixels ({percentage:.2f}%) - {flag_desc}")

def get_recommended_validity_thresholds(stats: Dict) -> Dict[str, int]:
    """
    Analyze QA statistics and recommend optimal validity thresholds
    
    Args:
        stats: Statistics dictionary from analyze_qa_statistics
        
    Returns:
        Dictionary with recommended threshold values
    """
    recommendations = {
        "cloud_confidence_threshold": 1,  # Default: exclude medium+ confidence clouds
        "shadow_confidence_threshold": 1,  # Default: exclude medium+ confidence shadows
        "exclude_water": False,  # Default: include water pixels
        "exclude_snow": False   # Default: include snow pixels
    }
    
    # Analyze validity percentages to adjust recommendations
    validity_pct = stats.get('validity_percentages', {})
    
    # If too much cloud contamination, be more strict
    if validity_pct.get('invalid_cloud', 0) > 30:
        recommendations["cloud_confidence_threshold"] = 0  # Exclude even low confidence clouds
        
    # If too much shadow contamination, be more strict  
    if validity_pct.get('invalid_shadow', 0) > 20:
        recommendations["shadow_confidence_threshold"] = 0  # Exclude even low confidence shadows
        
    # If water dominates and we want land observations, consider excluding
    if validity_pct.get('valid_water', 0) > 50:
        recommendations["exclude_water"] = True
        
    # If snow dominates and we want non-snow observations, consider excluding
    if validity_pct.get('valid_snow', 0) > 40:
        recommendations["exclude_snow"] = True
        
    return recommendations

def quality_assessment_pipeline(qa_array: np.ndarray,
                               roi_mask: Optional[np.ndarray] = None,
                               partition_id: str = "unknown") -> Tuple[np.ndarray, Dict, float]:
    """
    Complete quality assessment pipeline for Landsat QA_PIXEL data
    
    Args:
        qa_array: 2D numpy array of QA_PIXEL values
        roi_mask: Optional ROI mask to limit analysis area
        partition_id: Identifier for logging
        
    Returns:
        Tuple of (validity_mask, statistics, valid_coverage_percentage)
    """
    logging.info(f"[{partition_id}] Starting Landsat QA quality assessment pipeline")
    
    # Perform comprehensive statistical analysis
    stats = analyze_qa_statistics(qa_array, roi_mask, partition_id)
    
    # Log summary
    log_qa_summary(stats, partition_id)
    
    # Get recommended thresholds
    recommendations = get_recommended_validity_thresholds(stats)
    logging.info(f"[{partition_id}] Recommended validity thresholds: {recommendations}")
    
    # Create validity mask using recommended thresholds
    validity_mask = create_validity_mask(
        qa_array,
        exclude_water=recommendations["exclude_water"],
        exclude_snow=recommendations["exclude_snow"],
        cloud_confidence_threshold=recommendations["cloud_confidence_threshold"],
        shadow_confidence_threshold=recommendations["shadow_confidence_threshold"]
    )
    
    # Calculate final valid coverage
    if roi_mask is not None:
        # Ensure shapes match for coverage calculation
        if validity_mask.shape != roi_mask.shape:
            min_height = min(validity_mask.shape[0], roi_mask.shape[0])
            min_width = min(validity_mask.shape[1], roi_mask.shape[1])
            validity_subset = validity_mask[:min_height, :min_width]
            roi_subset = roi_mask[:min_height, :min_width]
            valid_pixels = np.sum(validity_subset & (roi_subset > 0))
            total_roi_pixels = np.sum(roi_subset > 0)
        else:
            valid_pixels = np.sum(validity_mask & (roi_mask > 0))
            total_roi_pixels = np.sum(roi_mask > 0)
    else:
        valid_pixels = np.sum(validity_mask)
        total_roi_pixels = validity_mask.size
    
    valid_coverage_pct = 100.0 * valid_pixels / total_roi_pixels if total_roi_pixels > 0 else 0.0
    
    logging.info(f"[{partition_id}] Final valid coverage: {valid_coverage_pct:.2f}% "
                f"({valid_pixels:,}/{total_roi_pixels:,} pixels)")
    
    return validity_mask, stats, valid_coverage_pct

if __name__ == "__main__":
    # Example usage and testing
    import sys
    
    # Create synthetic test data
    np.random.seed(42)
    test_qa = np.random.randint(0, 65536, size=(100, 100), dtype=np.uint16)
    test_roi = np.ones((100, 100), dtype=np.uint8)
    
    # Run pipeline
    validity_mask, statistics, coverage = quality_assessment_pipeline(
        test_qa, test_roi, "test"
    )
    
    print(f"Test completed. Valid coverage: {coverage:.2f}%")
    print(f"Validity mask shape: {validity_mask.shape}")
    print(f"Statistics keys: {list(statistics.keys())}")