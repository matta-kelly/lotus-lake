"""
Type conversion functions for data pipeline preprocessing.

This module provides reusable converters that transform raw API data into
types compatible with PyArrow schemas. Following the adapter pattern used by
Airbyte, Fivetran, and other professional data platforms.

Usage:
    from resources.load import schema_converters
    
    FIELD_CONVERTERS = {
        'created_at': schema_converters.convert_timestamp,
        'amount': schema_converters.convert_decimal,
    }
"""

import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Union, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================================
# TIMESTAMP CONVERTERS
# ============================================================================

def convert_timestamp(value: Any) -> Optional[datetime]:
    """
    Convert ISO 8601 timestamp strings to timezone-aware datetime objects.
    
    Handles:
        - ISO 8601 strings with Z suffix: "2025-10-14T12:00:00Z"
        - ISO 8601 strings with timezone: "2025-10-14T12:00:00+00:00"
        - None/null values (returns None)
        - Invalid timestamps (logs warning, returns None)
    
    Args:
        value: Timestamp string or None
        
    Returns:
        datetime object with UTC timezone, or None if invalid/null
        
    Examples:
        >>> convert_timestamp("2025-10-14T12:00:00Z")
        datetime.datetime(2025, 10, 14, 12, 0, tzinfo=datetime.timezone.utc)
        
        >>> convert_timestamp(None)
        None
        
        >>> convert_timestamp("invalid")
        None  # Logs warning
    """
    if value is None or pd.isna(value):
        return None
    
    if isinstance(value, datetime):
        # Already a datetime, ensure it's UTC
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    
    if not isinstance(value, str):
        logger.warning(f"Expected string or datetime for timestamp, got {type(value)}: {value}")
        return None
    
    try:
        # pandas handles ISO 8601 parsing including timezone variants
        dt = pd.to_datetime(value, utc=True)
        return dt.to_pydatetime()
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse timestamp '{value}': {e}")
        return None


def convert_nested_timestamps(obj: Any) -> Any:
    """
    Recursively convert timestamp strings in nested structures.
    
    Traverses nested dicts and lists, converting any ISO 8601 timestamp
    strings to datetime objects while preserving structure.
    
    Use this for complex nested fields that may contain timestamps at
    various depths (e.g., nested event data, metadata objects).
    
    Args:
        obj: Nested structure (dict, list, or primitive)
        
    Returns:
        Same structure with timestamp strings converted to datetime objects
        
    Examples:
        >>> data = {
        ...     "events": [
        ...         {"timestamp": "2025-10-14T12:00:00Z", "type": "click"},
        ...         {"timestamp": "2025-10-14T13:00:00Z", "type": "view"}
        ...     ]
        ... }
        >>> convert_nested_timestamps(data)
        {
            "events": [
                {"timestamp": datetime(...), "type": "click"},
                {"timestamp": datetime(...), "type": "view"}
            ]
        }
    """
    if isinstance(obj, dict):
        return {key: convert_nested_timestamps(value) for key, value in obj.items()}
    
    elif isinstance(obj, list):
        return [convert_nested_timestamps(item) for item in obj]
    
    elif isinstance(obj, str):
        # Check if string looks like ISO timestamp
        if "T" in obj and ("Z" in obj or "+" in obj or "-" in obj[-6:]):
            converted = convert_timestamp(obj)
            # Return original if conversion failed (might not be a timestamp)
            return converted if converted is not None else obj
        return obj
    
    else:
        # Primitives (int, float, bool, None) pass through unchanged
        return obj


# ============================================================================
# NUMERIC CONVERTERS
# ============================================================================

def convert_decimal(value: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
    """
    Convert string or numeric values to Decimal for precise money calculations.
    
    Handles:
        - String representations: "123.45"
        - Integer values: 123
        - Float values: 123.45 (with precision warning)
        - None/null values (returns default)
        - Invalid values (logs warning, returns default)
    
    Args:
        value: Numeric value as string, int, float, or None
        default: Value to return if conversion fails (default: None)
        
    Returns:
        Decimal object, or default if invalid/null
        
    Examples:
        >>> convert_decimal("123.45")
        Decimal('123.45')
        
        >>> convert_decimal(None, default=Decimal('0.00'))
        Decimal('0.00')
        
        >>> convert_decimal("invalid")
        None  # Logs warning
    """
    if value is None or pd.isna(value):
        return default
    
    if isinstance(value, Decimal):
        return value
    
    if isinstance(value, float):
        logger.debug(f"Converting float to Decimal may lose precision: {value}")
    
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as e:
        logger.warning(f"Failed to convert '{value}' to Decimal: {e}")
        return default


def convert_integer(value: Any, default: Optional[int] = None) -> Optional[int]:
    """
    Convert string or numeric values to integer.
    
    Handles:
        - String representations: "123"
        - Float values: 123.0 (converts if no decimal part)
        - None/null values (returns default)
        - Invalid values (logs warning, returns default)
    
    Args:
        value: Numeric value as string, int, float, or None
        default: Value to return if conversion fails (default: None)
        
    Returns:
        Integer value, or default if invalid/null
        
    Examples:
        >>> convert_integer("123")
        123
        
        >>> convert_integer(123.0)
        123
        
        >>> convert_integer(123.7)
        None  # Logs warning (has decimal part)
    """
    if value is None or pd.isna(value):
        return default
    
    if isinstance(value, int):
        return value
    
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        else:
            logger.warning(f"Float value {value} has decimal part, cannot convert to int safely")
            return default
    
    try:
        return int(value)
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to convert '{value}' to integer: {e}")
        return default


# ============================================================================
# STRING CONVERTERS
# ============================================================================

def convert_string(value: Any, default: str = "") -> str:
    """
    Convert any value to string, with null handling.
    
    Args:
        value: Any value
        default: Value to return for None/null (default: empty string)
        
    Returns:
        String representation, or default if null
    """
    if value is None or pd.isna(value):
        return default
    return str(value)


def convert_boolean(value: Any, default: Optional[bool] = None) -> Optional[bool]:
    """
    Convert string or numeric values to boolean.
    
    Handles common boolean representations:
        - Strings: "true", "false", "yes", "no", "1", "0" (case-insensitive)
        - Integers: 1 (True), 0 (False)
        - Booleans: Pass through
        - None/null: Returns default
    
    Args:
        value: Boolean representation or None
        default: Value to return if conversion fails (default: None)
        
    Returns:
        Boolean value, or default if invalid/null
    """
    if value is None or pd.isna(value):
        return default
    
    if isinstance(value, bool):
        return value
    
    if isinstance(value, (int, float)):
        return bool(value)
    
    if isinstance(value, str):
        lower_value = value.lower().strip()
        if lower_value in ("true", "yes", "1", "t", "y"):
            return True
        elif lower_value in ("false", "no", "0", "f", "n"):
            return False
    
    logger.warning(f"Cannot convert '{value}' to boolean")
    return default


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def apply_converter_to_nested_field(
    obj: Dict[str, Any],
    field_path: str,
    converter: callable
) -> Dict[str, Any]:
    """
    Apply a converter to a nested field specified by dot notation.
    
    Useful for converting deeply nested fields without writing custom logic.
    
    Args:
        obj: Dictionary to process
        field_path: Dot-separated path to field (e.g., "order.customer.created_at")
        converter: Converter function to apply
        
    Returns:
        Modified dictionary with converter applied to specified field
        
    Examples:
        >>> data = {"order": {"customer": {"created_at": "2025-10-14T12:00:00Z"}}}
        >>> apply_converter_to_nested_field(data, "order.customer.created_at", convert_timestamp)
        {"order": {"customer": {"created_at": datetime(...)}}}
    """
    keys = field_path.split(".")
    current = obj
    
    # Navigate to parent of target field
    for key in keys[:-1]:
        if key not in current:
            return obj  # Path doesn't exist, return unchanged
        current = current[key]
        if not isinstance(current, dict):
            return obj  # Can't navigate further, return unchanged
    
    # Apply converter to target field
    final_key = keys[-1]
    if final_key in current:
        current[final_key] = converter(current[final_key])
    
    return obj


# ============================================================================
# VALIDATION HELPERS
# ============================================================================

def validate_timestamp_field(df, field_name: str) -> Dict[str, Any]:
    """
    Validate timestamp field in DataFrame and return statistics.
    
    Useful for data quality checks after conversion.
    
    Args:
        df: pandas DataFrame
        field_name: Name of timestamp field to validate
        
    Returns:
        Dictionary with validation statistics:
            - total_rows: Total number of rows
            - null_count: Number of null values
            - valid_count: Number of valid timestamps
            - min_timestamp: Earliest timestamp
            - max_timestamp: Latest timestamp
    """
    if field_name not in df.columns:
        return {"error": f"Field '{field_name}' not found in DataFrame"}
    
    col = df[field_name]
    null_count = col.isna().sum()
    valid_count = (~col.isna()).sum()
    
    stats = {
        "total_rows": len(df),
        "null_count": int(null_count),
        "null_percentage": float(null_count / len(df) * 100),
        "valid_count": int(valid_count),
    }
    
    if valid_count > 0:
        stats["min_timestamp"] = col.min()
        stats["max_timestamp"] = col.max()
    
    return stats