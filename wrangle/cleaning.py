"""Data cleaning functions for the Data Wrangler app."""

import re
import chardet
import pandas as pd
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def detect_encoding(file_path: str, sample_size: int = 10000) -> str:
    """Detect file encoding using chardet.
    
    Args:
        file_path: Path to the file
        sample_size: Number of bytes to sample for detection
        
    Returns:
        Detected encoding string
    """
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0)
            
            # Fallback to utf-8 if confidence is too low
            if confidence < 0.7:
                logger.warning(f"Low encoding confidence ({confidence:.2f}) for {file_path}, using utf-8")
                return 'utf-8'
            
            return encoding
    except Exception as e:
        logger.error(f"Error detecting encoding for {file_path}: {e}")
        return 'utf-8'


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Convert column names to snake_case.
    
    Args:
        df: DataFrame to standardize
        
    Returns:
        DataFrame with standardized column names
    """
    def to_snake_case(name: str) -> str:
        # Remove special characters and replace with underscores
        name = re.sub(r'[^\w\s]', '_', str(name))
        # Convert to lowercase and replace spaces with underscores
        name = re.sub(r'\s+', '_', name.lower())
        # Remove multiple underscores
        name = re.sub(r'_+', '_', name)
        # Remove leading/trailing underscores
        return name.strip('_')
    
    df.columns = [to_snake_case(col) for col in df.columns]
    return df


def infer_dtypes(df: pd.DataFrame) -> Dict[str, str]:
    """Infer optimal data types for DataFrame columns.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary mapping column names to suggested dtypes
    """
    dtype_map = {}
    
    for col in df.columns:
        if df[col].dtype == 'object':
            # Try to convert to numeric first
            try:
                pd.to_numeric(df[col], errors='raise')
                dtype_map[col] = 'float64'
                continue
            except (ValueError, TypeError):
                pass
            
            # Try to convert to datetime
            try:
                pd.to_datetime(df[col], errors='raise')
                dtype_map[col] = 'datetime64[ns]'
                continue
            except (ValueError, TypeError):
                pass
            
            # Check for boolean-like values
            unique_vals = df[col].dropna().unique()
            if len(unique_vals) <= 2:
                bool_vals = set(str(v).lower() for v in unique_vals)
                if bool_vals.issubset({'true', 'false', '1', '0', 'yes', 'no', 'y', 'n', 't', 'f'}):
                    dtype_map[col] = 'bool'
                    continue
            
            # Default to string
            dtype_map[col] = 'object'
        else:
            dtype_map[col] = str(df[col].dtype)
    
    return dtype_map


def coerce_dtypes(df: pd.DataFrame, dtype_map: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Coerce DataFrame columns to specified data types.
    
    Args:
        df: DataFrame to coerce
        dtype_map: Dictionary mapping column names to dtypes
        
    Returns:
        DataFrame with coerced dtypes
    """
    if dtype_map is None:
        dtype_map = infer_dtypes(df)
    
    df_clean = df.copy()
    
    for col, dtype in dtype_map.items():
        if col in df_clean.columns:
            try:
                if dtype == 'bool':
                    # Handle boolean conversion
                    df_clean[col] = df_clean[col].astype(str).str.lower().map({
                        'true': True, 'false': False, '1': True, '0': False,
                        'yes': True, 'no': False, 'y': True, 'n': False,
                        't': True, 'f': False
                    })
                elif dtype == 'datetime64[ns]':
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                elif dtype in ['int64', 'float64']:
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                else:
                    df_clean[col] = df_clean[col].astype(dtype)
            except Exception as e:
                logger.warning(f"Could not coerce column {col} to {dtype}: {e}")
    
    return df_clean


def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace from string columns.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with trimmed strings
    """
    df_clean = df.copy()
    
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].astype(str).str.strip()
    
    return df_clean


def handle_na_values(df: pd.DataFrame, policy: str = 'keep', fill_value: Optional[Union[str, int, float]] = None) -> pd.DataFrame:
    """Handle NA values according to specified policy.
    
    Args:
        df: DataFrame to clean
        policy: 'drop', 'keep', or 'fill'
        fill_value: Value to fill NAs with (required if policy is 'fill')
        
    Returns:
        DataFrame with NA handling applied
    """
    df_clean = df.copy()
    
    if policy == 'drop':
        df_clean = df_clean.dropna()
    elif policy == 'fill' and fill_value is not None:
        df_clean = df_clean.fillna(fill_value)
    # 'keep' policy requires no action
    
    return df_clean


def enforce_date_format(df: pd.DataFrame, date_columns: Optional[List[str]] = None, 
                       target_format: str = '%Y-%m-%d') -> pd.DataFrame:
    """Enforce consistent date format for specified columns.
    
    Args:
        df: DataFrame to clean
        date_columns: List of column names to format as dates
        target_format: Target date format string
        
    Returns:
        DataFrame with formatted dates
    """
    df_clean = df.copy()
    
    if date_columns is None:
        # Auto-detect date columns
        date_columns = []
        for col in df_clean.columns:
            if df_clean[col].dtype == 'datetime64[ns]':
                date_columns.append(col)
    
    for col in date_columns:
        if col in df_clean.columns:
            try:
                # Convert to datetime if not already
                if df_clean[col].dtype != 'datetime64[ns]':
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                
                # Format as string in target format
                df_clean[col] = df_clean[col].dt.strftime(target_format)
            except Exception as e:
                logger.warning(f"Could not format date column {col}: {e}")
    
    return df_clean


def standardize_datetime_format(df: pd.DataFrame, datetime_columns: Optional[List[str]] = None,
                               target_format: str = '%Y-%m-%d %H:%M:%S') -> pd.DataFrame:
    """Standardize datetime columns to a consistent format with time components.
    
    Args:
        df: DataFrame to clean
        datetime_columns: List of column names to format as datetime
        target_format: Target datetime format string (default: YYYY-MM-DD HH:MM:SS)
        
    Returns:
        DataFrame with standardized datetime columns
    """
    df_clean = df.copy()
    
    if datetime_columns is None:
        # Auto-detect datetime columns
        datetime_columns = []
        for col in df_clean.columns:
            if df_clean[col].dtype == 'datetime64[ns]':
                datetime_columns.append(col)
    
    for col in datetime_columns:
        if col in df_clean.columns:
            try:
                # Convert to datetime if not already
                if df_clean[col].dtype != 'datetime64[ns]':
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                
                # Format as string in target format
                df_clean[col] = df_clean[col].dt.strftime(target_format)
            except Exception as e:
                logger.warning(f"Could not standardize datetime column {col}: {e}")
    
    return df_clean


def detect_datetime_columns(df: pd.DataFrame, sample_size: int = 100) -> List[str]:
    """Detect columns that contain datetime data.
    
    Args:
        df: DataFrame to analyze
        sample_size: Number of rows to sample for detection
        
    Returns:
        List of column names that appear to contain datetime data
    """
    datetime_columns = []
    
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            datetime_columns.append(col)
            continue
        
        if df[col].dtype == 'object':
            # Sample data for analysis
            sample_data = df[col].dropna().head(sample_size)
            
            if len(sample_data) == 0:
                continue
            
            # Check for common datetime patterns
            datetime_patterns = [
                r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
                r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
                r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
                r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
                r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}',  # YYYY-MM-DD HH:MM
                r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',  # MM/DD/YYYY HH:MM:SS
                r'\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2}',  # MM-DD-YYYY HH:MM:SS
            ]
            
            # Check if any pattern matches
            pattern_matches = 0
            for pattern in datetime_patterns:
                matches = sample_data.astype(str).str.match(pattern, na=False).sum()
                if matches > 0:
                    pattern_matches += matches
            
            # If more than 50% of samples match datetime patterns, consider it a datetime column
            if pattern_matches > len(sample_data) * 0.5:
                datetime_columns.append(col)
    
    return datetime_columns


def remove_duplicates(df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
    """Remove duplicate rows from DataFrame.
    
    Args:
        df: DataFrame to clean
        subset: Columns to consider for duplicate detection
        
    Returns:
        DataFrame with duplicates removed
    """
    return df.drop_duplicates(subset=subset)


def clean_dataframe(df: pd.DataFrame, 
                   standardize_cols: bool = True,
                   infer_dtypes_flag: bool = True,
                   trim_whitespace_flag: bool = True,
                   na_policy: str = 'keep',
                   na_fill_value: Optional[Union[str, int, float]] = None,
                   date_columns: Optional[List[str]] = None,
                   datetime_columns: Optional[List[str]] = None,
                   datetime_format: str = '%Y-%m-%d %H:%M:%S',
                   auto_detect_datetime: bool = True,
                   remove_duplicates_flag: bool = True,
                   duplicate_subset: Optional[List[str]] = None) -> pd.DataFrame:
    """Apply comprehensive data cleaning to a DataFrame.
    
    Args:
        df: DataFrame to clean
        standardize_cols: Whether to standardize column names
        infer_dtypes_flag: Whether to infer and coerce data types
        trim_whitespace_flag: Whether to trim whitespace
        na_policy: How to handle NA values ('drop', 'keep', 'fill')
        na_fill_value: Value to fill NAs with (if na_policy is 'fill')
        date_columns: List of columns to format as dates (YYYY-MM-DD)
        datetime_columns: List of columns to format as datetime (YYYY-MM-DD HH:MM:SS)
        datetime_format: Target datetime format string
        auto_detect_datetime: Whether to auto-detect datetime columns
        remove_duplicates_flag: Whether to remove duplicates
        duplicate_subset: Columns to consider for duplicate detection
        
    Returns:
        Cleaned DataFrame
    """
    df_clean = df.copy()
    
    if standardize_cols:
        df_clean = standardize_column_names(df_clean)
    
    if trim_whitespace_flag:
        df_clean = trim_whitespace(df_clean)
    
    if infer_dtypes_flag:
        dtype_map = infer_dtypes(df_clean)
        df_clean = coerce_dtypes(df_clean, dtype_map)
    
    if na_policy != 'keep':
        df_clean = handle_na_values(df_clean, na_policy, na_fill_value)
    
    if date_columns:
        df_clean = enforce_date_format(df_clean, date_columns)
    
    # Handle datetime standardization
    if datetime_columns or auto_detect_datetime:
        if auto_detect_datetime and not datetime_columns:
            # Auto-detect datetime columns
            detected_datetime_cols = detect_datetime_columns(df_clean)
            if detected_datetime_cols:
                df_clean = standardize_datetime_format(df_clean, detected_datetime_cols, datetime_format)
        elif datetime_columns:
            # Use specified datetime columns
            df_clean = standardize_datetime_format(df_clean, datetime_columns, datetime_format)
    
    if remove_duplicates_flag:
        df_clean = remove_duplicates(df_clean, duplicate_subset)
    
    return df_clean
