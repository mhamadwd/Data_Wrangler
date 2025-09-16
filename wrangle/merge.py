"""Data merging functionality for the Data Wrangler app."""

import pandas as pd
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


def detect_schema_compatibility(dataframes: Dict[str, pd.DataFrame]) -> Tuple[bool, List[str]]:
    """Check if DataFrames have compatible schemas for merging.
    
    Args:
        dataframes: Dictionary mapping file names to DataFrames
        
    Returns:
        Tuple of (is_compatible, list_of_issues)
    """
    if len(dataframes) < 2:
        return True, []
    
    issues = []
    first_df = list(dataframes.values())[0]
    first_columns = set(first_df.columns)
    
    for name, df in dataframes.items():
        current_columns = set(df.columns)
        
        # Check for missing columns
        missing_in_current = first_columns - current_columns
        if missing_in_current:
            issues.append(f"{name} is missing columns: {missing_in_current}")
        
        # Check for extra columns
        extra_in_current = current_columns - first_columns
        if extra_in_current:
            issues.append(f"{name} has extra columns: {extra_in_current}")
    
    is_compatible = len(issues) == 0
    return is_compatible, issues


def merge_dataframes(dataframes: Dict[str, pd.DataFrame], 
                    merge_mode: str = 'append',
                    join_type: str = 'outer') -> pd.DataFrame:
    """Merge multiple DataFrames based on specified mode.
    
    Args:
        dataframes: Dictionary mapping file names to DataFrames
        merge_mode: 'append' to concatenate rows, 'join' to merge on common columns
        join_type: Type of join ('inner', 'outer', 'left', 'right') - only used if merge_mode is 'join'
        
    Returns:
        Merged DataFrame
    """
    if not dataframes:
        return pd.DataFrame()
    
    if len(dataframes) == 1:
        return list(dataframes.values())[0]
    
    if merge_mode == 'append':
        return append_dataframes(dataframes)
    elif merge_mode == 'join':
        return join_dataframes(dataframes, join_type)
    else:
        raise ValueError(f"Unknown merge mode: {merge_mode}")


def append_dataframes(dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Append DataFrames by concatenating rows.
    
    Args:
        dataframes: Dictionary mapping file names to DataFrames
        
    Returns:
        Concatenated DataFrame
    """
    try:
        # Ensure all DataFrames have the same columns
        all_columns = set()
        for df in dataframes.values():
            all_columns.update(df.columns)
        
        # Add missing columns to each DataFrame
        standardized_dfs = []
        for name, df in dataframes.items():
            df_copy = df.copy()
            for col in all_columns:
                if col not in df_copy.columns:
                    df_copy[col] = None
            standardized_dfs.append(df_copy)
        
        # Concatenate all DataFrames
        result = pd.concat(standardized_dfs, ignore_index=True)
        return result
    
    except Exception as e:
        logger.error(f"Error appending DataFrames: {e}")
        raise


def join_dataframes(dataframes: Dict[str, pd.DataFrame], join_type: str = 'outer') -> pd.DataFrame:
    """Join DataFrames on common columns.
    
    Args:
        dataframes: Dictionary mapping file names to DataFrames
        join_type: Type of join ('inner', 'outer', 'left', 'right')
        
    Returns:
        Joined DataFrame
    """
    if len(dataframes) < 2:
        return list(dataframes.values())[0]
    
    try:
        df_list = list(dataframes.values())
        result = df_list[0]
        
        for i, df in enumerate(df_list[1:], 1):
            # Find common columns for joining
            common_columns = set(result.columns) & set(df.columns)
            
            if not common_columns:
                logger.warning(f"No common columns found between DataFrames, using outer join")
                result = pd.concat([result, df], axis=1)
            else:
                # Use the first common column as the join key
                join_key = list(common_columns)[0]
                result = result.merge(df, on=join_key, how=join_type, suffixes=('', f'_df{i}'))
        
        return result
    
    except Exception as e:
        logger.error(f"Error joining DataFrames: {e}")
        raise


def prepare_excel_export(dataframes: Dict[str, pd.DataFrame], 
                        merge_mode: str = 'per_sheet',
                        merge_options: Optional[Dict] = None) -> Dict[str, pd.DataFrame]:
    """Prepare DataFrames for Excel export based on merge mode.
    
    Args:
        dataframes: Dictionary mapping file names to DataFrames
        merge_mode: 'per_sheet' or 'single_sheet'
        merge_options: Additional merge options
        
    Returns:
        Dictionary mapping sheet names to DataFrames
    """
    if merge_mode == 'per_sheet':
        return dataframes
    elif merge_mode == 'single_sheet':
        # Check schema compatibility
        is_compatible, issues = detect_schema_compatibility(dataframes)
        
        if not is_compatible:
            logger.warning(f"Schema compatibility issues detected: {issues}")
            # Still proceed but log warnings
        
        # Merge all DataFrames into one
        merged_df = merge_dataframes(dataframes, merge_mode='append')
        return {'merged_data': merged_df}
    else:
        raise ValueError(f"Unknown merge mode: {merge_mode}")


def validate_merge_operation(dataframes: Dict[str, pd.DataFrame], 
                           merge_mode: str) -> Tuple[bool, List[str]]:
    """Validate that a merge operation can be performed successfully.
    
    Args:
        dataframes: Dictionary mapping file names to DataFrames
        merge_mode: 'per_sheet', 'single_sheet', or 'join'
        
    Returns:
        Tuple of (is_valid, list_of_warnings)
    """
    warnings = []
    
    if not dataframes:
        warnings.append("No data to merge")
        return False, warnings
    
    if len(dataframes) == 1:
        return True, warnings
    
    if merge_mode in ['single_sheet', 'join']:
        is_compatible, issues = detect_schema_compatibility(dataframes)
        if not is_compatible:
            warnings.extend(issues)
    
    # Check for potential data loss
    total_rows = sum(len(df) for df in dataframes.values())
    if merge_mode == 'join':
        warnings.append("Join operations may result in data multiplication")
    
    return len(warnings) == 0, warnings
