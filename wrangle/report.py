"""Quality reporting functionality for the Data Wrangler app."""

import pandas as pd
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_quality_report(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """Generate comprehensive quality report for DataFrames.
    
    Args:
        dataframes: Dictionary mapping file names to DataFrames
        
    Returns:
        Dictionary containing quality metrics
    """
    report = {
        'timestamp': datetime.now().isoformat(),
        'total_files': len(dataframes),
        'files': {}
    }
    
    for name, df in dataframes.items():
        file_report = generate_single_file_report(df, name)
        report['files'][name] = file_report
    
    # Add summary statistics
    report['summary'] = generate_summary_stats(report['files'])
    
    return report


def generate_single_file_report(df: pd.DataFrame, filename: str) -> Dict[str, Any]:
    """Generate quality report for a single DataFrame.
    
    Args:
        df: DataFrame to analyze
        filename: Name of the file
        
    Returns:
        Dictionary containing file-specific metrics
    """
    report = {
        'filename': filename,
        'shape': {
            'rows': len(df),
            'columns': len(df.columns)
        },
        'columns': {},
        'data_types': {},
        'missing_data': {},
        'duplicates': 0,
        'warnings': []
    }
    
    # Column analysis
    for col in df.columns:
        col_data = df[col]
        
        # Basic stats
        col_report = {
            'dtype': str(col_data.dtype),
            'null_count': col_data.isnull().sum(),
            'null_percentage': (col_data.isnull().sum() / len(df)) * 100,
            'unique_count': col_data.nunique(),
            'duplicate_count': len(col_data) - col_data.nunique()
        }
        
        # Numeric column stats
        if pd.api.types.is_numeric_dtype(col_data):
            col_report.update({
                'min': col_data.min(),
                'max': col_data.max(),
                'mean': col_data.mean(),
                'std': col_data.std(),
                'median': col_data.median()
            })
        
        # String column stats
        elif col_data.dtype == 'object':
            col_report.update({
                'avg_length': col_data.astype(str).str.len().mean(),
                'max_length': col_data.astype(str).str.len().max(),
                'min_length': col_data.astype(str).str.len().min()
            })
        
        # Date column stats
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            col_report.update({
                'min_date': col_data.min(),
                'max_date': col_data.max(),
                'date_range_days': (col_data.max() - col_data.min()).days if not col_data.isnull().all() else None
            })
        
        report['columns'][col] = col_report
        report['data_types'][col] = str(col_data.dtype)
        report['missing_data'][col] = col_data.isnull().sum()
    
    # Duplicate analysis
    report['duplicates'] = df.duplicated().sum()
    
    # Generate warnings
    warnings = []
    
    # High missing data warning
    high_missing_cols = [col for col, stats in report['columns'].items() 
                        if stats['null_percentage'] > 50]
    if high_missing_cols:
        warnings.append(f"High missing data (>50%) in columns: {high_missing_cols}")
    
    # Duplicate rows warning
    if report['duplicates'] > 0:
        warnings.append(f"Found {report['duplicates']} duplicate rows")
    
    # Empty columns warning
    empty_cols = [col for col, stats in report['columns'].items() 
                 if stats['null_count'] == len(df)]
    if empty_cols:
        warnings.append(f"Empty columns: {empty_cols}")
    
    # Single value columns warning
    single_value_cols = [col for col, stats in report['columns'].items() 
                        if stats['unique_count'] == 1]
    if single_value_cols:
        warnings.append(f"Single value columns: {single_value_cols}")
    
    report['warnings'] = warnings
    
    return report


def generate_summary_stats(file_reports: Dict[str, Any]) -> Dict[str, Any]:
    """Generate summary statistics across all files.
    
    Args:
        file_reports: Dictionary of file reports
        
    Returns:
        Dictionary containing summary statistics
    """
    total_rows = sum(report['shape']['rows'] for report in file_reports.values())
    total_columns = sum(report['shape']['columns'] for report in file_reports.values())
    total_duplicates = sum(report['duplicates'] for report in file_reports.values())
    
    # Collect all warnings
    all_warnings = []
    for report in file_reports.values():
        all_warnings.extend(report['warnings'])
    
    # Count warnings by type
    warning_counts = {}
    for warning in all_warnings:
        warning_type = warning.split(':')[0] if ':' in warning else warning
        warning_counts[warning_type] = warning_counts.get(warning_type, 0) + 1
    
    return {
        'total_rows': total_rows,
        'total_columns': total_columns,
        'total_duplicates': total_duplicates,
        'warning_summary': warning_counts,
        'files_with_issues': len([r for r in file_reports.values() if r['warnings']])
    }


def format_report_text(report: Dict[str, Any]) -> str:
    """Format quality report as human-readable text.
    
    Args:
        report: Quality report dictionary
        
    Returns:
        Formatted report text
    """
    lines = []
    lines.append("=" * 60)
    lines.append("DATA WRANGLER QUALITY REPORT")
    lines.append("=" * 60)
    lines.append(f"Generated: {report['timestamp']}")
    lines.append(f"Total Files: {report['total_files']}")
    lines.append("")
    
    # Summary section
    summary = report['summary']
    lines.append("SUMMARY")
    lines.append("-" * 20)
    lines.append(f"Total Rows: {summary['total_rows']:,}")
    lines.append(f"Total Columns: {summary['total_columns']:,}")
    lines.append(f"Total Duplicates: {summary['total_duplicates']:,}")
    lines.append(f"Files with Issues: {summary['files_with_issues']}")
    lines.append("")
    
    # Warning summary
    if summary['warning_summary']:
        lines.append("WARNING SUMMARY")
        lines.append("-" * 20)
        for warning_type, count in summary['warning_summary'].items():
            lines.append(f"{warning_type}: {count} occurrences")
        lines.append("")
    
    # File details
    for filename, file_report in report['files'].items():
        lines.append(f"FILE: {filename}")
        lines.append("-" * 40)
        lines.append(f"Rows: {file_report['shape']['rows']:,}")
        lines.append(f"Columns: {file_report['shape']['columns']:,}")
        lines.append(f"Duplicates: {file_report['duplicates']:,}")
        
        if file_report['warnings']:
            lines.append("Warnings:")
            for warning in file_report['warnings']:
                lines.append(f"  - {warning}")
        
        lines.append("")
        
        # Column details
        lines.append("COLUMN ANALYSIS")
        lines.append("-" * 20)
        for col, stats in file_report['columns'].items():
            lines.append(f"{col} ({stats['dtype']}):")
            lines.append(f"  Nulls: {stats['null_count']} ({stats['null_percentage']:.1f}%)")
            lines.append(f"  Unique: {stats['unique_count']}")
            
            if 'mean' in stats:
                lines.append(f"  Mean: {stats['mean']:.2f}")
                lines.append(f"  Std: {stats['std']:.2f}")
            
            if 'avg_length' in stats:
                lines.append(f"  Avg Length: {stats['avg_length']:.1f}")
            
            lines.append("")
    
    return "\n".join(lines)


def generate_processing_log(operations: List[Dict[str, Any]]) -> str:
    """Generate a log of processing operations.
    
    Args:
        operations: List of operation dictionaries
        
    Returns:
        Formatted log text
    """
    lines = []
    lines.append("=" * 60)
    lines.append("DATA WRANGLER PROCESSING LOG")
    lines.append("=" * 60)
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append("")
    
    for i, op in enumerate(operations, 1):
        lines.append(f"Operation {i}: {op.get('name', 'Unknown')}")
        lines.append(f"  Timestamp: {op.get('timestamp', 'Unknown')}")
        lines.append(f"  Status: {op.get('status', 'Unknown')}")
        
        if 'details' in op:
            lines.append(f"  Details: {op['details']}")
        
        if 'warnings' in op and op['warnings']:
            lines.append("  Warnings:")
            for warning in op['warnings']:
                lines.append(f"    - {warning}")
        
        if 'errors' in op and op['errors']:
            lines.append("  Errors:")
            for error in op['errors']:
                lines.append(f"    - {error}")
        
        lines.append("")
    
    return "\n".join(lines)
