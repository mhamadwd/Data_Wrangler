"""Tests for data merging functions."""

import pytest
import pandas as pd
from wrangle.merge import (
    detect_schema_compatibility,
    merge_dataframes,
    append_dataframes,
    join_dataframes,
    prepare_excel_export,
    validate_merge_operation
)


class TestDetectSchemaCompatibility:
    """Test schema compatibility detection."""
    
    def test_compatible_schemas(self):
        """Test detection of compatible schemas."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
        
        dataframes = {'file1': df1, 'file2': df2}
        is_compatible, issues = detect_schema_compatibility(dataframes)
        
        assert is_compatible is True
        assert len(issues) == 0
    
    def test_incompatible_schemas(self):
        """Test detection of incompatible schemas."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'C': [7, 8]})
        
        dataframes = {'file1': df1, 'file2': df2}
        is_compatible, issues = detect_schema_compatibility(dataframes)
        
        assert is_compatible is False
        assert len(issues) > 0
        assert any('missing columns' in issue for issue in issues)
        assert any('extra columns' in issue for issue in issues)
    
    def test_single_dataframe(self):
        """Test with single DataFrame."""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        dataframes = {'file1': df}
        
        is_compatible, issues = detect_schema_compatibility(dataframes)
        assert is_compatible is True
        assert len(issues) == 0


class TestAppendDataframes:
    """Test DataFrame appending."""
    
    def test_append_compatible_schemas(self):
        """Test appending DataFrames with compatible schemas."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
        
        dataframes = {'file1': df1, 'file2': df2}
        result = append_dataframes(dataframes)
        
        assert len(result) == 4
        assert list(result.columns) == ['A', 'B']
        assert result['A'].tolist() == [1, 2, 5, 6]
        assert result['B'].tolist() == [3, 4, 7, 8]
    
    def test_append_incompatible_schemas(self):
        """Test appending DataFrames with incompatible schemas."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'C': [7, 8]})
        
        dataframes = {'file1': df1, 'file2': df2}
        result = append_dataframes(dataframes)
        
        assert len(result) == 4
        assert set(result.columns) == {'A', 'B', 'C'}
        # Check that missing values are filled with None
        assert result['C'].iloc[0] is None
        assert result['C'].iloc[1] is None
        assert result['B'].iloc[2] is None
        assert result['B'].iloc[3] is None
    
    def test_append_single_dataframe(self):
        """Test appending single DataFrame."""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        dataframes = {'file1': df}
        
        result = append_dataframes(dataframes)
        assert result.equals(df)


class TestJoinDataframes:
    """Test DataFrame joining."""
    
    def test_join_on_common_column(self):
        """Test joining DataFrames on common column."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [1, 3], 'C': [5, 6]})
        
        dataframes = {'file1': df1, 'file2': df2}
        result = join_dataframes(dataframes, 'outer')
        
        assert 'A' in result.columns
        assert 'B' in result.columns
        assert 'C' in result.columns
    
    def test_join_no_common_columns(self):
        """Test joining DataFrames with no common columns."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'C': [5, 6], 'D': [7, 8]})
        
        dataframes = {'file1': df1, 'file2': df2}
        result = join_dataframes(dataframes, 'outer')
        
        assert set(result.columns) == {'A', 'B', 'C', 'D'}


class TestMergeDataframes:
    """Test DataFrame merging."""
    
    def test_merge_append_mode(self):
        """Test merging in append mode."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
        
        dataframes = {'file1': df1, 'file2': df2}
        result = merge_dataframes(dataframes, merge_mode='append')
        
        assert len(result) == 4
        assert list(result.columns) == ['A', 'B']
    
    def test_merge_join_mode(self):
        """Test merging in join mode."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [1, 3], 'C': [5, 6]})
        
        dataframes = {'file1': df1, 'file2': df2}
        result = merge_dataframes(dataframes, merge_mode='join', join_type='outer')
        
        assert 'A' in result.columns
        assert 'B' in result.columns
        assert 'C' in result.columns
    
    def test_merge_single_dataframe(self):
        """Test merging single DataFrame."""
        df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        dataframes = {'file1': df}
        
        result = merge_dataframes(dataframes)
        assert result.equals(df)
    
    def test_merge_empty_dataframes(self):
        """Test merging empty DataFrames."""
        dataframes = {}
        result = merge_dataframes(dataframes)
        assert result.empty


class TestPrepareExcelExport:
    """Test Excel export preparation."""
    
    def test_per_sheet_mode(self):
        """Test per-sheet export mode."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
        
        dataframes = {'file1': df1, 'file2': df2}
        result = prepare_excel_export(dataframes, 'per_sheet')
        
        assert result == dataframes
        assert 'file1' in result
        assert 'file2' in result
    
    def test_single_sheet_mode(self):
        """Test single-sheet export mode."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
        
        dataframes = {'file1': df1, 'file2': df2}
        result = prepare_excel_export(dataframes, 'single_sheet')
        
        assert 'merged_data' in result
        assert len(result) == 1
        merged_df = result['merged_data']
        assert len(merged_df) == 4  # 2 rows from each DataFrame


class TestValidateMergeOperation:
    """Test merge operation validation."""
    
    def test_valid_merge_operation(self):
        """Test validation of valid merge operation."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
        
        dataframes = {'file1': df1, 'file2': df2}
        is_valid, warnings = validate_merge_operation(dataframes, 'single_sheet')
        
        assert is_valid is True
        assert len(warnings) == 0
    
    def test_invalid_merge_operation(self):
        """Test validation of invalid merge operation."""
        dataframes = {}
        is_valid, warnings = validate_merge_operation(dataframes, 'single_sheet')
        
        assert is_valid is False
        assert len(warnings) > 0
        assert any('No data to merge' in warning for warning in warnings)
    
    def test_merge_with_warnings(self):
        """Test merge operation with warnings."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'C': [7, 8]})
        
        dataframes = {'file1': df1, 'file2': df2}
        is_valid, warnings = validate_merge_operation(dataframes, 'single_sheet')
        
        # Should be valid but with warnings
        assert is_valid is False  # Schema incompatibility makes it invalid
        assert len(warnings) > 0
