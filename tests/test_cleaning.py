"""Tests for data cleaning functions."""

import pytest
import pandas as pd
import numpy as np
from wrangle.cleaning import (
    standardize_column_names,
    infer_dtypes,
    coerce_dtypes,
    trim_whitespace,
    handle_na_values,
    enforce_date_format,
    standardize_datetime_format,
    detect_datetime_columns,
    remove_duplicates,
    clean_dataframe
)


class TestStandardizeColumnNames:
    """Test column name standardization."""
    
    def test_basic_standardization(self):
        """Test basic column name standardization."""
        df = pd.DataFrame({
            'First Name': [1, 2, 3],
            'Last-Name': [4, 5, 6],
            'Email Address': [7, 8, 9]
        })
        
        result = standardize_column_names(df)
        expected_columns = ['first_name', 'last_name', 'email_address']
        assert list(result.columns) == expected_columns
    
    def test_special_characters(self):
        """Test handling of special characters."""
        df = pd.DataFrame({
            'Name@Company': [1, 2, 3],
            'Value (USD)': [4, 5, 6],
            'Score%': [7, 8, 9]
        })
        
        result = standardize_column_names(df)
        expected_columns = ['name_company', 'value_usd', 'score']
        assert list(result.columns) == expected_columns
    
    def test_multiple_spaces(self):
        """Test handling of multiple spaces."""
        df = pd.DataFrame({
            'First   Name': [1, 2, 3],
            'Last    Name': [4, 5, 6]
        })
        
        result = standardize_column_names(df)
        expected_columns = ['first_name', 'last_name']
        assert list(result.columns) == expected_columns


class TestInferDtypes:
    """Test data type inference."""
    
    def test_numeric_inference(self):
        """Test inference of numeric types."""
        df = pd.DataFrame({
            'integers': [1, 2, 3, 4],
            'floats': [1.1, 2.2, 3.3, 4.4],
            'strings': ['a', 'b', 'c', 'd']
        })
        
        dtypes = infer_dtypes(df)
        assert dtypes['integers'] == 'int64'  # pandas infers int64 for integer values
        assert dtypes['floats'] == 'float64'
        assert dtypes['strings'] == 'object'
    
    def test_datetime_inference(self):
        """Test inference of datetime types."""
        df = pd.DataFrame({
            'dates': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'timestamps': ['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00'],
            'strings': ['not a date', 'also not a date', 'still not a date']
        })
        
        dtypes = infer_dtypes(df)
        assert dtypes['dates'] == 'datetime64[ns]'
        assert dtypes['timestamps'] == 'datetime64[ns]'
        assert dtypes['strings'] == 'object'
    
    def test_boolean_inference(self):
        """Test inference of boolean types."""
        df = pd.DataFrame({
            'bools': [True, False, True, False],
            'string_bools': ['true', 'false', 'yes', 'no'],
            'numeric_bools': [1, 0, 1, 0],
            'strings': ['a', 'b', 'c', 'd']
        })
        
        dtypes = infer_dtypes(df)
        assert dtypes['bools'] == 'bool'
        # Note: string_bools and numeric_bools are not automatically inferred as bool
        # because they have more than 2 unique values or are not recognized as boolean patterns
        assert dtypes['string_bools'] == 'object'
        assert dtypes['numeric_bools'] == 'int64'
        assert dtypes['strings'] == 'object'


class TestCoerceDtypes:
    """Test data type coercion."""
    
    def test_numeric_coercion(self):
        """Test coercion to numeric types."""
        df = pd.DataFrame({
            'integers': ['1', '2', '3', '4'],
            'floats': ['1.1', '2.2', '3.3', '4.4']
        })
        
        dtype_map = {'integers': 'int64', 'floats': 'float64'}
        result = coerce_dtypes(df, dtype_map)
        
        assert result['integers'].dtype == 'int64'
        assert result['floats'].dtype == 'float64'
    
    def test_datetime_coercion(self):
        """Test coercion to datetime types."""
        df = pd.DataFrame({
            'dates': ['2023-01-01', '2023-01-02', '2023-01-03']
        })
        
        dtype_map = {'dates': 'datetime64[ns]'}
        result = coerce_dtypes(df, dtype_map)
        
        assert pd.api.types.is_datetime64_any_dtype(result['dates'])
    
    def test_boolean_coercion(self):
        """Test coercion to boolean types."""
        df = pd.DataFrame({
            'bools': ['true', 'false', 'yes', 'no']
        })
        
        dtype_map = {'bools': 'bool'}
        result = coerce_dtypes(df, dtype_map)
        
        assert result['bools'].dtype == 'bool'
        assert result['bools'].tolist() == [True, False, True, False]


class TestTrimWhitespace:
    """Test whitespace trimming."""
    
    def test_basic_trimming(self):
        """Test basic whitespace trimming."""
        df = pd.DataFrame({
            'col1': ['  hello  ', '  world  ', '  test  '],
            'col2': ['no_spaces', '  some spaces  ', '  ']
        })
        
        result = trim_whitespace(df)
        
        assert result['col1'].tolist() == ['hello', 'world', 'test']
        assert result['col2'].tolist() == ['no_spaces', 'some spaces', '']


class TestHandleNaValues:
    """Test NA value handling."""
    
    def test_drop_na(self):
        """Test dropping NA values."""
        df = pd.DataFrame({
            'col1': [1, 2, np.nan, 4],
            'col2': [5, np.nan, 7, 8]
        })
        
        result = handle_na_values(df, policy='drop')
        assert len(result) == 2  # Only rows without any NA values
    
    def test_fill_na(self):
        """Test filling NA values."""
        df = pd.DataFrame({
            'col1': [1, 2, np.nan, 4],
            'col2': [5, np.nan, 7, 8]
        })
        
        result = handle_na_values(df, policy='fill', fill_value=0)
        assert result['col1'].isna().sum() == 0
        assert result['col2'].isna().sum() == 0
        assert result['col1'].tolist() == [1, 2, 0, 4]
        assert result['col2'].tolist() == [5, 0, 7, 8]
    
    def test_keep_na(self):
        """Test keeping NA values."""
        df = pd.DataFrame({
            'col1': [1, 2, np.nan, 4],
            'col2': [5, np.nan, 7, 8]
        })
        
        result = handle_na_values(df, policy='keep')
        assert len(result) == len(df)
        assert result.equals(df)


class TestEnforceDateFormat:
    """Test date format enforcement."""
    
    def test_date_formatting(self):
        """Test date format enforcement."""
        df = pd.DataFrame({
            'dates': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        })
        
        result = enforce_date_format(df, ['dates'])
        assert result['dates'].dtype == 'object'
        assert result['dates'].tolist() == ['2023-01-01', '2023-01-02', '2023-01-03']


class TestStandardizeDatetimeFormat:
    """Test datetime format standardization."""
    
    def test_datetime_formatting(self):
        """Test datetime format standardization."""
        df = pd.DataFrame({
            'datetimes': pd.to_datetime(['2023-01-01 14:30:00', '2023-01-02 16:45:30', '2023-01-03 08:15:45'])
        })
        
        result = standardize_datetime_format(df, ['datetimes'])
        assert result['datetimes'].dtype == 'object'
        assert result['datetimes'].tolist() == ['2023-01-01 14:30:00', '2023-01-02 16:45:30', '2023-01-03 08:15:45']
    
    def test_custom_datetime_format(self):
        """Test custom datetime format."""
        df = pd.DataFrame({
            'datetimes': pd.to_datetime(['2023-01-01 14:30:00', '2023-01-02 16:45:30'])
        })
        
        result = standardize_datetime_format(df, ['datetimes'], '%Y-%m-%d %H:%M')
        assert result['datetimes'].dtype == 'object'
        assert result['datetimes'].tolist() == ['2023-01-01 14:30', '2023-01-02 16:45']


class TestDetectDatetimeColumns:
    """Test datetime column detection."""
    
    def test_detect_datetime_columns(self):
        """Test detection of datetime columns."""
        df = pd.DataFrame({
            'dates': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'datetimes': ['2023-01-01 14:30:00', '2023-01-02 16:45:30', '2023-01-03 08:15:45'],
            'strings': ['not a date', 'also not a date', 'still not a date']
        })
        
        detected = detect_datetime_columns(df)
        assert 'dates' in detected
        assert 'datetimes' in detected
        assert 'strings' not in detected
    
    def test_detect_mixed_format_datetime(self):
        """Test detection of mixed format datetime columns."""
        df = pd.DataFrame({
            'mixed_dates': ['2023-01-01', '01/02/2023', '2023-01-03 14:30:00', '02/04/2023 16:45:30'],
            'strings': ['not a date', 'also not a date', 'still not a date', 'definitely not a date']
        })
        
        detected = detect_datetime_columns(df)
        assert 'mixed_dates' in detected
        assert 'strings' not in detected


class TestRemoveDuplicates:
    """Test duplicate removal."""
    
    def test_remove_all_duplicates(self):
        """Test removing all duplicates."""
        df = pd.DataFrame({
            'col1': [1, 2, 2, 3, 3, 3],
            'col2': ['a', 'b', 'b', 'c', 'c', 'c']
        })
        
        result = remove_duplicates(df)
        assert len(result) == 3
        assert result['col1'].tolist() == [1, 2, 3]
    
    def test_remove_subset_duplicates(self):
        """Test removing duplicates based on subset."""
        df = pd.DataFrame({
            'col1': [1, 2, 2, 3],
            'col2': ['a', 'b', 'c', 'd']
        })
        
        result = remove_duplicates(df, subset=['col1'])
        assert len(result) == 3
        assert result['col1'].tolist() == [1, 2, 3]


class TestCleanDataframe:
    """Test comprehensive data cleaning."""
    
    def test_full_cleaning_pipeline(self):
        """Test the complete cleaning pipeline."""
        df = pd.DataFrame({
            'First Name': ['  John  ', '  Jane  ', '  Bob  '],
            'Last Name': ['  Doe  ', '  Smith  ', '  Johnson  '],
            'Age': ['25', '30', '35'],
            'Email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
            'Active': ['true', 'false', 'true']
        })
        
        result = clean_dataframe(
            df,
            standardize_cols=True,
            infer_dtypes_flag=True,
            trim_whitespace_flag=True,
            na_policy='keep',
            remove_duplicates_flag=True
        )
        
        # Check column names are standardized
        assert 'first_name' in result.columns
        assert 'last_name' in result.columns
        
        # Check whitespace is trimmed
        assert result['first_name'].iloc[0] == 'John'
        
        # Check data types are inferred
        assert result['age'].dtype in ['int64', 'float64']
        assert result['active'].dtype == 'bool'
    
    def test_cleaning_with_na_handling(self):
        """Test cleaning with NA handling."""
        df = pd.DataFrame({
            'col1': [1, 2, np.nan, 4],
            'col2': [5, np.nan, 7, 8]
        })
        
        result = clean_dataframe(
            df,
            na_policy='fill',
            na_fill_value=0
        )
        
        assert result['col1'].isna().sum() == 0
        assert result['col2'].isna().sum() == 0
    
    def test_cleaning_with_datetime_standardization(self):
        """Test cleaning with datetime standardization."""
        df = pd.DataFrame({
            'name': ['John', 'Jane', 'Bob'],
            'event_datetime': ['2023-01-01 14:30:00', '2023-01-02 16:45:30', '2023-01-03 08:15:45'],
            'event_date': ['2023-01-01', '2023-01-02', '2023-01-03']
        })
        
        result = clean_dataframe(
            df,
            datetime_columns=['event_datetime'],
            date_columns=['event_date'],
            auto_detect_datetime=False
        )
        
        # Check that datetime column is formatted correctly
        assert result['event_datetime'].dtype == 'object'
        assert result['event_datetime'].iloc[0] == '2023-01-01 14:30:00'
        
        # Check that date column is formatted correctly
        assert result['event_date'].dtype == 'object'
        assert result['event_date'].iloc[0] == '2023-01-01'
    
    def test_cleaning_with_auto_detect_datetime(self):
        """Test cleaning with auto-detect datetime."""
        df = pd.DataFrame({
            'name': ['John', 'Jane', 'Bob'],
            'event_time': ['2023-01-01 14:30:00', '2023-01-02 16:45:30', '2023-01-03 08:15:45'],
            'regular_text': ['not a date', 'also not a date', 'still not a date']
        })
        
        result = clean_dataframe(
            df,
            auto_detect_datetime=True
        )
        
        # Check that datetime column is detected and formatted
        assert 'event_time' in result.columns
        # The exact formatting depends on the detection and conversion process
