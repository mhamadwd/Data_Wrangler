# Data Wrangler 🔧

A desktop application for cleaning and merging CSV files, designed specifically for data analysts.
Created by Mohamad W.

## Features

- **Multi-file Upload**: Upload multiple CSV files at once
- **Smart Data Cleaning**: 
  - Automatic encoding detection
  - Column name standardization (snake_case)
  - Data type inference (dates, integers, floats, booleans)
  - Whitespace trimming
  - NA value handling (drop/keep/fill)
  - Date format enforcement (YYYY-MM-DD)
  - Duplicate removal
- **Flexible Merging**: 
  - Append mode (combine rows)
  - Per-sheet mode (separate sheets in Excel)
- **Quality Reporting**: Detailed data quality metrics and warnings
- **Excel Export**: Download cleaned data as Excel files
- **User-friendly Interface**: Clean desktop interface with tkinter

## Installation

### Using Poetry (Recommended)

```bash
# Install Poetry if you haven't already
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### Using pip

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Running the Application


1. Double-click `Data_Wrangler.bat` to launch the application
2. The desktop interface will open automatically

**Command Line (Alternative):**
```bash
python desktop_app.py
```

### Basic Workflow

1. **Upload Files**: Click "Select CSV Files" to choose one or more CSV files
2. **Configure Options**: Set processing options like delimiter, decimal separator, merge mode, etc.
3. **Process Data**: Click "Process Files" to clean and prepare your data
4. **Review Results**: Check the data preview and quality report
5. **Download**: Download the cleaned Excel file and quality report

### Sample Data

The `assets/` directory contains sample CSV files for testing:
- `sample_customers.csv` - Customer data
- `sample_orders.csv` - Order data  
- `sample_products.csv` - Product data
- `sample_dirty_data.csv` - Data with cleaning challenges

## Configuration Options

### File Processing
- **CSV Delimiter**: Choose from comma, semicolon, tab, or pipe
- **Decimal Separator**: Period or comma
- **Thousands Separator**: Comma, period, space, or none
- **Encoding**: Auto-detected or manually specified

### Data Cleaning
- **Column Standardization**: Convert to snake_case
- **Data Type Inference**: Automatically detect optimal types
- **Whitespace Trimming**: Remove leading/trailing spaces
- **NA Handling**: Drop, keep, or fill missing values
- **Date Formatting**: Standardize to YYYY-MM-DD
- **Duplicate Removal**: Remove duplicate rows

### Export Options
- **Merge Mode**: 
  - `per_sheet`: Each CSV becomes a separate Excel sheet
  - `single_sheet`: All data combined into one sheet
- **Output Format**: Excel (.xlsx)

## Development

### Project Structure

```
data-wrangler/
├── app.py                 # Main Streamlit application
├── wrangle/              # Core processing modules
│   ├── __init__.py
│   ├── cleaning.py       # Data cleaning functions
│   ├── merge.py          # Data merging functions
│   ├── report.py         # Quality reporting
│   └── io.py            # Safe I/O operations
├── tests/                # Unit tests
│   ├── __init__.py
│   ├── test_cleaning.py
│   └── test_merge.py
├── assets/               # Sample data files
│   ├── sample_customers.csv
│   ├── sample_orders.csv
│   ├── sample_products.csv
│   └── sample_dirty_data.csv
├── requirements.txt      # Python dependencies
├── pyproject.toml       # Poetry configuration
└── README.md
```

### Running Tests

```bash
# Using Poetry
poetry run pytest

# Using pip
pytest
```

### Code Quality

```bash
# Format code
black .

# Lint code
ruff check .

# Type checking (if using mypy)
mypy .
```



### Core Functions

#### Data Cleaning (`wrangle.cleaning`)

- `clean_dataframe()`: Apply comprehensive data cleaning
- `standardize_column_names()`: Convert to snake_case
- `infer_dtypes()`: Detect optimal data types
- `coerce_dtypes()`: Convert to specified types
- `trim_whitespace()`: Remove leading/trailing spaces
- `handle_na_values()`: Process missing values
- `enforce_date_format()`: Standardize date formats
- `remove_duplicates()`: Remove duplicate rows

#### Data Merging (`wrangle.merge`)

- `merge_dataframes()`: Merge multiple DataFrames
- `append_dataframes()`: Concatenate rows
- `join_dataframes()`: Join on common columns
- `prepare_excel_export()`: Prepare for Excel export
- `validate_merge_operation()`: Validate merge parameters

#### Quality Reporting (`wrangle.report`)

- `generate_quality_report()`: Create comprehensive report
- `generate_single_file_report()`: Analyze single file
- `format_report_text()`: Format as readable text
- `generate_processing_log()`: Create operation log

#### I/O Operations (`wrangle.io`)

- `safe_read_csv()`: Read CSV with error handling
- `safe_write_excel()`: Write Excel with error handling
- `read_multiple_csvs()`: Read multiple files
- `detect_encoding_from_file()`: Auto-detect encoding

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request



## Changelog

### v0.1.0
- Initial release
- Basic CSV cleaning and merging functionality
- Quality reporting
- Excel export
