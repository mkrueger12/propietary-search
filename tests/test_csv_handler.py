import pytest
import polars as pl
from pathlib import Path
import tempfile
from src.data_loader.csv_handler import CSVHandler, CSVValidationError, CompanyRecord


# === Test data fixtures ===
@pytest.fixture
def valid_csv_data():
    return pl.DataFrame({
        "Company Name": ["Tech Corp", "Data Inc"],
        "Parent Company Name": ["Parent Corp", None],
        "Executive First Name": ["John", "Jane"],
        "Executive Last Name": ["Doe", "Smith"],
        "Address": ["123 Tech St", "456 Data Ave"],
        "City": ["San Francisco", "New York"],
        "State": ["CA", "NY"],
        "ZIP Code": ["94105", "10001"],
        "Legal Name": ["TC Limited", "DI Corp"],
        "Record Type": ["HQ", "Branch"]
    })


@pytest.fixture
def temp_csv_file(valid_csv_data, tmp_path):
    """Create a temporary CSV file with valid data."""
    file_path = tmp_path / "test_data.csv"
    valid_csv_data.write_csv(file_path)
    return file_path


# === Test CSV Handler initialization ===
def test_csv_handler_init():
    """Test CSVHandler initialization."""
    handler = CSVHandler("dummy.csv")
    assert handler.file_path == Path("dummy.csv")
    assert handler.data is None


# === Test file validation ===
def test_validate_file_exists(temp_csv_file):
    """Test file existence validation."""
    handler = CSVHandler(temp_csv_file)
    handler.validate_file_exists()  # Should not raise


def test_validate_file_not_exists():
    """Test file not found error."""
    handler = CSVHandler("nonexistent.csv")
    with pytest.raises(FileNotFoundError):
        handler.validate_file_exists()


def test_validate_non_csv_file(tmp_path):
    """Test non-CSV file error."""
    txt_file = tmp_path / "test.txt"
    txt_file.touch()
    handler = CSVHandler(str(txt_file))
    with pytest.raises(ValueError, match="File must be a CSV file"):
        handler.validate_file_exists()


# === Test column validation ===
def test_validate_columns_success(valid_csv_data):
    """Test successful column validation."""
    handler = CSVHandler("dummy.csv")
    handler.validate_columns(valid_csv_data)  # Should not raise


def test_validate_columns_missing(valid_csv_data):
    """Test missing columns validation."""
    invalid_data = valid_csv_data.drop("Company Name")
    handler = CSVHandler("dummy.csv")
    with pytest.raises(CSVValidationError, match="Missing required columns"):
        handler.validate_columns(invalid_data)


# === Test data type validation ===
def test_validate_data_types_success(valid_csv_data):
    """Test successful data type validation."""
    handler = CSVHandler("dummy.csv")
    handler.validate_data_types(valid_csv_data)  # Should not raise


def test_validate_data_types_null_values():
    """Test null values in required fields."""
    invalid_data = pl.DataFrame({
        "Company Name": [None, "Data Inc"],
        "Parent Company Name": ["Parent Corp", None],
        "Executive First Name": ["John", "Jane"],
        "Executive Last Name": ["Doe", "Smith"],
        "Address": ["123 Tech St", "456 Data Ave"],
        "City": ["San Francisco", None],
        "State": ["CA", "NY"],
        "ZIP Code": ["94105", "10001"],
        "Legal Name": ["TC Limited", "DI Corp"],
        "Record Type": ["HQ", "Branch"]
    })
    handler = CSVHandler("dummy.csv")
    with pytest.raises(CSVValidationError, match="Null values found in required fields"):
        handler.validate_data_types(invalid_data)


# === Test data cleaning ===
def test_clean_data():
    """Test data cleaning functionality."""
    dirty_data = pl.DataFrame({
        "Company Name": [" Tech Corp ", "Data Inc"],
        "Parent Company Name": ["Parent Corp", ""],
        "Executive First Name": ["John ", "Jane"],
        "Executive Last Name": ["Doe", "Smith"],
        "Address": ["123 Tech St", "456 Data Ave"],
        "City": ["San Francisco", "New York"],
        "State": ["ca", "ny"],
        "ZIP Code": ["94105-1234", "10001"],
        "Legal Name": ["TC Limited", "DI Corp"],
        "Record Type": ["HQ", "Branch"]
    })
    handler = CSVHandler("dummy.csv")
    cleaned = handler.clean_data(dirty_data)

    assert cleaned["Company Name"][0] == "Tech Corp"  # Whitespace stripped
    assert cleaned["State"][0] == "CA"  # State uppercase
    assert cleaned["ZIP Code"][0] == "94105"  # ZIP code standardized
    assert cleaned["Parent Company Name"][1] is None  # Empty string converted to None


# === Test full processing ===
def test_read_and_validate(temp_csv_file):
    """Test complete read and validate process."""
    handler = CSVHandler(temp_csv_file)
    df = handler.read_and_validate()
    assert len(df) == 2
    assert all(col in df.columns for col in handler.REQUIRED_COLUMNS)


# === Test company record conversion ===
def test_get_company_records(temp_csv_file):
    """Test conversion to CompanyRecord objects."""
    handler = CSVHandler(temp_csv_file)
    handler.read_and_validate()
    records = handler.get_company_records()

    assert len(records) == 2
    assert isinstance(records[0], CompanyRecord)
    assert records[0].company_name == "Tech Corp"
    assert records[1].company_name == "Data Inc"


# === Test save functionality ===
def test_save_processed_data(temp_csv_file, tmp_path):
    """Test saving processed data."""
    handler = CSVHandler(temp_csv_file)
    handler.read_and_validate()

    output_path = tmp_path / "processed.csv"
    handler.save_processed_data(output_path)

    assert output_path.exists()
    # Verify the saved data can be read back
    saved_data = pl.read_csv(output_path)
    assert len(saved_data) == 2
    assert all(col in saved_data.columns for col in handler.REQUIRED_COLUMNS)


# === Test error handling ===
def test_error_handling_get_records_without_data():
    """Test error when accessing records before loading data."""
    handler = CSVHandler("dummy.csv")
    with pytest.raises(ValueError, match="Data not loaded"):
        handler.get_company_records()


def test_error_handling_save_without_data():
    """Test error when saving before loading data."""
    handler = CSVHandler("dummy.csv")
    with pytest.raises(ValueError, match="No data to save"):
        handler.save_processed_data("output.csv")