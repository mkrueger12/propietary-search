from pathlib import Path
import polars as pl
from typing import List, Dict, Optional
import logging
from dataclasses import dataclass

# === Set up logging ===
logger = logging.getLogger(__name__)


@dataclass
class CompanyRecord:
    """Data class to represent a single company record."""
    company_name: str
    parent_company_name: Optional[str]
    executive_first_name: Optional[str]
    executive_last_name: Optional[str]
    address: str
    city: str
    state: str
    zip_code: str
    legal_name: Optional[str]
    record_type: str


class CSVValidationError(Exception):
    """Custom exception for CSV validation errors."""
    pass


class CSVHandler:
    """Handles reading, validation, and processing of company CSV data."""

    REQUIRED_COLUMNS = {
        "Company Name",
        "Parent Company Name",
        "Executive First Name",
        "Executive Last Name",
        "Address",
        "City",
        "State",
        "ZIP Code",
        "Legal Name",
        "Record Type"
    }

    def __init__(self, file_path: str):
        """Initialize the CSV handler with file path."""
        self.file_path = Path(file_path)
        self.data: Optional[pl.DataFrame] = None

    def validate_file_exists(self) -> None:
        """Check if the CSV file exists."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found at: {self.file_path}")

        if not self.file_path.suffix.lower() == '.csv':
            raise ValueError(f"File must be a CSV file, got: {self.file_path.suffix}")

    def validate_columns(self, df: pl.DataFrame) -> None:
        """Validate that all required columns are present."""
        missing_columns = self.REQUIRED_COLUMNS - set(df.columns)
        if missing_columns:
            raise CSVValidationError(f"Missing required columns: {missing_columns}")

    def validate_data_types(self, df: pl.DataFrame) -> None:
        """Validate data types and required fields."""
        # === Check for null values in required fields ===
        required_non_null = ["Company Name", "Address", "City", "State", "ZIP Code"]

        # Using Polars' efficient null checking
        null_counts = df.select([
            pl.col(col).is_null().sum().alias(col)
            for col in required_non_null
        ])

        null_fields = [
            f"{col}: {count}"
            for col, count in zip(required_non_null, null_counts.row(0))
            if count > 0
        ]

        if null_fields:
            raise CSVValidationError(f"Null values found in required fields: {', '.join(null_fields)}")

        # === Validate ZIP code format (basic check for now) ===
        invalid_zips = df.filter(
            ~pl.col("ZIP Code").cast(str).str.contains(r'^\d{5}(-\d{4})?$')
        )

        if len(invalid_zips) > 0:
            logger.warning(f"Found {len(invalid_zips)} records with invalid ZIP codes")

    def clean_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean and standardize the data."""
        # === Get string columns ===
        string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]

        # === First pass: strip whitespace and handle empty strings ===
        cleaned = df.with_columns([
            pl.when(pl.col(col).str.strip_chars() == "")
            .then(None)
            .otherwise(pl.col(col).str.strip_chars())
            .alias(col)
            for col in string_cols
        ])

        # === Second pass: handle state and zip code ===
        cleaned = cleaned.with_columns([
            pl.col("State").str.to_uppercase().alias("State"),
            pl.col("ZIP Code").cast(str).str.extract(r'(\d{5})', 1).alias("ZIP Code")
        ])

        return cleaned

    def read_and_validate(self) -> pl.DataFrame:
        """Read the CSV file, validate its contents, and return the processed DataFrame."""
        try:
            # === Check if file exists ===
            self.validate_file_exists()

            # === Read the CSV file efficiently with Polars ===
            logger.info(f"Reading CSV file: {self.file_path}")
            df = pl.scan_csv(self.file_path).collect()

            # === Validate structure and content ===
            self.validate_columns(df)
            self.validate_data_types(df)

            # === Clean the data ===
            cleaned_df = self.clean_data(df)

            self.data = cleaned_df
            logger.info(f"Successfully processed {len(cleaned_df)} records")

            return cleaned_df

        except Exception as e:
            logger.error(f"Error processing CSV file: {str(e)}")
            raise

    def get_company_records(self) -> List[CompanyRecord]:
        """Convert the DataFrame to a list of CompanyRecord objects."""
        if self.data is None:
            raise ValueError("Data not loaded. Call read_and_validate() first.")

        # === Convert to records efficiently using Polars ===
        records = []
        for row in self.data.iter_rows(named=True):
            try:
                record = CompanyRecord(
                    company_name=row["Company Name"],
                    parent_company_name=row["Parent Company Name"],
                    executive_first_name=row["Executive First Name"],
                    executive_last_name=row["Executive Last Name"],
                    address=row["Address"],
                    city=row["City"],
                    state=row["State"],
                    zip_code=row["ZIP Code"],
                    legal_name=row["Legal Name"],
                    record_type=row["Record Type"]
                )
                records.append(record)
            except Exception as e:
                logger.error(f"Error creating record for company {row['Company Name']}: {str(e)}")
                continue

        return records

    def save_processed_data(self, output_path: str) -> None:
        """Save the processed DataFrame to a new CSV file."""
        if self.data is None:
            raise ValueError("No data to save. Call read_and_validate() first.")

        output_path = Path(output_path)
        self.data.write_csv(output_path)
        logger.info(f"Saved processed data to {output_path}")