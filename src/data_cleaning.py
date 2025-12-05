import pandas as pd
import os


# Function to load raw sales data from CSV file
# What: Reads the raw CSV file into a pandas DataFrame
# Why: Centralizes file loading logic and adds error handling for missing files
def load_data(file_path: str) -> pd.DataFrame:
    """Load sales data from CSV file with error handling."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found at: {file_path}")
    
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} rows from {file_path}")
    print(f"Original columns: {list(df.columns)}")
    return df


# Function to standardize all column names to lowercase with underscores
# What: Converts column names to snake_case format (lowercase with underscores)
# Why: Ensures consistency across the dataset and prevents case-sensitivity issues
#      in downstream analysis. Makes column referencing easier and more predictable.
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to lowercase with underscores."""
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.strip()
    print(f"Standardized column names: {list(df.columns)}")
    return df


# Function to remove leading/trailing whitespace from text columns
# What: Strips whitespace from all text (object) columns
# Why: Whitespace can cause duplicate entries ("Apple " vs "Apple") and matching issues.
#      This ensures data integrity and accurate grouping/filtering operations.
def clean_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Remove leading and trailing whitespace from all text columns."""
    # Find text columns dynamically
    text_columns = df.select_dtypes(include=['object']).columns
    
    for col in text_columns:
        df[col] = df[col].astype(str).str.strip()
        print(f"Cleaned whitespace from '{col}'")
    
    return df


# Function to convert price and quantity columns to numeric types
# What: Converts price and quantity columns from text to numbers
# Why: CSV files often read numeric data as strings. We need numeric types
#      to perform mathematical operations and comparisons (like filtering negatives).
def convert_to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Convert price and quantity columns to numeric data types."""
    # Find columns that should be numeric
    price_cols = [col for col in df.columns if 'price' in col.lower()]
    quantity_cols = [col for col in df.columns if 'quantity' in col.lower() or 'qty' in col.lower()]
    
    numeric_cols = price_cols + quantity_cols
    
    for col in numeric_cols:
        if col in df.columns:
            # Convert to numeric, coercing errors to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            print(f"Converted '{col}' to numeric type")
    
    return df


# Function to handle missing values in critical numeric columns
# What: Drops rows with missing values in numeric columns (price/quantity)
# Why: Missing prices and quantities prevent accurate sales calculations.
#      We drop these rows because imputing prices would create inaccurate financial data.
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values in numeric columns by dropping those rows."""
    initial_rows = len(df)
    
    # Find numeric columns (likely to be price and quantity)
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    print(f"Numeric columns found: {numeric_cols}")
    
    # Drop rows where any numeric column has missing values
    # We cannot reliably impute prices or quantities without business context
    if numeric_cols:
        df = df.dropna(subset=numeric_cols)
    
    rows_dropped = initial_rows - len(df)
    print(f"Dropped {rows_dropped} rows with missing numeric values")
    
    return df


# Function to remove rows with invalid numeric values
# What: Filters out rows with negative prices or negative quantities
# Why: Negative prices and quantities are data entry errors or system glitches.
#      They represent invalid business transactions and would skew analysis results.
#      Real sales cannot have negative prices or quantities (returns should be marked differently).
def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with negative prices or quantities (data entry errors)."""
    initial_rows = len(df)
    
    # Find columns that look like price or quantity
    # Check for columns containing 'price' or 'quantity' in their names
    price_cols = [col for col in df.columns if 'price' in col.lower()]
    quantity_cols = [col for col in df.columns if 'quantity' in col.lower() or 'qty' in col.lower()]
    
    # Combine the columns to check
    cols_to_check = price_cols + quantity_cols
    
    print(f"Checking for negative values in: {cols_to_check}")
    
    # Remove rows where price or quantity columns are negative
    for col in cols_to_check:
        if col in df.columns:
            initial_col_rows = len(df)
            df = df[df[col] >= 0]
            removed = initial_col_rows - len(df)
            if removed > 0:
                print(f"Removed {removed} rows with negative values in '{col}'")
    
    rows_removed = initial_rows - len(df)
    print(f"Total removed: {rows_removed} rows with invalid (negative) values")
    
    return df


# Function to save the cleaned DataFrame to CSV
# What: Writes the cleaned data to the processed data directory
# Why: Separates cleaned data from raw data, preserving the original dataset.
#      Creates a checkpoint for reproducible analysis pipeline.
def save_cleaned_data(df: pd.DataFrame, output_path: str) -> None:
    """Save cleaned DataFrame to CSV file."""
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_csv(output_path, index=False)
    print(f"Saved cleaned data to {output_path} ({len(df)} rows)")


if __name__ == "__main__":
    raw_path = "data/raw/sales_data_raw.csv"
    cleaned_path = "data/processed/sales_data_clean.csv"

    print("=" * 60)
    print("Starting Data Cleaning Pipeline")
    print("=" * 60)
    
    df_raw = load_data(raw_path)
    df_clean = clean_column_names(df_raw)
    df_clean = clean_text_fields(df_clean)
    df_clean = convert_to_numeric(df_clean)
    df_clean = handle_missing_values(df_clean)
    df_clean = remove_invalid_rows(df_clean)
    df_clean.to_csv(cleaned_path, index=False)
    
    print("=" * 60)
    print("Cleaning complete. First few rows:")
    print("=" * 60)
    print(df_clean.head())
    print(f"\nFinal shape: {df_clean.shape[0]} rows × {df_clean.shape[1]} columns")