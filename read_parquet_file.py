import os
import pandas as pd
import pyarrow.parquet as pq
import logging

# Set up logging
logging.basicConfig(
    filename="parquet_to_csv.log",
    level=logging.INFO,
    format="%(message)s",
    filemode="w"
)


def log(message):
    """Log message to both console and file"""
    print(message)
    logging.info(message)


def parquet_to_csv_pandas():
    """
    Read Parquet file using pandas and write to CSV
    This is the simplest approach using pandas
    """
    log("Converting Parquet to CSV using pandas")

    try:
        parquet_file = "FIN_Kenya_governmentpayroll_NPV_detail_20250331.parquet"
        csv_output = "FIN_Kenya_governmentpayroll_NPV_detail_20250331_pandas.csv"

        log(f"Reading Parquet file: {parquet_file}")

        # Read the Parquet file with pandas
        df = pd.read_parquet(parquet_file)
        log(f"Successfully read Parquet file with {len(df)} rows and {len(df.columns)} columns")

        # Display sample of the data
        log("Sample data (first 5 rows):")
        for i in range(min(5, len(df))):
            record = df.iloc[i].to_dict()
            log(f"Record {i + 1}: {record}")

        # Write to CSV
        log(f"Writing data to CSV file: {csv_output}")
        df.to_csv(csv_output, index=False)

        # Verify the CSV file was created
        if os.path.exists(csv_output):
            file_size = os.path.getsize(csv_output)
            log(f"Successfully wrote CSV file. Size: {file_size} bytes")
            return True
        else:
            log(f"Failed to create CSV file: {csv_output}")
            return False

    except Exception as e:
        log(f"Error in parquet_to_csv_pandas: {str(e)}")
        log(f"Error details: {repr(e)}")
        return False

# Run both conversion methods
if __name__ == "__main__":
    log("Starting Parquet to CSV conversion...")

    # Check if Parquet file exists
    parquet_file = "FIN_Kenya_governmentpayroll_NPV_detail_20250331.parquet"
    if not os.path.exists(parquet_file):
        log(f"Parquet file does not exist: {parquet_file}")
        exit(1)

    # Convert using pandas
    success_pandas = parquet_to_csv_pandas()
    if success_pandas:
        log("Successfully converted Parquet to CSV using pandas")
    else:
        log("Failed to convert Parquet to CSV using pandas")

    # Summary
    if success_pandas:
        log("Only pandas conversion completed successfully")
    else:
        log("Both conversion methods failed")

    log("Script execution completed")
