"""
File: data_processing.py
Author: Satej
Description:
    This script handles the processing and cleaning of raw data using PySpark.
    It performs tasks such as removing missing values, standardizing formats, and storing
    the cleaned data in a processed zone in AWS S3.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import logging

# Setting up logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataProcessing") \
    .getOrCreate()

def process_data(input_s3_path, output_s3_path):
    """
    Function to process and clean raw data from AWS S3.

    Parameters:
        input_s3_path (str): Path to the raw data in S3.
        output_s3_path (str): Path to store the cleaned data in S3.

    Example Usage:
        process_data('s3://satej-data-lake/raw-zone/file.csv', 's3://satej-data-lake/processed-zone/file.csv')
    """
    try:
        logging.info(f"Reading raw data from S3: {input_s3_path}")
        # Load raw data into a Spark DataFrame
        raw_data = spark.read.csv(input_s3_path, header=True, inferSchema=True)

        logging.info("Cleaning data...")
        # Example cleaning steps
        cleaned_data = raw_data.na.drop()  # Drop rows with null values
        cleaned_data = cleaned_data.withColumn("source", lit("processed"))  # Add a metadata column

        logging.info(f"Writing processed data to S3: {output_s3_path}")
        # Write the cleaned data back to S3
        cleaned_data.write.csv(output_s3_path, mode="overwrite", header=True)
        logging.info("Data processing and upload completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during data processing: {e}")

def main():
    """
    Main function to trigger data processing.
    """
    # Replace these paths with actual S3 paths
    input_s3_path = 's3://satej-data-lake/raw-zone/file.csv'
    output_s3_path = 's3://satej-data-lake/processed-zone/file.csv'

    process_data(input_s3_path, output_s3_path)

if __name__ == "__main__":
    main()
