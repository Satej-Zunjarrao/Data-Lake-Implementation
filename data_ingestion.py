"""
File: data_ingestion.py
Author: Satej
Description:
    This script handles the ingestion of data from multiple sources into AWS S3.
    It leverages AWS Glue for schema detection and metadata management.
    This ensures seamless integration of transactional systems, IoT devices, and log files.
"""

import boto3
import logging
import pandas as pd
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Setting up logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize AWS S3 client
s3_client = boto3.client('s3')

# Initialize Spark and Glue contexts
sc = SparkContext()
glue_context = GlueContext(sc)

def ingest_data_to_s3(source_path, bucket_name, s3_key_prefix):
    """
    Function to ingest data from a local or remote path into AWS S3.

    Parameters:
        source_path (str): Path to the source data (local or remote).
        bucket_name (str): Name of the target S3 bucket.
        s3_key_prefix (str): Prefix for the S3 key where data will be stored.

    Example Usage:
        ingest_data_to_s3('path/to/local/file.csv', 'satej-data-lake', 'raw-zone/')
    """
    try:
        logging.info(f"Reading data from source: {source_path}")
        # Example: Read local file into a DataFrame
        data = pd.read_csv(source_path)
        
        # Create the full S3 key
        s3_key = f"{s3_key_prefix}{source_path.split('/')[-1]}"
        logging.info(f"Uploading data to S3 bucket: {bucket_name}, Key: {s3_key}")
        
        # Upload data to S3
        s3_client.put_object(Body=data.to_csv(index=False), Bucket=bucket_name, Key=s3_key)
        logging.info("Data successfully uploaded to S3.")
    except FileNotFoundError:
        logging.error("Source file not found. Please check the path.")
    except (NoCredentialsError, PartialCredentialsError):
        logging.error("AWS credentials are not properly configured.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def main():
    """
    Main function to trigger data ingestion.
    """
    # Replace these paths with actual source paths and bucket details
    source_path = 'path/to/local/file.csv'  # Example local path
    bucket_name = 'satej-data-lake'
    s3_key_prefix = 'raw-zone/'

    ingest_data_to_s3(source_path, bucket_name, s3_key_prefix)

if __name__ == "__main__":
    main()
