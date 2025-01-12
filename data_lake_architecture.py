"""
File: data_lake_architecture.py
Author: Satej
Description:
    This script defines the multi-zone architecture of the data lake.
    It creates zones (Raw, Processed, Curated) and organizes data for optimized performance.
"""

import boto3
import logging
from botocore.exceptions import NoCredentialsError

# Setting up logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the S3 client
s3_client = boto3.client('s3')

def create_data_lake_zones(bucket_name, zones):
    """
    Function to create zones within the data lake.

    Parameters:
        bucket_name (str): Name of the S3 bucket.
        zones (list): List of zone names to create (e.g., ['raw-zone', 'processed-zone', 'curated-zone']).

    Example Usage:
        create_data_lake_zones('satej-data-lake', ['raw-zone', 'processed-zone', 'curated-zone'])
    """
    try:
        for zone in zones:
            folder_path = f"{zone}/"
            logging.info(f"Creating folder for zone: {folder_path} in bucket: {bucket_name}")
            s3_client.put_object(Bucket=bucket_name, Key=folder_path)
        logging.info("Data lake zones created successfully.")
    except NoCredentialsError:
        logging.error("AWS credentials are not properly configured.")
    except Exception as e:
        logging.error(f"An error occurred while creating zones: {e}")

def partition_data(bucket_name, zone, partition_key, partition_values):
    """
    Function to partition data within a specific zone.

    Parameters:
        bucket_name (str): Name of the S3 bucket.
        zone (str): Zone name (e.g., 'processed-zone').
        partition_key (str): Partition key (e.g., 'date').
        partition_values (list): List of partition values (e.g., ['2023-01-01', '2023-01-02']).

    Example Usage:
        partition_data('satej-data-lake', 'processed-zone', 'date', ['2023-01-01', '2023-01-02'])
    """
    try:
        for value in partition_values:
            folder_path = f"{zone}/{partition_key}={value}/"
            logging.info(f"Creating partition folder: {folder_path} in bucket: {bucket_name}")
            s3_client.put_object(Bucket=bucket_name, Key=folder_path)
        logging.info("Partitions created successfully.")
    except Exception as e:
        logging.error(f"An error occurred while creating partitions: {e}")

def main():
    """
    Main function to set up the data lake architecture.
    """
    # Replace with actual bucket name and zone details
    bucket_name = 'satej-data-lake'
    zones = ['raw-zone', 'processed-zone', 'curated-zone']
    partition_key = 'date'
    partition_values = ['2025-01-01', '2025-01-02']

    create_data_lake_zones(bucket_name, zones)
    partition_data(bucket_name, 'processed-zone', partition_key, partition_values)

if __name__ == "__main__":
    main()
