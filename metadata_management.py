"""
File: metadata_management.py
Author: Satej
Description:
    This script manages metadata for the data lake using AWS Glue Catalog.
    It includes functionality to create, update, and query metadata repositories.
"""

import boto3
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Setting up logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the Glue client
glue_client = boto3.client('glue')

def create_metadata_table(database_name, table_name, s3_path, schema):
    """
    Function to create a metadata table in the AWS Glue Catalog.

    Parameters:
        database_name (str): Name of the Glue database.
        table_name (str): Name of the table to create.
        s3_path (str): S3 location of the data.
        schema (list): List of dictionaries defining the schema (name, type).

    Example Usage:
        create_metadata_table(
            'satej_data_lake_db',
            'raw_data_table',
            's3://satej-data-lake/raw-zone/',
            [{'Name': 'id', 'Type': 'int'}, {'Name': 'name', 'Type': 'string'}]
        )
    """
    try:
        logging.info(f"Creating metadata table '{table_name}' in database '{database_name}'")
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': schema,
                    'Location': s3_path,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'Compressed': False
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        logging.info("Metadata table created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        logging.warning(f"Table '{table_name}' already exists in database '{database_name}'.")
    except (NoCredentialsError, PartialCredentialsError):
        logging.error("AWS credentials are not properly configured.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def main():
    """
    Main function to trigger metadata management.
    """
    # Replace these parameters with actual values
    database_name = 'satej_data_lake_db'
    table_name = 'raw_data_table'
    s3_path = 's3://satej-data-lake/raw-zone/'
    schema = [
        {'Name': 'id', 'Type': 'int'},
        {'Name': 'name', 'Type': 'string'},
        {'Name': 'timestamp', 'Type': 'string'}
    ]

    create_metadata_table(database_name, table_name, s3_path, schema)

if __name__ == "__main__":
    main()
