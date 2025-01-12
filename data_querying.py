"""
File: data_querying.py
Author: Satej
Description:
    This script performs data querying from the data lake using AWS Athena.
    It supports both real-time and historical data analysis.
"""

import boto3
import logging
from time import sleep

# Setting up logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the Athena client
athena_client = boto3.client('athena')

def query_data(database_name, query_string, output_s3_path):
    """
    Function to execute a query in AWS Athena and store results in S3.

    Parameters:
        database_name (str): Name of the Athena database.
        query_string (str): SQL query to execute.
        output_s3_path (str): S3 location to store the query results.

    Example Usage:
        query_data(
            'satej_data_lake_db',
            'SELECT * FROM raw_data_table LIMIT 10;',
            's3://satej-data-lake/query-results/'
        )
    """
    try:
        logging.info("Starting query execution...")
        response = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': database_name
            },
            ResultConfiguration={
                'OutputLocation': output_s3_path
            }
        )

        query_execution_id = response['QueryExecutionId']
        logging.info(f"Query execution started with ID: {query_execution_id}")

        # Wait for the query to complete
        while True:
            status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            logging.info("Query is still running. Waiting for completion...")
            sleep(2)

        if state == 'SUCCEEDED':
            logging.info("Query completed successfully.")
        else:
            logging.error(f"Query failed with state: {state}")

    except (NoCredentialsError, PartialCredentialsError):
        logging.error("AWS credentials are not properly configured.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def main():
    """
    Main function to trigger data querying.
    """
    # Replace these parameters with actual values
    database_name = 'satej_data_lake_db'
    query_string = 'SELECT * FROM raw_data_table LIMIT 10;'
    output_s3_path = 's3://satej-data-lake/query-results/'

    query_data(database_name, query_string, output_s3_path)

if __name__ == "__main__":
    main()
