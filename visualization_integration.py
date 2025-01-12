"""
File: visualization_integration.py
Author: Satej
Description:
    This script integrates the processed data from the data lake with SQL-based dashboards
    and machine learning pipelines. It includes sample data visualization using Matplotlib
    and Pandas for quick insights.
"""

import pandas as pd
import matplotlib.pyplot as plt
import boto3
import logging
from io import StringIO
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Setting up logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize S3 client
s3_client = boto3.client('s3')

def load_data_from_s3(bucket_name, file_key):
    """
    Function to load processed data from S3.

    Parameters:
        bucket_name (str): Name of the S3 bucket.
        file_key (str): Key of the file in the bucket.

    Returns:
        pandas.DataFrame: DataFrame containing the processed data.

    Example Usage:
        df = load_data_from_s3('satej-data-lake', 'processed-zone/sample.csv')
    """
    try:
        logging.info(f"Fetching data from S3 bucket: {bucket_name}, Key: {file_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        data = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(data))
        logging.info("Data successfully loaded into a DataFrame.")
        return df
    except s3_client.exceptions.NoSuchKey:
        logging.error("The specified file does not exist in the bucket.")
    except (NoCredentialsError, PartialCredentialsError):
        logging.error("AWS credentials are not properly configured.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

def visualize_data(df):
    """
    Function to visualize data using Matplotlib.

    Parameters:
        df (pandas.DataFrame): DataFrame containing the processed data.

    Example Usage:
        visualize_data(df)
    """
    try:
        logging.info("Generating visualizations...")
        # Example: Line plot for trends
        if 'date' in df.columns and 'value' in df.columns:
            plt.figure(figsize=(10, 6))
            plt.plot(pd.to_datetime(df['date']), df['value'], marker='o', linestyle='-', label='Value Trend')
            plt.title("Data Trend Over Time")
            plt.xlabel("Date")
            plt.ylabel("Value")
            plt.legend()
            plt.grid(True)
            plt.show()
            logging.info("Visualization generated successfully.")
        else:
            logging.warning("Required columns ('date', 'value') not found in the DataFrame.")
    except Exception as e:
        logging.error(f"An error occurred while visualizing data: {e}")

def integrate_with_ml_pipeline(df):
    """
    Function to integrate data with a machine learning pipeline.

    Parameters:
        df (pandas.DataFrame): DataFrame containing the processed data.

    Example Usage:
        integrate_with_ml_pipeline(df)
    """
    try:
        logging.info("Integrating data with the ML pipeline...")
        # Example: Splitting data into features and labels
        if 'label' in df.columns:
            features = df.drop(columns=['label'])
            labels = df['label']
            logging.info(f"Features and labels prepared for ML pipeline. Features shape: {features.shape}, Labels shape: {labels.shape}")
        else:
            logging.warning("Column 'label' not found in the DataFrame for ML pipeline integration.")
    except Exception as e:
        logging.error(f"An error occurred during ML pipeline integration: {e}")

def main():
    """
    Main function to load data, visualize it, and integrate with ML pipelines.
    """
    # Replace these with actual S3 bucket name and file key
    bucket_name = 'satej-data-lake'
    file_key = 'processed-zone/sample.csv'

    # Load data from S3
    df = load_data_from_s3(bucket_name, file_key)

    if df is not None:
        # Visualize data
        visualize_data(df)

        # Integrate data with ML pipeline
        integrate_with_ml_pipeline(df)

if __name__ == "__main__":
    main()
