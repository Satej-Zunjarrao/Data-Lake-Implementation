"""
File: data_access_security.py
Author: Satej
Description:
    This script implements role-based access control (RBAC) and encryption mechanisms
    to ensure secure data access in the data lake. It uses AWS IAM policies and
    S3 bucket policies for access control.
"""

import boto3
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Setting up logging for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize AWS clients
iam_client = boto3.client('iam')
s3_client = boto3.client('s3')

def create_s3_bucket_policy(bucket_name, user_arn):
    """
    Function to create a bucket policy for role-based access control.

    Parameters:
        bucket_name (str): Name of the S3 bucket.
        user_arn (str): ARN of the IAM user or role allowed to access the bucket.

    Example Usage:
        create_s3_bucket_policy('satej-data-lake', 'arn:aws:iam::123456789012:user/Satej')
    """
    try:
        logging.info(f"Creating bucket policy for S3 bucket: {bucket_name}")
        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": user_arn},
                    "Action": "s3:*",
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                }
            ]
        }

        s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=str(bucket_policy)
        )
        logging.info("Bucket policy created successfully.")
    except NoCredentialsError:
        logging.error("AWS credentials are not properly configured.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def enable_s3_bucket_encryption(bucket_name):
    """
    Function to enable encryption for an S3 bucket.

    Parameters:
        bucket_name (str): Name of the S3 bucket.

    Example Usage:
        enable_s3_bucket_encryption('satej-data-lake')
    """
    try:
        logging.info(f"Enabling bucket encryption for S3 bucket: {bucket_name}")
        encryption_configuration = {
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                }
            ]
        }

        s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration=encryption_configuration
        )
        logging.info("Bucket encryption enabled successfully.")
    except Exception as e:
        logging.error(f"An error occurred while enabling encryption: {e}")

def main():
    """
    Main function to apply security configurations.
    """
    # Replace with actual bucket name and user ARN
    bucket_name = 'satej-data-lake'
    user_arn = 'arn:aws:iam::123456789012:user/Satej'

    create_s3_bucket_policy(bucket_name, user_arn)
    enable_s3_bucket_encryption(bucket_name)

if __name__ == "__main__":
    main()
