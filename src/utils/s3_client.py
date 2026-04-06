import os
import boto3


def get_s3_client():
    """
    Create S3 client for AWS or LocalStack.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )


def get_bucket_name() -> str:
    return os.environ["S3_BUCKET"]