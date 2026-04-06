import os
import json
import boto3


def _get_s3_client():
    """
    Create S3 client (works with AWS or LocalStack).
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )


def save_raw_to_s3(data: dict) -> str:
    """
    Save raw JSON data to S3 and return object key.
    """

    s3 = _get_s3_client()
    bucket = os.environ["S3_BUCKET"]

    key = f"raw/{data['timestamp']}.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )

    print(f"Saved RAW → s3://{bucket}/{key}")

    return key


def read_raw_from_s3(key: str) -> dict:
    """
    Read raw JSON from S3.
    """

    s3 = _get_s3_client()
    bucket = os.environ["S3_BUCKET"]

    obj = s3.get_object(Bucket=bucket, Key=key)

    data = json.loads(obj["Body"].read())

    return data