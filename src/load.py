import os
import json
import boto3

def save_raw_to_s3(data: dict):
    """
    Saves raw crypto API response to S3 (LocalStack or AWS).
    Configuration is taken from environment variables.
    """

    bucket_name = os.environ["S3_BUCKET"]
    endpoint_url = os.environ["S3_ENDPOINT_URL"]
    region = os.environ["AWS_DEFAULT_REGION"]

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
    )

    key = f"raw/{data['timestamp']}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Saved to S3 → {bucket_name}/{key}")