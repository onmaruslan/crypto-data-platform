import os
import io
import boto3
import pandas as pd

def save_processed_to_s3(df: pd.DataFrame, run_ts: str):
    bucket = os.environ["S3_BUCKET"]
    endpoint_url = os.environ["S3_ENDPOINT_URL"]
    region = os.environ["AWS_DEFAULT_REGION"]

    s3 = boto3.client("s3", endpoint_url=endpoint_url, region_name=region)

    key = f"processed/crypto_{run_ts}.parquet"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"Saved processed to S3 → {bucket}/{key}")