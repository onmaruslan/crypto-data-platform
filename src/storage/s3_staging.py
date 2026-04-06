import io
import pandas as pd
from utils.s3_client import get_s3_client, get_bucket_name


def save_parquet_to_s3(df: pd.DataFrame, run_ts: str) -> str:
    """
    Save transformed DataFrame as Parquet to S3 (staging layer).
    """

    s3 = get_s3_client()
    bucket = get_bucket_name()

    key = f"staging/crypto_{run_ts}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"Saved STAGING → s3://{bucket}/{key}")

    return key


def read_parquet_from_s3(key: str) -> pd.DataFrame:
    """
    Read Parquet file from S3.
    """

    s3 = get_s3_client()
    bucket = get_bucket_name()

    obj = s3.get_object(Bucket=bucket, Key=key)

    return pd.read_parquet(io.BytesIO(obj["Body"].read()))