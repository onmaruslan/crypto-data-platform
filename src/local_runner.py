from extract import fetch_crypto_data
from load import save_raw_to_s3
from transform import transform_raw_to_df
from load_processed import save_processed_to_s3

def run_locally():
    raw = fetch_crypto_data()
    print("RAW:", raw)

    df = transform_raw_to_df(raw)
    print("DF:", df)

    run_ts = raw["timestamp"]
    save_processed_to_s3(df, run_ts)

if __name__ == "__main__":
    run_locally()