import os
import pandas as pd
from sqlalchemy import create_engine, text


def upsert_prices_to_postgres(df: pd.DataFrame) -> None:
    """
    Upserts rows into public.crypto_prices in the data database (POSTGRES_DATA_DB).

    Expected DataFrame columns:
      - ts
      - symbol
      - price_usd
      - market_cap
      - volume_24h
    """

    pg_user = os.environ["POSTGRES_USER"]
    pg_pass = os.environ["POSTGRES_PASSWORD"]
    pg_db = os.environ["POSTGRES_DATA_DB"]

    # Inside the Docker network, the Postgres service is reachable via hostname "postgres"
    conn_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@postgres:5432/{pg_db}"
    engine = create_engine(conn_str)

    with engine.begin() as conn:
        # Ensure target table exists in the data database
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.crypto_prices (
              ts TIMESTAMPTZ NOT NULL,
              symbol TEXT NOT NULL,
              price_usd NUMERIC,
              market_cap NUMERIC,
              volume_24h NUMERIC,
              PRIMARY KEY (ts, symbol)
            );
        """))

        # Ensure staging table exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.crypto_prices_staging (
              ts TIMESTAMPTZ NOT NULL,
              symbol TEXT NOT NULL,
              price_usd NUMERIC,
              market_cap NUMERIC,
              volume_24h NUMERIC
            );
        """))

        # Clear staging before loading a new batch
        conn.execute(text("TRUNCATE TABLE public.crypto_prices_staging;"))

        df.to_sql("crypto_prices_staging", conn, schema="public", if_exists="append", index=False)

        # Upsert into the target table
        conn.execute(text("""
            INSERT INTO public.crypto_prices (ts, symbol, price_usd, market_cap, volume_24h)
            SELECT ts, symbol, price_usd, market_cap, volume_24h
            FROM public.crypto_prices_staging
            ON CONFLICT (ts, symbol)
            DO UPDATE SET
              price_usd = EXCLUDED.price_usd,
              market_cap = EXCLUDED.market_cap,
              volume_24h = EXCLUDED.volume_24h;
        """))

    print(f"Upserted {len(df)} rows into {pg_db}.public.crypto_prices")