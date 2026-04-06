import os
import pandas as pd
from sqlalchemy import create_engine, text


def _get_engine():
    """
    Create SQLAlchemy engine for Postgres.
    """

    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DATA_DB"]

    conn_str = f"postgresql+psycopg2://{user}:{password}@postgres:5432/{db}"

    return create_engine(conn_str)


def upsert_prices(df: pd.DataFrame) -> None:
    """
    Upsert data into final table.
    """

    engine = _get_engine()

    with engine.begin() as conn:

        # staging table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.crypto_prices_staging (
              ts TIMESTAMPTZ,
              symbol TEXT,
              price_usd NUMERIC,
              market_cap NUMERIC,
              volume_24h NUMERIC
            );
        """))

        conn.execute(text("TRUNCATE TABLE public.crypto_prices_staging;"))

        df.to_sql(
            "crypto_prices_staging",
            conn,
            schema="public",
            if_exists="append",
            index=False,
        )

        # final table (idempotent)
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

    print(f"Upserted {len(df)} rows into crypto_prices")