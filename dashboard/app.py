import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

st.set_page_config(page_title="Crypto Data Platform", layout="wide")

st.title("Real-Time Crypto Data Platform")
st.caption(
    "Data source: CoinGecko • Storage: S3 (LocalStack) • Orchestration: Airflow • Warehouse: PostgreSQL"
)

pg_user = os.environ.get("POSTGRES_USER", "airflow")
pg_pass = os.environ.get("POSTGRES_PASSWORD", "airflow")
pg_db = os.environ.get("POSTGRES_DATA_DB", "crypto")

# When running inside Docker Compose, Postgres is reachable via the service name "postgres"
conn_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@postgres:5432/{pg_db}"
engine = create_engine(conn_str)

symbols = ["btc", "eth"]
symbol = st.selectbox("Symbol", symbols, index=0)

query = """
SELECT ts, price_usd
FROM public.crypto_prices
WHERE symbol = %(symbol)s
ORDER BY ts DESC
LIMIT 500
"""

df = pd.read_sql(query, engine, params={"symbol": symbol})
df = df.sort_values("ts")

if df.empty:
    st.warning("No data yet. Trigger the Airflow DAG and refresh this page.")
    st.stop()

start_price = float(df["price_usd"].iloc[0])
df["delta_usd"] = df["price_usd"] - start_price
df["pct_change"] = (df["price_usd"] / start_price - 1.0) * 100

st.subheader(f"Price (USD): {symbol.upper()}")
st.line_chart(df.set_index("ts")["price_usd"])

st.subheader("Latest rows")
st.dataframe(df.tail(20), use_container_width=True)