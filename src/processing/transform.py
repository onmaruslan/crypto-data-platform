import pandas as pd


def _floor_to_minute(ts: str) -> str:
    dt = pd.to_datetime(ts, utc=True)
    dt = dt.floor("min")
    return dt.isoformat()


def transform_raw_to_prices_df(raw: dict) -> pd.DataFrame:
    """
    Convert raw API payload into a normalized DataFrame.
    """

    ts = _floor_to_minute(str(raw["timestamp"]))
    items = raw.get("data", [])

    rows = []
    for item in items:
        rows.append(
            {
                "ts": ts,
                "symbol": str(item.get("symbol", "")).lower(),
                "price_usd": item.get("current_price"),
                "market_cap": item.get("market_cap"),
                "volume_24h": item.get("total_volume"),
            }
        )

    return pd.DataFrame(rows)