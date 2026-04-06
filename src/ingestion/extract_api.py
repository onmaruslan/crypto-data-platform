import requests
from datetime import datetime


def fetch_crypto_data() -> dict:
    """
    Fetch crypto data from public API (CoinGecko).
    """

    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "ids": "bitcoin,ethereum",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "data": data,
    }