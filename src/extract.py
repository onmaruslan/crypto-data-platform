import requests

from datetime import datetime

def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": "bitcoin,ethereum",
    }

    response = requests.get(url, params=params)
    data = response.json()

    timestamp = datetime.utcnow().isoformat()

    return {"timestamp": timestamp, "data": data}


