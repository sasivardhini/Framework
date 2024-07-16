import requests

def fetch_binance_data():
    url = "https://api4.binance.com/api/v3/ticker/24hr"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None
