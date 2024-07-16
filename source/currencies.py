import requests

def fetch_currency_data():
    url = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies.json"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None
