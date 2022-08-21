import requests
import pandas as pd
from google.cloud import storage


def get_prices(**kwargs):
    api_key = kwargs['api_key']
    yesterday = kwargs['yesterday']
    query = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{yesterday}?adjusted=true&apiKey={api_key}'

    req = requests.get(query)
    response_content = req.json()

    try:
        data = response_content['results']
        rows = [list(d.values()) for d in data]
        cols = ['ticker', 'volume', 'vol_weighted_avg_price', 'open', 'close', 'high', 'low', 'unix_timestamp', 'num_transactions']
        df = pd.DataFrame(rows, columns=cols)
        df['datetime'] = yesterday
        client = storage.Client()
        bucket = client.bucket('stocks-data-output')
        blob = bucket.blob(f'prices_{yesterday}.csv')
        blob.upload_from_string(df.to_csv(index=False, header=False))
    except:
        print('no available data')