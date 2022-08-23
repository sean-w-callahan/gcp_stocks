import pandas as pd
import requests
import os



def call_api():
    day = os.environ['DAY']
    #key = 'LD_O1p2lKFNN059mYVIHl8aFSUpsdPDu'
    api_key = os.environ['API_KEY']
    query = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{day}?adjusted=true&apiKey={api_key}'
    req = requests.get(query)
    response_content = req.json()
    try:
        data = response_content['results']
        rows = [list(d.values()) for d in data]
        cols = ['ticker', 'volume', 'vol_weighted_avg_price', 'open', 'close', 'high', 'low', 'unix_timestamp', 'num_transactions']
        df = pd.DataFrame(rows, columns=cols)
        df['date'] = day
        return {'req' : req, 'rows' : rows, 'df' : df}
    except:
        print('no data available')


class TestCases:
    data = call_api()
    req = data['req']
    rows = data['rows']
    df = data['df']


    def check_response_code(self):
        assert self.req.status_code == 200


    def check_content(self):
        assert len(self.rows) > 0


    def test_dtypes(self):
        dtypes = self.df.dtypes.tolist()
        expected = ['O','float64','float64', 'float64', 'float64',
        'float64', 'float64', 'float64', 'float64', 'O'
        ]
        assert dtypes == expected






