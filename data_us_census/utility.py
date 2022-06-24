import pandas as pd


def url_to_df(url, prefix = None):
    import requests
    response = requests.request("GET",url)
    if response.status_code != 200:
        raise ValueError('Got status code {0} with reason {1}'.format(response.status_code, response.reason))
    rows = response.json()
    if prefix is None:
        columns = rows[0]
    else:
        columns = ["{0}_{1}".format(prefix, col_ if prefix != col_ else 'id')  for col_ in rows[0]]
    df = pd.DataFrame.from_records(rows[1:], columns=columns)
    return df

