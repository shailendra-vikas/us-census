from urllib.parse import non_hierarchical
import pandas as pd



def url_to_df(url, arguments):
    argument_str = [url,]
    for argkey, argvalue in arguments.items():
        if isinstance(argvalue, dict):
            value_str = " ".join(["{0}:{1}".format(_key,_value) for _key, _value in argvalue.items()])
        else:
            value_str = argvalue
        argument_str.append("{0}={1}".format(argkey,value_str))
    
    full_url = "&".join(argument_str)
    import requests
    response = requests.request("GET",full_url)
    if response.status_code == 204:
        return None

    if response.status_code != 200:
        raise ValueError('Got status code {0} with reason {1} "{2}"'.format(response.status_code, response.reason, response.text))
    rows = response.json()
    columns = rows[0]
    df = pd.DataFrame.from_records(rows[1:], columns=columns)
    return df