
import os
import requests
import pandas as pd

personal_repository = "/home/shailendra_vikas/personal_repository"

def get_census_key():
    df = pd.read_csv(os.path.join(personal_repository,"api_key.csv"))
    return df.loc[df['source'] == 'census']['key_value'].iloc[0]
census_api_key = get_census_key()


def convert_to_csv(response, filename):
    if response.status_code != 200:
        raise ValueError('Got status code {0} with reason {1}'.format(response.status_code, response.reason))
    
    rows = response.json()
    df = pd.DataFrame.from_records(rows[1:], columns=rows[0])
    df.to_csv(filename,index=False)
    print("Saving file {0}".format(filename))


def print_population2021():
    url1 = "https://api.census.gov/data/2021/pep/population?get=NAME,POP_2021&for=county:*&in=state:12&key={0}".format(census_api_key)
    url = """https://api.census.gov/data/2019/pep/population?get=NAME,POP&for=county%20subdivision:*&in=state:12%20county:011&key={0}""".format(census_api_key)
    url = """https://api.census.gov/data/2019/pep/population?get=NAME,POP&for=county%20subdivision:*&in=state:09%20county:2&key={0}""".format(census_api_key)
    print(url)
    response = requests.request("GET",url)
    filename_to_save = os.path.join(personal_repository,"us-census","csv_save","population_estimate_broward_.csv")
    convert_to_csv(response, filename_to_save)

def print_population2019():
    state = ""
    county =""


def main():
    print_population2021()

def main2():
    geographies = ['us','region','division','state']
    geographies = ['division','state']
    for geography in geographies:
        print_population2021()

if __name__=='__main__':
    main()

