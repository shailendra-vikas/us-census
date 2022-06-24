
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
    url = "https://api.census.gov/data/2021/pep/population?get=NAME,POP_2021,POP_2020,DESC_POP_2021&for=state:*&key={0}".format(census_api_key)
    print(url)
    response = requests.request("GET",url)
    filename_to_save = os.path.join(personal_repository,"us-census","csv_save","population_estimate_all_state.csv")
    convert_to_csv(response, filename_to_save)

def print_population2019():
    for_geography = 'division'
    higher_geography = 'REGION'
    url = """https://api.census.gov/data/2019/pep/population?get=NAME,POP,{0}&for={1}:*&key={2}""".format(higher_geography,for_geography,  census_api_key)
    #url = """https://api.census.gov/data/2019/pep/population?get=NAME,POP&for={0}:*&key={1}""".format(for_geography, higher_geography, census_api_key)
    print(url)
    response = requests.request("GET",url)
    filename_to_save = os.path.join(personal_repository,"us-census","csv_save","population_2019_{0}.csv".format(for_geography))
    convert_to_csv(response, filename_to_save)


def main2():
    print_population2021()

def main():
    #geographies = ['us','region','division','state']
    #print_population2021()
    print_population2019()
if __name__=='__main__':
    main()

