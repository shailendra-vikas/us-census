import requests
import pandas as pd


df = pd.read_csv('../api_key.csv')
census_api_key = df.loc[df['source'] == 'census']['key_value'].iloc[0]

print(census_api_key)


def print_population(geography):
    url = "https://api.census.gov/data/2021/pep/population?get=NAME,POP_2021&for={0}:*&key={1}".format(geography,census_api_key)
    response = requests.request("GET",url)
    print(response)
    print(response.text)



def main():
    geographies = ['us','region','division','state']
    for geography in geographies:
        print_population(geography)

if __name__=='__main__':
    main()

