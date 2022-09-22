
import os
import pandas as pd

class KeyChain(object):
    def __init__(self):
        self.personal_repository = "/home/shailendra_vikas/personal_repository"
        try:
            self.key_df = pd.read_csv(os.path.join(self.personal_repository,"api_key.csv"))
        except FileNotFoundError:
            self.personal_repository = "C:\\Users\\Shailendra\\Documents\\GitHub"
        self.key_df = pd.read_csv(os.path.join(self.personal_repository,"api_key.csv"))

    
    def get_key(self, source):
        key_row  = self.key_df.loc[self.key_df['source'] == source]
        return key_row['key_value'].iloc[0]


class Params(object):
    """ This class surves as template for parameter class which when derived and params value
        overwritten, govern the behaviour of the program
    """
    short_wordmap = {    'population': 'pop',    'county': 'cnty',       'county subdivision': 'csub'
                        }
    def __init__(self):
        self.keychain = KeyChain()
        self.data_year = 2019
        self.surveys = ['pep','population']
        self.geography_order = ["state","county","county subdivision"]
        self.data_columns = ['POP','DENSITY','PRIMGEOFLAG','GEO_ID']
        #self.sleep = 1.0 #in sec
        #self.restrict_rows = 3
        self.sleep = .10 #in sec
        self.restrict_rows = None

    def all_tags(self):
        all_tags = self.surveys + [str(self.data_year),] + self.geography_order
        all_tags = [tag.replace(" ","_") for tag in all_tags]
        return all_tags

    def all_tags_short(self):
        all_tags = self.surveys + [str(self.data_year),] + self.geography_order
        all_tags = [self.short_wordmap.get(tag,tag).replace(" ","_") for tag in all_tags]
        return all_tags

    def csv_filename(self):
        csv_filename = "_".join(self.all_tags()) +".csv"
        return os.path.join(self.keychain.personal_repository, "us-census", "csv_files", csv_filename)
        


