import pandas as pd
import utility as util



class KeyChain(object):
    def __init__(self):
        self.personal_repository = "/home/shailendra_vikas/personal_repository"
        self.key_df = pd.read_csv(os.path.join(self.personal_repository,"api_key.csv"))
    
    def get_key(self, source):
        key_row  = df.loc[df['source'] == source]
        return key_row['key_value'].iloc[0]


class DownloadBase(object):
    def __init__(self, params):
        self.params = params
        self.key_chain = KeyChain()
        #self.collected_geography =  dict()
        self.master_data = None
        self.url = self._make_url()
        
    def _make_url(self):
        survey_str = "/".join(self.params.surveys)
        column_str = "NAME," + ",".join(self.params.data_columns)
        url = "https://api.census.gov/data/{0}?get={1}".format(survey_str,column_str)
        return url

    def _collect_data(self, url_argument, prefix=None):
        url_argument['key'] = self.key_chain.get_key('census')
        url_with_arguments = self.url + "".join(["&{0}={1}".format(name, value) for name, value in url_argument.items()])
        data_df = util.url_to_df(url_with_arguments, prefix=prefix)
        return data_df
    
    def fetch_and_fill(self):
        for geo_level, geo_label in enumerate(self.params.geography_order):
            url_argument = dict()
            url_argument['for'] = "{0}:*".format(geo_level)
            if geo_level==0:
                self.master_data = self._collect_data(url_argument, prefix = geo_label)
            else:
                self.master_data[["{0}_id"%col_ for col_ in self.params.geography_order[:geo_level]]]                
                #fetch from last_geo_level and fill
                pass
            last_geo_level = geo_level
            
                



