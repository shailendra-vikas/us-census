import os
import logging
import pandas as pd
import utility as util



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
        url = "https://api.census.gov/data/{0}/{1}?get={2}".format(self.params.data_year, survey_str, column_str)
        logging.warning("URL={0}".format(url))
        return url

    def _collect_data(self, url_argument, geo_label):
        url_argument['key'] = self.key_chain.get_key('census')
        if self.params.sleep is not None:
            import time
            time.sleep(self.params.sleep)
        data_df = util.url_to_df(self.url, url_argument)
        if data_df is None:
            return None

        rename_dict = {}
        for col in data_df.columns:
            col_wo_space = col.replace(" ","_")
            if col==geo_label or ("in" in url_argument and col in url_argument['in'].keys()):
                rename_dict[col] = "{0}_id".format(col_wo_space)
            else:
                rename_dict[col] = "{0}_{1}".format(geo_label, col_wo_space)
        if self.params.restrict_rows is not None:
            return data_df.iloc[0:self.params.restrict_rows].rename(columns=rename_dict)
        
        return data_df.rename(columns=rename_dict)

    def fetch_and_fill(self):
        for geo_level, geo_label in enumerate(self.params.geography_order):
            url_argument = dict()
            #url_argument['for'] = "{0}:*".format(geo_label)
            url_argument['for'] = {geo_label:"*"}

            if geo_level==0:
                # no in clause needed
                self.master_data = self._collect_data(url_argument, geo_label)
            else:
                cols_in_inclause = self.params.geography_order[:geo_level]
                list_current_level_data =[]
                for index, row in self.master_data.iterrows():
                    copy_url_argument = url_argument.copy()
                    copy_url_argument['in'] = dict([(col, row["{0}_id".format(col)]) for col in cols_in_inclause])
                    current_level_data = self._collect_data(copy_url_argument, geo_label)
                    if current_level_data is not None:
                        list_current_level_data.append(current_level_data)

                if list_current_level_data:
                    all_current_level_df = pd.concat(list_current_level_data)
                    join_columns = list(set(self.master_data.columns) & set(all_current_level_df.columns))
                    self.master_data = self.master_data.merge(all_current_level_df, on=join_columns, how="left")
        
        #Now accumulated data. Make tables to insert data
        all_tags = self.params.surveys + self.params.geography_order
        all_tags = [tag.replace(" ","_") for tag in all_tags]
        csv_filename = "_".join(all_tags) +".csv"
        self.master_data.to_csv(csv_filename, index=False)
        print("Making csv file {0}".format(csv_filename))
                

def test_download():
    import params_base
    params = params_base.Params()
    download_base = DownloadBase(params)
    download_base.fetch_and_fill()


if __name__=='__main__':
    test_download()

