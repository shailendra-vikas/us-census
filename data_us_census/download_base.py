import os
import logging
import pandas as pd
import utility as util



class DownloadBase(object):
    def __init__(self, params):
        self.params = params
        self.master_data = None
        self.url = self._make_url()

        self.tag_tables_table, self.geo_labels_table, self.geo_data_table = self._load_tables()
    
    def _load_tables(self):
        #In future this should be lot more complicated
        return dict(), dict(), dict()


    def _make_url(self):
        """ Format the base part of the Url"""
        survey_str = "/".join(self.params.surveys)
        column_str = "NAME," + ",".join(self.params.data_columns)
        url = "https://api.census.gov/data/{0}/{1}?get={2}".format(self.params.data_year, survey_str, column_str)
        logging.warning("URL={0}".format(url))
        return url

    def _collect_data(self, url_argument, geo_label):
        """ Fetch data from census-url website at the geo_label level with arguments for url provided in url_argument"""
        url_argument['key'] = self.params.key_chain.get_key('census')
        if self.params.sleep is not None:
            import time
            time.sleep(self.params.sleep)
        data_df = util.url_to_df(self.url, url_argument)
        if data_df is None:
            return None

        rename_dict = {}
        for col in data_df.columns:
            col_wo_space = col.replace(" ","_") #don't use spaces in the name, use underscore instead
            if col==geo_label or ("in" in url_argument and col in url_argument['in'].keys()):
                rename_dict[col] = "{0}_id".format(col_wo_space)
            else:
                rename_dict[col] = "{0}_{1}".format(geo_label, col_wo_space)
        if self.params.restrict_rows is not None:
            return data_df.iloc[0:self.params.restrict_rows].rename(columns=rename_dict)
        
        return data_df.rename(columns=rename_dict)

    def fetch(self):
        """ Fetch the data for all level for the paramters provided in params file."""
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
        csv_filename = self.params.csv_filename()
        self.master_data.to_csv(csv_filename, index=False)
        print("Making csv file {0}".format(csv_filename))
    
    def _load_master_data(self):
        csv_filename = self.params.csv_filename()
        self.master_data = pd.read_csv(csv_filename)

    def fill(self):
        if self.master_data is None:
            self._load_master_data()
            if self.master_data is None:
                return None
        

        #The table structure would be like
        # Geo_lavels: geo_group_id(generated, One id for one geo heirarchy), level, name, GEO_ID
        # Table geo_location : location_id(generatedId), Name, census_id, geo_id

        id_related_columns = ['id','NAME']
        if 'GEO_ID' in self.params.data_columns:
            id_related_columns.append('GEO_ID')
        data_related_columns = list(set(self.params.data_columns) - set(id_related_columns))

        geo_group_id =  dict()
        geo_group_data = dict()
        for index, row in self.master_data.iterrows():
            for geo_level, geo_label in enumerate(self.params.geography_order):



        

        
        
        


def test_download():
    import params_base
    params = params_base.Params()
    download_base = DownloadBase(params)
    #download_base.fetch_and_fill()


if __name__=='__main__':
    test_download()

