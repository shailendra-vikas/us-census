import os
import logging
import numpy as np
import pandas as pd
import logging
import download_utility as dutil


class DataCollection(object):
    """ This class is to fill the data in the format of table as it is design.
        It generates location_id and parent_id for each rows.
    """
     # Key-> Column : Value -> (type, nullable )
    column_properties = { 'location_id' : (int, 'int(11)', False),              'id' : (int, 'int(11)', False),          'NAME' : (str, 'varchar(200)', False),
                               'GEO_ID' : (str, 'varchar(20)', False) ,  'parent_id' : (int, 'int(11)', True),    'PRIMGEOFLAG' : (int, 'int(2)', True),
                                  'POP' : (int, 'int(11)', False),         'DENSITY' : (float, 'float', True)
        }  
    def __init__(self, params):
        self.params = params

        self.id_related_columns = ['id','NAME']
        if 'GEO_ID' in self.params.data_columns:
            self.id_related_columns.append('GEO_ID')
        self.data_related_columns = list(set(self.params.data_columns) - set(self.id_related_columns))

        self.next_location_id = 0
        self.geo_group_id =  dict()
        self.geo_group_data = dict()


    def add_location_id(self, key_tuple, parent_id):
        self.geo_group_id[key_tuple] = (self.next_location_id, parent_id)
        current_location_id = self.next_location_id
        self.next_location_id += 1
        return current_location_id


    def add_geo_group_data(self, location_id, geo_data):
        self.geo_group_data[location_id] = geo_data


    def add(self, row):
        """  Adding a row to the dataset
        """
        for geo_level, geo_label in enumerate(self.params.geography_order):
            id_primary = (*row[["{0}_{1}".format(geo_label,_col) for _col in self.id_related_columns]],)
            if all(map(np.isnan, id_primary)):
                continue

            location_id_and_parent_id = self.geo_group_id.get(id_primary)
            if location_id_and_parent_id is not None:
                location_id = location_id_and_parent_id[0]
            else:
                parent_level = geo_level - 1
                if parent_level<0:
                    parent_id = None
                else:
                    parent_primary = (*row[["{0}_{1}".format(self.params.geography_order[parent_level],_col) for _col in self.id_related_columns]],)
                    parent_id, junk = self.geo_group_id[parent_primary]
                location_id = self.add_location_id(id_primary, parent_id)   
                    
            #Added to the geo_group_data
            geo_data = (*row[["{0}_{1}".format(geo_label,_col) for _col in self.data_related_columns]],)
            self.add_geo_group_data(location_id, geo_data)

    
    def _to_df(self):
        id_columns = ['location_id',] + self.id_related_columns + ['parent_id',]
        id_table = []
        for key, values in self.geo_group_id.items():
            row = (values[0], *key, values[1])
            id_table.append(row)
        id_table_df = pd.DataFrame.from_records(id_table, columns = id_columns)
        
        data_columns = ['location_id'] + self.data_related_columns
        data_table = []
        for location_id, values in self.geo_group_data.items():
            row = (location_id, *values)
            data_table.append(row)
        data_table_df = pd.DataFrame.from_records(data_table, columns = data_columns)

        return id_table_df, data_table_df


    def save_as_file(self):
        id_table_df, data_table_df = self._to_df()
        id_table_df.to_csv('id_table.csv', index=False)
        data_table_df.to_csv('data_table.csv', index=False)


    def _create_table_and_fill(self, tablename, table_df):
        import sql_connect.mysql_connect as mysql_connect
        table_column = []
        for col in table_df.columns:
            col_type, col_mysql_type, nullable = self.column_properties[col]
            table_column.append(" `{0}` {1} {2} NULL ".format(col, col_mysql_type, "" if nullable else "NOT" ))

        mysql = mysql_connect.MySQL()
        create_qry = " CREATE TABLE IF NOT EXISTS `{0}` ({1})".format(tablename, ",".join(table_column))
        #create_qry = " DROP TABLE IF EXISTS `{0}`; CREATE TABLE `{1}` ({2})".format(tablename, tablename, ",".join(table_column))
        mysql.execute(create_qry)
        logging.debug(create_qry)

        all_rows = []
        insert_qry =  """ INSERT INTO {0} VALUES ({1})""".format(tablename,",".join(['%s']*len(table_df.columns)))
        for index, row in table_df.iterrows():
            row_values = []
            for col in table_df.columns:
                col_type, col_mysql_type, nullable = self.column_properties[col]
                if col_type == str:
                    #row_values.append("{}".format(col_type(row[col])))
                    row_values.append(col_type(row[col]))
                elif np.isnan(row[col]):
                    row_values.append(None)
                else:
                    #row_values.append("{}".format(col_type(row[col])))
                    row_values.append(col_type(row[col]))
            all_rows.append(tuple(row_values))

            if index%100 == 1:
                mysql.insertmany(insert_qry, all_rows)
                all_rows = []
        mysql.insertmany(insert_qry, all_rows)
        


    def save_in_table(self):
        id_table_df, data_table_df = self._to_df()
        
        # First create table
        table_prefix = "_".join(self.params.all_tags_short())
        id_table_name = "{0}_idinfo".format(table_prefix)
        data_table_name = "{0}_datainfo".format(table_prefix)

        self._create_table_and_fill(id_table_name, id_table_df)
        self._create_table_and_fill(data_table_name, data_table_df)


class DownloadBase(object):
    def __init__(self, params):
        self.params = params
        self.master_data = None
        self.data_collection = DataCollection(self.params)
        self.url = self._make_url()

        self.tag_tables_table, self.geo_labels_table, self.geo_data_table = self._load_tables()


    def _load_tables(self):
        #In future this should be lot more complicated
        return dict(), dict(), dict()


    def _make_url(self):
        """ Format the base part of the Url."""
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
        data_df = dutil.url_to_df(self.url, url_argument)
        if data_df is None:
            return None

        rename_dict = {}
        for col in data_df.columns:
            if col==geo_label or ("in" in url_argument and col in url_argument['in'].keys()):
                rename_dict[col] = "{0}_id".format(col)
            else:
                rename_dict[col] = "{0}_{1}".format(geo_label, col)

        if self.params.restrict_rows is not None:
            return data_df.iloc[0:self.params.restrict_rows].rename(columns=rename_dict)
        
        return data_df.rename(columns=rename_dict)


    def fetch(self):
        """ Fetch the data for all level for the paramters provided in params file."""
        for geo_level, geo_label in enumerate(self.params.geography_order):
            url_argument = dict()
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
        logging.info("Making csv file {}".format(csv_filename))
    

    def _load_master_data(self):
        csv_filename = self.params.csv_filename()
        logging.info("Loading {}".format(csv_filename))
        self.master_data = pd.read_csv(csv_filename)

    def fill(self, save_csv=False, save_table=True ):
        if self.master_data is None:
            self._load_master_data()
            if self.master_data is None:
                return None
        
        for index, row in self.master_data.iterrows():
            self.data_collection.add(row)
        
        if save_csv:
            self.data_collection.save_as_file()

        if save_table:
            self.data_collection.save_in_table()
        
            

def test_download():
    import params_base
    params = params_base.Params()
    download_base = DownloadBase(params)
    download_base.fill()
    pass
    


if __name__=='__main__':
    test_download()

