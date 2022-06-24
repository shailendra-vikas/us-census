
import requests

class MainPage(object):
    def __init__(self, url="https://api.census.gov/data.json"):
        self.url = url
        self.json = self._get_json(self.url)
        self.dataset = self.json['dataset']
        self.dataset_cond = [True,]*len(self.dataset)


    def _get_json(self, url): #TODO: Move it to utility file
        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError("Response with status code {0} with message {1}".format(response.status_code, response.reason))
        return response.json()


    def set_condition(self, data_year, survey):
        dataset_cond = list()
        survey_set = set(survey)
        for row_i in self.dataset:
            if row_i.get('c_vintage') == data_year:
                if survey is None:
                    dataset_cond.append(True)
                else:
                    row_dataset_set = set(row_i.get('c_dataset'))
                    dataset_cond.append(survey_set <= row_dataset_set)
            else:
                dataset_cond.append(False)
        self.dataset_cond = dataset_cond
        

    def _apply_condition(self):
        dataset_after_cond = [data for data, choose in zip(self.dataset, self.dataset_cond) if choose]
        #Temporary print the dataset
        for data_i in dataset_after_cond:
            print(data_i.get('c_dataset'))
        assert len(dataset_after_cond)==1, ValueError('Too many/few rows. row count={0}'.format(len(dataset_after_cond)))
        return dataset_after_cond[0]


    def show_variables(self):
        data_selected = self._apply_condition()
        import pdb; pdb.set_trace()
        variable_url = data_selected["c_variablesLink"]        
        variable_json = self._get_json(variable_url)
        return variable_json


def test_main_page():
    main_page = MainPage()
    main_page.set_condition(2019,['pep','population'])
    main_page.show_variables()
    #dataset_after_cond = main_page._apply_condition()
        

if __name__=='__main__':
    test_main_page()