

class Params(object):
    """ This class surves as template for parameter class which when derived and params value
        overwritten, govern the behaviour of the program
    """
    def __init__(self):
        self.data_year = 2019
        self.surveys = ['pep','population']
        self.geography_order = ["state","county","county subdivision"]
        self.data_columns = ['POP','DENSITY','PRIMGEOFLAG','GEO_ID']
        #self.sleep = 1.0 #in sec
        #self.restrict_rows = 3
        self.sleep = .10 #in sec
        self.restrict_rows = None

        


