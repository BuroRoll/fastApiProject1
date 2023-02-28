from dask import dataframe as dd
from models import Passport


class PassportData:
    """
    Синглтон класс для отсортированного набора данных для поиска через Dask
    """

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(PassportData, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.dataframe = dd.read_csv('list_of_expired_passports.csv',
                                     dtype={'PASSP_SERIES': str, 'PASSP_NUMBER': str})
        self.dataframe['index'] = self.dataframe.apply(lambda r: str([r.PASSP_SERIES, r.PASSP_NUMBER]), axis=1,
                                                       meta=(None, 'object'))
        self.dataframe = self.dataframe.set_index('index')
        self.dataframe = self.dataframe.map_partitions(lambda x: x.sort_index())

    def search_passport(self, passport: Passport):
        passport_data = self.dataframe.loc[[str(passport.series) + str(passport.number)]]
        return passport_data
