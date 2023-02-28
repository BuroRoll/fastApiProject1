import pandas as pd
from sqlalchemy import create_engine


def transport_from_file_to_database():
    """
    Вспомогательный метод для переноса данных из файла в базу данных
    """
    engine = create_engine('postgresql://admin:123456@localhost:5432/my_database')
    for chunk in pd.read_csv('list_of_expired_passports.csv',
                             dtype={'PASSP_SERIES': str, 'PASSP_NUMBER': str},
                             chunksize=100_000):
        chunk.to_sql('passport', engine, if_exists='append')
