import pandas as pd
from memory_profiler import profile
from sqlalchemy import create_engine, text

from dask import dataframe as dd
from models import Passport
from Passport_data import PassportData


# @profile
async def find_password_count_by_sql(passport: Passport) -> int:
    """
        Поиск паспорта в базе данных
        :param Passport passport:
        :return количество найденных записей:
    """
    engine = create_engine('postgresql://admin:123456@localhost:5432/my_database')
    with engine.connect() as con:
        sql = text(
            f"SELECT * FROM passport p "
            f"WHERE p.\"PASSP_SERIES\" = '{passport.series}' "
            f"and   p.\"PASSP_NUMBER\" = '{passport.number}'")
        result = con.execute(sql).fetchall()
        return len(result)


# @profile
async def find_passport_count_by_pandas(passport: Passport) -> int:
    """
        Поиск паспорта с помощью pandas
        :param Passport passport:
        :return количество найденных записей:
    """
    for chunk in pd.read_csv('list_of_expired_passports.csv',
                             dtype={'PASSP_SERIES': str, 'PASSP_NUMBER': str},
                             chunksize=100_000):
        data = chunk[(chunk["PASSP_SERIES"] == str(passport.series)) &
                     (chunk["PASSP_NUMBER"] == str(passport.number))]
        if len(data) != 0:
            return len(data)
    return 0


async def find_passport_count_by_dask(passport: Passport) -> int:
    """
        Поиск паспорта с помощью dask
        :param Passport passport:
        :return количество найденных записей:
    """
    ddf = dd.read_csv('list_of_expired_passports.csv',
                      dtype={'PASSP_SERIES': str, 'PASSP_NUMBER': str})
    ddf_selected = ddf[(ddf['PASSP_SERIES'] == str(passport.series)) &
                       (ddf["PASSP_NUMBER"] == str(passport.number))]
    return len(ddf_selected)


async def find_passport_count_by_dask2(passport: Passport) -> int:
    """
        Поиск паспорта с помощью отсортированного синглтон класса через dask
        :param Passport passport:
        :return количество найденных записей:
    """
    passport_data = PassportData()
    passport_info = passport_data.search_passport(passport)
    return len(passport_info)
