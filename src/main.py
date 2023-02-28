import time

from loguru import logger
from fastapi import FastAPI
from starlette import status
from starlette.responses import JSONResponse

from models import Passport
from finder import find_password_count_by_sql, find_passport_count_by_pandas, find_passport_count_by_dask, \
    find_passport_count_by_dask2
from Passport_data import PassportData

app = FastAPI()


@app.post("/check_passport_by_sql")
async def check_passport_by_sql(passport_data: Passport):
    logger.info(f'Поступил запрос на проверку паспорта через SQL {passport_data}')
    start_time = time.time()
    passport_count = await find_password_count_by_sql(passport_data)
    time_result = time.time() - start_time
    result = {
        'status': 'found' if passport_count != 0 else 'not_found',
        'time': time_result
    }
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=result,
    )


@app.post("/check_passport_by_pandas")
async def check_passport_by_sql(passport_data: Passport):
    logger.info(f'Поступил запрос на проверку паспорта через Pandas {passport_data}')
    start_time = time.time()
    passport_count = await find_passport_count_by_pandas(passport_data)
    time_result = time.time() - start_time
    result = {
        'status': 'found' if passport_count != 0 else 'not_found',
        'time': time_result
    }
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=result,
    )


@app.post("/check_passport_by_dask")
async def check_passport_by_sql(passport_data: Passport):
    logger.info(f'Поступил запрос на проверку паспорта через Dask {passport_data}')
    start_time = time.time()
    passport_count = await find_passport_count_by_dask(passport_data)
    time_result = time.time() - start_time
    result = {
        'status': 'found' if passport_count != 0 else 'not_found',
        'time': time_result
    }
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=result,
    )


@app.post("/check_passport_by_sorted_dask")
async def check_passport_by_sorted_dask(passport_data: Passport):
    logger.info(f'Поступил запрос на проверку паспорта через отсортированный Dask {passport_data}')
    start_time = time.time()
    passport_count = await find_passport_count_by_dask2(passport_data)
    time_result = time.time() - start_time
    result = {
        'status': 'found' if passport_count != 0 else 'not_found',
        'time': time_result
    }
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=result,
    )


@app.on_event("startup")
async def startup_event():
    PassportData()
