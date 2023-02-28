from pydantic import BaseModel, validator
import re
from loguru import logger


class Passport(BaseModel):
    series: int
    number: int

    @validator("series", pre=True)
    def validate_series(cls, value: int):
        if re.fullmatch(r'[0-9]{4}', str(value)):
            return value
        logger.info(f'Неправильный формат серии паспорта {value}')
        raise ValueError('Неверный формат серии паспорта')

    @validator("number", pre=True)
    def validate_number(cls, value: int):
        if re.fullmatch(r'[0-9]{6}', str(value)):
            return value
        logger.info(f'Неправильный формат номера паспорта {value}')
        raise ValueError('Неверный формат номера паспорта')
