from typing import Dict
from pandas import DataFrame
from sqlalchemy.engine import Engine
import difflib
import pandas as pd
from datetime import date, datetime, timedelta
from dateutil import parser

def revert_dict(dictionary: Dict) -> Dict:
    """
    Reverts a dictionary by swapping its keys and values.
    Input dict has like structure:

    >>> {
    >>>     "key": [values]
    >>> }

    Args:
    dict (dict): The input dictionary to be reverted.

    Returns:
    dict: A new dictionary with keys and values swapped.

    Raises:
    TypeError: If any value in the input dictionary is not hashable.
    ValueError: If 

    Notes:
    - If the input dictionary is empty, an empty dictionary is returned.
    - If there are duplicate values in the input dictionary, the reverted dictionary
      will contain only the last key-value pair for that value.
    """
    if not dictionary:
        raise ValueError("Input dictionary is empty.")
    if not isinstance(dictionary, dict):
        raise TypeError("Input must be a dictionary.")
    try:
        mapping = {
            raw_name: std_name
            for std_name, raw_names in dictionary.items()
            for raw_name in raw_names
        }
        return mapping
    except TypeError as e:
        raise TypeError("All values in the input dictionary must be hashable.") from e
    

class Mapping():
    def __init__(self, mapping: Dict, threshold: float = 0.8) -> None:
        '''
            Класс принимает словарь где ключом  является стандартное имя поля,
            а значением ключа список названий которые надо заенить 
            >>> {
            >>>     standard_field_name: [variable field names ]
            >>> }
            или словарь где ключами являются различные названия полей,
            а значением является стандартное имя поля
            >>> {
            >>>     variable_field_name1 : standard_field_name,
            >>>     variable_field_name2 : standard_field_name,
            >>>     # e.t.c.
            >>> }
            Args:
                mapping (Dict): Словарь для замены различных названий полей на стандартное
                threshold (float): Уровень совпадения строки в случае если в заголовках 
                названий полей имеются опечатки
        '''
        if not mapping:
            raise ValueError("Mapping is empty.")
        if not isinstance(mapping, dict):
            raise TypeError("Mapping must be a dictionary.")
        if any(isinstance(v, (list, tuple)) for v in mapping.values()) :
            self.mapping = revert_dict(mapping)
        else:
            self.mapping = mapping
        self.threshold = threshold
    
    def get(self, key: str) -> str:
        """
        Retrieves the value associated with the given key from the mapping.
        If the key is not found, returns the key itself.

        Args:
            key (str): The key to look for in the mapping.

        Returns:
            str: The value associated with the key if found, otherwise the key itself.
        """
        if key in self.mapping:
            return self.mapping[key]
        matches = difflib.get_close_matches(key, self.mapping.keys(), n=1, cutoff=self.threshold)
        if matches:
            return self.mapping[matches[0]]
        else:
            print(self.mapping)
            raise KeyError(f"Key '{key}' not found in mapping.")
        
        
def create_date_dim(
        start_date: str,
        end_date: str
) -> DataFrame:
    """
    Creates a date dimension table for the given date range.

    Args:
        start_date (str): The start date in the format 'YYYY-MM-DD'.
        end_date (str): The end date in the format 'YYYY-MM-DD'.

    Returns:
        pandas.DataFrame: A DataFrame containing the date dimension table.

    Генерирует датафрейм таблицы измерений времени (date dimension).
    
    :param start_date: начало периода (в формате 'YYYY-MM-DD')
    :param end_date: конец периода (в формате 'YYYY-MM-DD')
    :return: pandas.DataFrame со столбцами:
        date_id, date, year, month, day_of_month, day_of_week, quarter

    """
    # Convert start and end dates to datetime objects
    # Create a list of dates within the range
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    # Create a DataFrame with the date
    df = pd.DataFrame({
        "date_id": dates.strftime("%Y%m%d").astype(int),  # surrogate key
        "date": dates,
        "year": dates.year,
        "month": dates.month,
        "day_of_month": dates.day,
        "day_of_week": dates.dayofweek + 1,  # 1=Пн ... 7=Вс
        "quarter": dates.quarter
    })

    return df

import datetime
import random

def get_random_date(start_date, end_date):
    """
    Возвращает случайную дату (datetime.date) из заданного диапазона.
    """
    if start_date > end_date:
        # Убедимся, что начальная дата не позже конечной
        start_date, end_date = end_date, start_date
        
    # 1. Вычисляем разницу в днях между датами
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    
    # 2. Генерируем случайное количество дней (включая end_date)
    random_number_of_days = random.randrange(days_between_dates + 1)
    
    # 3. Прибавляем случайное количество дней к начальной дате
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    result_date = int(random_date.strftime('%Y%m%d'))
    return result_date



def load_location_to_db(engine: Engine)-> None:
    locations = pd.read_csv("etl\\data\\locations.csv")
    locations = locations.drop(columns=["id"])
    locations.to_sql("dim_location", engine, if_exists="append", index=False)

def load_date_dim_to_db(engine: Engine)-> None:
    start_date = "2020, 1, 1"
    end_date = "2030, 12, 31"
    date_dim = create_date_dim(start_date, end_date)
    date_dim.to_sql("dim_date", engine, if_exists="append", index=False)