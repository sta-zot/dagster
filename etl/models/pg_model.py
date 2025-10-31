import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from time import sleep
from numpy.dtypes import DateTime64DType
from typing import Dict, List
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DataError
import yaml
from itertools import zip_longest
from etl.config import PACKAGE_ROOT

DWH_SCHEMA = PACKAGE_ROOT / "configs/schema.yaml"
MAPPING_SCHEMA = PACKAGE_ROOT / "configs/mapping.yaml"


class DWHModel:
    def __init__(
        self,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_password: str,
    ):
        self.engine = create_engine(
            f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )

        with open(DWH_SCHEMA, "r", encoding="utf-8") as f:
            self.schema = yaml.safe_load(f)
        with open(MAPPING_SCHEMA, "r", encoding="utf-8") as f:
            self.mapping = yaml.safe_load(f)
        """
            {'region': 
                    {
                        'type': 'string',
                        'db_field': 'region',
                        'db_table': 'dim_location',
                        'matches':[]
                        }
            }
        """
        self.dimensions = self.schema["dimensions"]
        self.facts = self.schema["facts"]
        self.field_mapping = {
            f'{self.mapping[df_field]["db_table"]}.{self.mapping[df_field]["db_field"]}': df_field
            for df_field in self.mapping.keys()
        }

    def load_facts(self, data: pd.DataFrame, facts: str) -> Dict:
        key_to_db_field = {
            key: data["db_field"]
            for key, data in self.mapping.items()
            if 'db_field' in data
        }
        new_columns = {
            key: key_to_db_field[key]
            for key in data.columns
            if key in key_to_db_field
        }
        data = data.rename(columns=new_columns)
        params = self.get_query_params(
            df=data,
            lookup_fields=[],
            target_fields=list(data.columns),
            query_type='insert'
        )
        
        query = f"""
            INSERT INTO {facts} (
                {params['columns']}
            ) VALUES (
                {params['placeholders']}
            )
        """
        print(query,'\n', params['params'])

    def process_dims(
        self,
        df: pd.DataFrame,
        table: str,
        lookup_fields: List[str],
        target_fields: List[str],
        key_col: str,
        batch_size: int = 1000,
    ):
        """
        функция прсматривает в таблицах базы данных наличие данных если они  есть возвращает ID
        если нет вставляет в БД и записывает ID, в указаное поле и возвращет изменённый набор данных
        Внимание поля указанные как целевые удалятся из набора "Заменяясь идентификатором
        Args:
            df (pd.DataFrame): Набор данных
            table (str): Название таблицы в БД
            lookup_fields (List[str]): Поля по которым идентифицируем запись в БД
            target_fields (List[str]): Поля которые необходимо заменить на идентификатор
            key_col (str): Название поля в БД которое будет использовано как идентификатор
            batch_size (int, optional): Размер батча для вставки данных. По умолчанию 1000.
        """
        # Проверяем наличие всех полей в словаре маппинга
        if not all(
            field in self.mapping for field in set(target_fields + lookup_fields)
        ):
            raise ValueError("Not all fields are present in the mapping dictionary.")

        slice_df = df[target_fields].copy()
        slice_df = slice_df.drop_duplicates().reset_index(drop=True)
        
        for field in lookup_fields[:]:
            if is_datetime64_any_dtype(slice_df[field]):
                print(f"Converting {field} (dtype={slice_df[field].dtype}) to int date_id")
                # Преобразуем дату в int в формате YYYYMMDD
                slice_df[field] = slice_df[field].apply(
                    lambda x:
                        int(x.strftime("%Y%m%d")) if pd.notnull(x) else None) 
                # Убираем преобразованное поле из списка целевых полей
                lookup_fields.remove(field)
        if not len(lookup_fields):
            df =  df.merge(
                        slice_df,
                        on=lookup_fields,
                        how="left",
                        suffixes=("", "_id"),
                    )
            df = df.drop(columns=target_fields)
            return df
        t_fields = list(self.mapping[field]["db_field"] for field in target_fields)
        l_fields = list(self.mapping[field]["db_field"] for field in lookup_fields)
        rename_map = dict(zip(target_fields, t_fields))
        slice_df = slice_df.rename(columns=rename_map)
        print(type(l_fields))
        for field in l_fields:
            print(f"\t -- {field} ({type(field)})")

        params = self.get_query_params(
            df= slice_df,
            lookup_fields= l_fields,
            target_fields=t_fields,
        )

        query = f"""
            SELECT {key_col}, {params["columns"]}
            FROM {table}
            WHERE ({params["columns"]}) IN ({params["placeholders"]})
        """
        # print(f"query:\n\t{query}")
        # print(f'Parametrs: \t {param_dict}')

        # Выполняем запрос
        with self.engine.connect() as conn:
            existing_rows = conn.execute(
                text(query), params["params"]
            ).fetchall()  # Должен вернуть список кортежей или словарей

        # Формируем Фрейм  из полученных полей и идентификаторов

        existing_df = pd.DataFrame(existing_rows)  # -> l_fieds + key_col

        # если нет записей в БД, то создаем пустой фрейм
        if existing_df.empty:
            existing_df = pd.DataFrame(columns=[key_col] + t_fields)
        
        
        try:
            # Определяем, какие строки отсутствуют в БД
            merged_df = slice_df.merge(existing_df, on=l_fields, how="left", indicator=True)
        except Exception as e:
            print(f"existing_df columns: \n {existing_df.columns}")
            print(f"slice_df columns: \n {slice_df.columns}")
            exit()

        # Проверяем есть не найденные ID
        new_rows = merged_df[merged_df["_merge"] == "left_only"]
        new_rows = new_rows.drop(columns=["_merge"])

        if not new_rows.empty:
            # подготавливаем параметры для запроса
            params = self.get_query_params(
                df=new_rows,
                lookup_fields=l_fields,
                target_fields=t_fields,
                query_type="insert",
            )
            query = f"""
                INSERT INTO {table} ({params['columns']})
                VALUES {params['placeholders']}
                RETURNING {key_col}, {params['columns']}
            """
            print(f"INSERT query:\n\t{query}")
            # Вставляем данные и получаем идентификаторы новых строк
            try:
                with self.engine.connect() as conn:
                    with conn.begin():
                        new_ids = conn.execute(text(query), params["params"]).fetchall()
            except DataError as e:
                print(f"Error inserting data: {e}")
                print(f"Parameters: {params["params"]}")
                raise e
            df_new_ids = pd.DataFrame(new_ids, columns=[key_col] + t_fields)
            existing_df = pd.concat([existing_df, df_new_ids], ignore_index=True)
            # Соединяем два датафрейма
        df_with_ids = df.merge(
            existing_df, right_on=l_fields, left_on=lookup_fields, how="left"
        )
        return df_with_ids.drop(columns=t_fields + target_fields)


    def get_query_params(
            self,
            df: pd.DataFrame,
            lookup_fields: List[str],
            target_fields: list[str],
            query_type: str = "select",
    ):
        # Подготовка параметров для запроса
        if query_type == "insert":
            fields = target_fields
        elif query_type == "select":
            fields = lookup_fields
        param_dict = {}
        placeholders_parts = []
        for i, (_, row) in enumerate(df.iterrows()):
            row_placeholders = []
            for j, (col) in enumerate(fields):
                param_name = f"val_{i}_{j}"
                #Преобразуем другие типы в типы python
                value = row[col]
                if hasattr(value, 'item'):
                    value = value.item()
                param_dict[param_name] =value
                row_placeholders.append(f":{param_name}")
            placeholders_parts.append(f"({', '.join(row_placeholders)})")
        placeholders = " ,".join(placeholders_parts)
        columns_str = ", ".join(fields)
        return {
            "params": param_dict,
            "placeholders": placeholders,
            "columns": columns_str
        } 

'''
    def get_ids_Dericated(
           self,
           df: pd.DataFrame,
           dimension: str,
    ):
        # Параметры из конфигурации измерения
        db_fields = self.dimensions[dimension]['natural_key_columns']
        key_col = self.dimensions[dimension]['id']
        table = dimension
        db_table_fields = [f"{table}.{field}" for field in db_fields]

        target_columns = [
            self.field_mapping[col]
            for col in db_table_fields if col in self.field_mapping]

        # Убедимся, что количество колонок совпадает
        if len(target_columns) != len(db_fields):
            raise ValueError(f"""Количество {target_columns} 
                             должно совпадать с {db_fields}""")

        # Переименуем колонки df для удобства сопоставления с БД
        df_renamed = df[target_columns].copy()
        rename_map = dict(zip(target_columns, db_fields))
        df_renamed.rename(columns=rename_map, inplace=True)

        # Удалим дубликаты, чтобы не делать лишних запросов
        df_unique = df_renamed.drop_duplicates().reset_index(drop=True)
         
        # Подготавливаем параметры для запроса как словарь
        param_dict = {}
        placeholders_parts = []

        for i, (_, row) in enumerate(df_unique.iterrows()):
            row_placeholders = []
            for j, col in enumerate(db_fields):
                param_name = f"val_{i}_{j}"
                param_dict[param_name] = row[col]
                row_placeholders.append(f":{param_name}")
            placeholders_parts.append(f"({', '.join(row_placeholders)})")

        placeholders = ", ".join(placeholders_parts)
        columns_str = ", ".join(db_fields)
        query = f"""
            SELECT {key_col}, {columns_str}
            FROM {table}
            WHERE ({columns_str}) IN ({placeholders})
        """
      
        # Выполняем запрос
        with self.engine.connect() as conn:
            existing_rows = conn.execute(text(query), param_dict).fetchall() # Должен вернуть список кортежей или словарей
        existing_df = pd.DataFrame(existing_rows)
        if existing_df.empty:
            existing_df = pd.DataFrame(columns=[key_col] + db_fields)


        # print(f"existing_df after get id's:\n {existing_df[db_fields +[key_col] ]}")
        # Определяем, какие строки отсутствуют в БД
        merged = df_unique.merge(
            existing_df,
            on=db_fields,
            how='left',
            indicator=True
        )
        missing = merged[merged['_merge'] == 'left_only'][db_fields]
        # print(f"missing:\n {existing_df[db_fields +[key_col] ]}")
        # Вставляем недостающие строки
        new_ids = []
        if not missing.empty:
            # Подготавливаем данные
            param_dict = {}
            placeholders_parts = []
            for i, (_, row) in enumerate(missing.iterrows()):
                row_placeholders = []
                for j, col in enumerate(db_fields):
                    param_name = f"val_{i}_{j}"
                    param_dict[param_name] = row[col]
                    row_placeholders.append(f":{param_name}")
                placeholders_parts.append(f"({', '.join(row_placeholders)})") 
            
            placeholders = ", ".join(placeholders_parts)
            insert_query = f"""
                INSERT INTO {table} ({', '.join(db_fields)})
                VALUES {placeholders}
                RETURNING {key_col}, {', '.join(db_fields)}
            """
            print(f"Target columns: {target_columns}")
            print(f"DB fields: {db_fields}")
            print(f"query:\n {query}")
            print(f"param_dict:\n {param_dict}")
            exit()
            with self.engine.connect() as conn:
                result = conn.execute(text(insert_query), param_dict)
                for row in result:
                    new_ids.append(tuple(row))  # (id, field1, field2, ...)
                conn.commit()
            # print(f"new_ids:\n {new_ids}")

            # Добавляем новые записи к existing_df
            new_df = pd.DataFrame(new_ids, columns=[key_col] + db_fields)
            existing_df = pd.concat([existing_df, new_df], ignore_index=True)
       
        # Теперь мержим обратно в исходный DataFrame
        df_with_keys = df.merge(
            existing_df[[key_col] + db_fields],
            left_on=target_columns,
            right_on=db_fields,
            how='left'
        )
        print(target_columns + db_fields)
        return df_with_keys.drop(columns=db_fields + target_columns, axis=1)
        # print(f"df_with_keys:\n {df_with_keys.columns}")
    
        
'''
