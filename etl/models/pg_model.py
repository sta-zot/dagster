import pandas as pd
import re
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

    def load_facts(self, data: pd.DataFrame,
                   facts: str,
                   lookup_fields: List[str]
    ) -> Dict:
        key_to_db_field = {
            key: value["db_field"]
            for key, value in self.mapping.items()
            if key in data.columns
        }
        #print(key_to_db_field)
        # for key, val in self.mapping.items():
        #     print(key, " : ", val["db_field"])

        new_columns = {
            key: key_to_db_field[key]
            for key in data.columns
            if key in key_to_db_field
        }
        _lookup_fields = {
            key:  key_to_db_field[key]
            for key in lookup_fields
            if key in key_to_db_field
        }
        # print('key_to_db_field : \t', key_to_db_field) 
        # print('new_columns : \t',new_columns)
        data = data.rename(columns=new_columns)
        lookup_params = self.get_query_params(
            df=data,
            lookup_fields=_lookup_fields,
            target_fields=[],
            query_type='select'
        )
        # print("lookup_params: \n", lookup_params)
        query = f"""
            SELECT {self.facts[facts]['id']}, { lookup_params["columns"]}
            FROM {facts}
            WHERE {lookup_params['placegolders']}
        """

        with self.engine.connect() as conn:
           result =  conn.execute(text(query), lookup_params['params']).fetchall()
        existing_df = pd.DataFrame(result, columns=[self.facts[facts]['id']] + list(_lookup_fields.values()))
        if existing_df.empty:
            existing_df = pd.DataFrame(columns=[self.facts[facts]['id']] + _lookup_fields.values())
        try:
            # Определяем, какие строки отсутствуют в БД
            merged_df = data.merge(existing_df, on=_lookup_fields, how="left", indicator=True)
        except Exception as e:
            print(f"existing_df columns: \n {existing_df.columns}")
            print(f"slice_df columns: \n {data.columns}")
            print(f"Error: {e}")
            exit()

        # Проверяем есть не найденные ID
        new_rows = merged_df[merged_df["_merge"] == "left_only"]
        new_rows = new_rows.drop(columns=["_merge"])
        # Если все записи существует в БД то возвращаем их
        if new_rows.empty:
            return merged_df.drop(columns=["_merge"])
        params = self.get_query_params(
            df=new_rows,
            lookup_fields=[],
            target_fields=list(new_columns.values()),
            query_type='insert'
        )

        query = f"""
            INSERT INTO {facts} ({params['columns']})
            VALUES {params['placeholders']}
            RETURNING {self.facts[facts]['id']}, {params['columns']}
        """
        # print("query: \n", query )
        # print("params: \n", params['params'])

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params['params']).fetchall()
            conn.commit()
        result_df = pd.DataFrame(
            result,
            columns=[self.facts[facts]['id']] + list(new_columns.values()))
        return df

    def process_dims(
        self,
        df: pd.DataFrame,
        table: str,
        lookup_fields: List[str],
        target_fields: List[str],
        key_col: str,
        custom_col: str = '',
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
        merge_fields = lookup_fields.copy()
        for field in lookup_fields[:]:
            if is_datetime64_any_dtype(slice_df[field]):
                print(f"Converting {field} (dtype={slice_df[field].dtype}) to int date_id")
                # Преобразуем дату в int в формате YYYYMMDD
                # slice_df[field] = slice_df[field].apply(
                #     lambda x:
                #         int(x.strftime("%Y%m%d")) if pd.notnull(x) else None)
                slice_df[field] = slice_df[field].dt.strftime("%Y%m%d").astype(int) 
                # Убираем преобразованное поле из списка целевых полей
                lookup_fields.remove(field)
        if not len(lookup_fields):
            df = df.merge(
                        slice_df,
                        on=merge_fields,
                        how="left",
                        suffixes=("", "_id"),
                    )
            df = df.drop(columns=target_fields)
            return df
        t_fields = list(self.mapping[field]["db_field"] for field in target_fields)
        l_fields = list(self.mapping[field]["db_field"] for field in lookup_fields)
        rename_map = dict(zip(target_fields, t_fields))
        slice_df = slice_df.rename(columns=rename_map)
        #print(type(l_fields))
        #for field in l_fields:
            #print(f"\t -- {field} ({type(field)})")

        params = self.get_query_params(
            df=slice_df,
            lookup_fields=l_fields,
            target_fields=t_fields,
        )

        query = f"""
            SELECT {key_col}, {params["columns"]}
            FROM {table}
            WHERE ({params["columns"]}) IN ({params["placeholders"]})
        """
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
            print(f"Error: {e}")
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
            #print(f"INSERT query:\n\t{query}")
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
        df = df_with_ids.drop(columns=t_fields + target_fields)
        if custom_col:
            df = df.rename(columns={key_col: custom_col})
        return df

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

    def load_events_fact(self, df: pd.DataFrame):
        p_df = self.process_dims(
            df=df,
            table="dim_location",
            lookup_fields=["settlement", "municipality", "region"],
            target_fields=["settlement", "municipality", "region"],
            key_col="location_id",
        )
        p_df = self.process_dims(
            df=p_df,
            table="dim_audience",
            lookup_fields=["age_group", "social_group"],
            target_fields=["age_group", "social_group"],
            key_col="audience_id",
        )
        p_df = self.process_dims(
            df=p_df,
            table="dim_event",
            lookup_fields=["event_type", "event_format",  "event_topic"],
            target_fields=["event_type", "event_format",  "event_topic"],
            key_col="event_id",
        )
        p_df = self.process_dims(
            df=p_df,
            table="dim_staff",
            lookup_fields=["organizer_name", "department", "personInCharge"],
            target_fields=["organizer_name", "department", "personInCharge"],
            key_col="staff_id",
            custom_col="organizer_id",
        )
        p_df = self.process_dims(
            df=p_df,
            table="dim_partner",
            lookup_fields=["partner_name", "partner_type"],
            target_fields=["partner_name", "partner_type"],
            key_col="partner_id",
        )
        # p_df = self.process_dims(
        #     df=p_df,
        #     table="dim_date",
        #     lookup_fields=["date"],
        #     target_fields=["date"],
        #     key_col="date_id",
        # )
        p_df['date'] = p_df['date'].apply(
            lambda field:
                int(field.strftime("%Y%m%d")) if pd.notnull(field) else None
        )

        # Вставка волонтёров 
        volunteers = []
        excluded_cols = ['volunteers', 'volunteers_type']
        fact_cols = [col for col in p_df.columns if col not in excluded_cols]
        volunteers_key_lookup_cols = [
            col for col in p_df.columns if col not in excluded_cols
            ]    
        for _, row in p_df.iterrows():
            _volunteers = row['volunteers'].split(', ')
            
            for volunteer in _volunteers:
                record= {
                        "name": re.sub(r'[^а-яА-ЯёЁa-zA-Z\s]','', volunteer).strip(),
                        "type":  re.sub(r'[^а-яА-ЯёЁa-zA-Z\s]','', row['volunteers_type']).strip(),
                    }
                for col in volunteers_key_lookup_cols:
                    record[col] = row[col]
                volunteers.append(record)
        df_volunteers = pd.DataFrame(volunteers)
        fact_df = p_df[fact_cols]
        print(fact_df.columns)
        print(df_volunteers.columns)
        print("#########################################\n\n\n\n")

        fact_id = self.load_facts(fact_df, "fact_events")
        print(fact_id)
