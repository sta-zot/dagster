import pandas as pd
from typing import Dict, List
from sqlalchemy import create_engine, text
import yaml
from itertools import zip_longest
from etl.config import PACKAGE_ROOT
DWH_SCHEMA = PACKAGE_ROOT / 'configs/schema.yaml'
MAPPING_SCHEMA = PACKAGE_ROOT / 'configs/mapping.yaml'

class DWHModel():
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
       
        with open(DWH_SCHEMA, 'r', encoding="utf-8") as f:
            self.schema = yaml.safe_load(f)
        with open(MAPPING_SCHEMA, 'r', encoding="utf-8") as f:
            self.mapping = yaml.safe_load(f)
        self.dimensions = self.schema['dimensions']
        self.facts = self.schema['facts']
        self.field_mapping = {f'{self.mapping[df_field]["db_table"]}.{self.mapping[df_field]["db_field"]}': df_field for df_field in self.mapping.keys() }
        


    def get_ids(
           self,
           df: pd.DataFrame,
           dimension: str,
    ):
        # Параметры из конфигурации измерения
        db_fields = self.dimensions[dimension]['natural_key_columns']
        key_col = self.dimensions[dimension]['id']
        table = dimension
        db_table_fields = [f"{table}.{field}" for field in db_fields]
        
        target_columns = [self.field_mapping[col] for col in db_table_fields if col in self.field_mapping]
      
       
        # Убедимся, что количество колонок совпадает
        if len(target_columns) != len(db_fields):
            raise ValueError("Количество target_columns должно совпадать с natural_key_columns")
        
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
    
        
 