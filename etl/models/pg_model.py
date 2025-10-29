import pandas as pd
from typing import Dict
from sqlalchemy import create_engine, text

class DWHModel():
   def __init__(
           self,
           db_host: str,
           db_port: int,
           db_name: str,
           db_user: str,
           db_password: str,
           db_schema: str
    ):
       self.db_schema = db_schema
       self.engine = create_engine(
           f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
       )
   

   def get_ids(self, df: pd.DataFrame, dim_config: Dict) -> pd.Series:
        """
        Возвращает суррогатные ключи для всех строк датафрейма.
        
        :param df: датафрейм со стандартизированными колонками
        :param dim_config: словарь вида {
            "table": "dim_Event",
            "key_column": "event_id",
            "natural_key_mapping": {"event_type": "type", ...}
        }
        :return: pd.Series с суррогатными ключами
        """
        table = dim_config["table"]
        key_col = dim_config["key_column"]
        nk_map = dim_config["natural_key_mapping"]  # df_col → db_col

        # Извлекаем только нужные колонки из датафрейма
        df_nk = df[list(nk_map.keys())].drop_duplicates().copy()

        # Формируем условия WHERE для каждой строки
        where_clauses = []
        params = []
        for _, row in df_nk.iterrows():
            conditions = []
            # -> conditions = ["region = :region_0", "municipality = :municipality_0", "settlement = :settlement_0"]
            row_params = {}
            # -> row_params = {"region_0": "Орловская область", "municipality_0": "Городской округ Орёл", "settlement_0": "Орёл"}
            for df_col, db_col in nk_map.items():
                param_name = f"{db_col}_{len(params)}"
                conditions.append(f"{db_col} = :{param_name}") 
                row_params[param_name] = row[df_col]
            where_clauses.append(" AND ".join(conditions))
            params.append(row_params)

        # Формируем полный запрос (UNION ALL для всех строк)
        selects = [
            f"SELECT {key_col}, {i} AS __row_id FROM {table} WHERE {clause}"
            for i, clause in enumerate(where_clauses)
        ]
        full_query = " UNION ALL ".join(selects) if selects else f"SELECT {key_col}, -1 AS __row_id FROM {table} WHERE 1=0"
        """
            SELECT location_id, 0 AS __row_id FROM dim_Location 
            WHERE region = :region_0 AND municipality = :municipality_0 AND settlement = :settlement_0
            UNION ALL
            SELECT location_id, 1 AS __row_id FROM dim_Location 
            WHERE region = :region_1 AND municipality = :municipality_1 AND settlement = :settlement_1
        """
        # Выполняем запрос
        if not params:
            result_df = pd.DataFrame(columns=[key_col, "__row_id"])
        else:
            # Объединяем все параметры в один словарь
            all_params = {}
            for p in params:
                all_params.update(p)
            result_df = pd.read_sql(full_query, self.engine, params=all_params)

        # Создаём маппинг row_id → key
        row_to_key = dict(zip(result_df["__row_id"], result_df[key_col]))

        # Восстанавливаем порядок и дублируем ключи для исходного датафрейма
        df_nk["__row_id"] = range(len(df_nk))
        df_nk[key_col] = df_nk["__row_id"].map(row_to_key)

        # Маппинг обратно на исходный датафрейм
        df_result = df[list(nk_map.keys())].merge(
            df_nk[list(nk_map.keys()) + [key_col]],
            on=list(nk_map.keys()),
            how="left"
        )
        return df_result[key_col]