from dagster import op
import pandas as pd
import json
from models import Minio, MongoDB, DWHModel, Meta
from tools import Mapping

minio_client = Minio()
mongo_client = MongoDB()
dwh_client = DWHModel()


# ----------------------
# Шаг 1: Загрузка файлов
# ----------------------
@op
def concat_files(file_list: list[dict]) -> tuple[pd.DataFrame, dict]:
    """
    Конкатенируем файлы одного типа activity в один DataFrame.
    Возвращаем:
        - df: объединённый датафрейм
        - meta: словарь со списками успешно обработанных
          и необработанных файлов  
    >>> {
    >>>     "activity": activity_id,
    >>>     "success": list[filename],
    >>>     "failed": [
    >>>         {
                    filename: str,
                    reason: str
    >>>         }
    >>>     ]
    >>> }
    """
    dfs = []
    meta = []
    for file in file_list:
        try:
            df = pd.read_excel(
                minio_client.get_file(
                    file.get("filename", '')
                    )
            )
            dfs.append(df)
            meta.append(
                Meta(
                    document_id=file.get("document_id"),
                    status="processed",
                    reason="ok"
                )
            )
        except Exception as e:
            meta.append(
                Meta(
                    document_id=file.get("document_id"),
                    status="failed",
                    reason=str(e)
                )
            )
            continue
        
    return pd.concat(dfs, ignore_index=True), meta


# ----------------------
# Шаг 2: Стандартизация заголовков
# ----------------------
@op
def standardize_columns(concat_result: tuple, mapping: dict) -> tuple[pd.DataFrame, list]:
    """
    Применяем общий mapping для замены заголовков.
    concat_result: (df, meta)
    params:
         concat_result: Кортеж из датафрейма и метаданных
         mapping: словарь для замены заголовков
    """
    meta_mapping = Mapping(mapping)
    df, meta = concat_result
    df = df.rename(columns=[
        meta_mapping.get(key) for key in df.columns])
    return df, meta

# ----------------------
# Шаг 3: Разделение на измерения и факты и запись в DWH
# ----------------------
@op
def split_and_write(standardized_result: tuple, schema: dict):
    """
    Для каждого activity берём подмножество колонок по схеме:
      - dimensions
      - facts
    Записываем в DWH.
    """
    df, meta = standardized_result
    processed_activities = []
    for activity_id in set([m["activity_id"] for m in meta]):
        s = schema[str(activity_id)]
        dims = s["dimensions"]
        facts = s["facts"]
        df_activity = df[df["activity_id"] == activity_id]
        dim_data = df_activity[dims].drop_duplicates()
        fact_data = df_activity[facts]
        dwh.write(dim_data, fact_data)
        processed_activities.append(activity_id)
    return processed_activities

# ----------------------
# Шаг 4: Обновление статуса файлов в Mongo
# ----------------------
@op
def update_status(processed_activities: list):
    """
    Обновляем статус обработанных файлов для каждой activity.
    """
    for activity_id in processed_activities:
        mongo.update_status(activity_id, status="processed")


