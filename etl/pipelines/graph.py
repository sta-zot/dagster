from typing import List, Dict, Tuple, Any
import yaml
from dagster import (
    op,
    graph,
    job,
    resource,
    Definitions,
    OpExecutionContext,
    DynamicOut,
    DynamicOutput,
    EnvVar,
    get_dagster_logger,
)  # noqa: TID252
import pandas as pd
from etl.tools import Mapping  # noqa:
from etl.config import CONFIGS_DIR

logger = get_dagster_logger()


###################################################

# ----------------------
# 1. Загружаем конфиг схемы
# ----------------------
with open(CONFIGS_DIR/"schema.yaml") as f:
    SCHEMA = yaml.safe_load(f)
with open(CONFIGS_DIR/"mapping.yaml") as f:
    MAPPING = yaml.safe_load(f)

# ==============================
# 2. OPS — шаги обработки данных
# ==============================


@op
def fetch_all_new_files(context: OpExecutionContext) -> List[dict]:
    """Запрашивает все записи со статусом "new"

    Args:
        context (_type_): Контекст dagster
    Returns:
        List[dict]: Возвращает список словарей содержащих пути к файлам
    """
    mongo = context.resources.mongo_client
    files = mongo.get_files_by_status()
    return list(files)


@op
def group_files_by_activity(
    context: OpExecutionContext,
    files: List[Dict[str, Any]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Группирует список файлов по activity['id'].
    Возвращает словарь: {activity_id: [файл1, файл2, ...]}
    """
    activity_groups = {}
    for file in files:
        activity_id = file.get("activity_id")
        activity_groups.setdefault(activity_id, []).append(file)
    return activity_groups
        


@op(out=DynamicOut())
def emit_file_groups(
    context: OpExecutionContext,
    grouped: Dict[str, List[Dict[str, Any]]]
):
    """
    Преобразует словарь групп в DynamicOutput для параллельной обработки.
    Каждая группа — отдельный dynamic output с mapping_key = activity_id.
    """
    for activity_id, file_list in grouped.items():
        # mapping_key должен быть валидным идентификатором (без точек, пробелов и т.п.)
        safe_key = str(activity_id).replace(".", "_").replace("-", "_")
        yield DynamicOutput(file_list, mapping_key=safe_key)


@op
def download_and_combine_files(
    context: OpExecutionContext,
    file_group: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    Скачивает файлы из S3 по метаданным, приводит заголовки к единому виду,
    объединяет в один DataFrame.
    Использует: context.resources.s3_client
    """
    metadata = []
    mapping = Mapping(MAPPING).get
    s3 = context.resources.s3_client
    dataframes = []
    for file in file_group:
        """file это словарь с ключами:
        >>> {
            >>>     "activity_id": int,
            >>>     "filename": str(полное имя файла. с префиксом),
            >>>     "document_id": str
            >>> }
        """
        try:
            # Загружаем  файл из хранилища
            df = pd.read_excel(s3.get(file["filename"]))
            df = df.rename(columns=[
                mapping(key) for key in df.columns
                ]
            )
            dataframes.append(df)
            metadata.append(
                Meta(
                    document_id=file.get("document_id"),
                    status="processed",
                    reason=None
                )
            )
        except Exception as e:
            metadata.append(
                Meta(
                    document_id=file.get("document_id"),
                    status="error",
                    reason=f"File not found. {e}"
                )
            )
            continue
    return pd.concat(dataframes, ignore_index=True), metadata

@op
def clean_and_enrich(
    context: OpExecutionContext,
    combined_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Выполняет дедупликацию, очистку и обогащение данных.
    """
    combined_df = combined_df.drop_duplicates()
    combined_df = combined_df.dropna(how="all")
    


@op
def split_to_dims_and_facts(
    context: OpExecutionContext, clean_df: pd.DataFrame
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Разделяет данные на измерения и факты.
    Возвращает: ({'dim_region': df1, 'dim_activity': df2, ...}, fact_table)
    """
    pass


@op
def load_dimensions_and_facts(
    context: OpExecutionContext, tables: Tuple[Dict[str, pd.DataFrame], pd.DataFrame]
) -> List[str]:
    """
    Загружает измерения и факты в целевую БД.
    Возвращает список _id исходных записей (как строки), которые были обработаны.
    Использует: context.resources.target_db
    """
    pass


@op
def update_mongo_status(context: OpExecutionContext, processed_ids: List[str]) -> None:
    """
    Обновляет статус записей в MongoDB на 'processed' (или другой).
    Использует: context.resources.mongo_client
    """
    pass


# ==============================
# 3. ГРАФ — обработка одной группы файлов
# ==============================

@graph
def process_file_group(file_group: List[Dict[str, Any]]):
    """
    Последовательная обработка одной группы файлов (по activity).
    """
    combined, metadata = download_and_combine_files(file_group)
    cleaned = clean_and_enrich(combined)
    tables = split_to_dims_and_facts(cleaned)
    ids = load_dimensions_and_facts(tables)
    update_mongo_status(ids)


# ==============================
# 4. JOB — основной pipeline
# ==============================

@job
def process_all_new_files():
    """
    Основной pipeline:
    1. Получить все новые файлы
    2. Сгруппировать по activity
    3. Для каждой группы — запустить process_file_group параллельно
    """
    files = fetch_all_new_files()
    grouped = group_files_by_activity(files)
    dynamic_groups = emit_file_groups(grouped)
    dynamic_groups.map(process_file_group)


# # ==============================
# # 5. ОПРЕДЕЛЕНИЕ — сборка всего вместе
# # ==============================

# defs = Definitions(
#     jobs=[process_all_new_files],
#     resources={
#         "mongo_client": mongo_client_resource,
#         "s3_client": s3_client_resource,
#         "target_db": target_db_resource,
#     },
# )