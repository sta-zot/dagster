from typing import List, Dict, Tuple, Any
import yaml
import re
from dagster import (
    op,
    graph,
    job,
    resource,
    Definitions,
    OpExecutionContext,
    DynamicOut,
    Out,
    DynamicOutput,
    In,
    EnvVar,
    get_dagster_logger,
    in_process_executor,
)  # noqa: TID252
import pandas as pd
from etl.tools import Mapping  # noqa:
from etl.config import CONFIGS_DIR
from etl.models import Meta as CustomMeta
logger = get_dagster_logger()


###################################################

# ----------------------
# 1. Загружаем конфиг схемы
# ----------------------
with open(CONFIGS_DIR/"schema.yaml", 'r', encoding='utf-8') as f:
    SCHEMA = yaml.safe_load(f)
    
with open(CONFIGS_DIR/"mapping.yaml", 'r', encoding='utf-8') as mf:
    MAPPING_SCHEMA = yaml.safe_load(mf)

FIELDS_STD_SCHEMA = (
    {key: MAPPING_SCHEMA[key]['matches'] for key, _ in MAPPING_SCHEMA.items()}
    )


# ==============================
# 2. OPS — шаги обработки данных
# ==============================


@op(required_resource_keys={"mongo_client"})
def fetch_all_new_files(context: OpExecutionContext) -> List[dict]:
    """Запрашивает все записи со статусом "new"

    Args:
        context (_type_): Контекст dagster
    Returns:
        List[dict]: Возвращает список словарей содержащих пути к файлам
    """
    mongo = context.resources.mongo_client
    files = mongo.get_files_by_status()
    """ `files` представляет собой список словарей вида
        >>> {
        >>>     "activity_id": int,
        >>>     "filename": str(полное имя файла. с префиксом),
        >>>     "document_id": str
        >>> }
    """
    files = list(files)
    logger.info(f"Found {len(files)} new files")
    return files


@op
def group_files_by_activity(
    context: OpExecutionContext,
    files: List[Dict[str, Any]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Группирует список файлов по activity_id.
    Возвращает словарь: {activity_id: [файл1, файл2, ...]}
    """
    activity_groups = {}
    for file in files:
        activity_id = file.get("activity_id")
        activity_groups.setdefault(activity_id, []).append(file)
    logger.info(f"Found {len(activity_groups)} activity groups")
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


@op(
    out={
        "combined_df": Out(pd.DataFrame),
        "c_metadata": Out(List[CustomMeta])
    },
    required_resource_keys={"s3_client"}
)
def download_and_combine_files(
    context: OpExecutionContext,
    file_group: List[Dict[str, Any]]
) -> Tuple[pd.DataFrame, List[CustomMeta]]:
    """
    Скачивает файлы из S3 по метаданным, приводит заголовки к единому виду,
    объединяет в один DataFrame.
    Использует: context.resources.s3_client
    """
    c_metadata = []
    mapping = Mapping(FIELDS_STD_SCHEMA)
    s3 = context.resources.s3_client
    dataframes = []
    logger.info(f"download_and_combine_files:\t {len(file_group)} files to process")
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
            logger.info(f"download_and_combine_files:\t {file['filename']} loaded")
        except Exception as e:
            c_metadata.append(
                CustomMeta(
                    document_id=file.get("document_id"),
                    status="error",
                    reason=f"Не удалось загрузить файл {e}"
                )
            )
            logger.error(f"download_and_combine_files:\t {file['filename']} not loaded")
            continue
        try:
            df = df.rename(columns=mapping.get)
            logger.info(f"download_and_combine_files:\t {file['filename']} renamed")
        except Exception as e:
            c_metadata.append(
                CustomMeta(
                    document_id=file.get("document_id"),
                    status="error",
                    reason=f"Не удалось стандартизировать имена заголовков отчёта. {e}"
                )
            )
            logger.error(f"download_and_combine_files:\t {file['filename']} not renamed")
            continue
        dataframes.append(df)
        c_metadata.append(
            CustomMeta(
                document_id=file.get("document_id", ""),
                status="processed",
                reason="None"
            )
        )
    if not dataframes:
        logger.error("download_and_combine_files:\t dataframes is empty")
        raise Exception("download_and_combine_files:\t dataframes is empty")
    return pd.concat(dataframes, ignore_index=True), c_metadata


@op
def clean_and_enrich(
    context: OpExecutionContext,
    combined_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Выполняет дедупликацию, очистку и обогащение данных.
    """
    if combined_df.empty:
        logger.error("clean_and_enrich:\t combined_df is empty")
        return pd.DataFrame()
    logger.info(f"clean_and_enrich:\t {len(combined_df)} rows to process")
    combined_df = combined_df.drop_duplicates()
    combined_df = combined_df.dropna(how="all")
    combined_df["settlement"] = (
                combined_df["settlement"]
                .str.strip()
                .str.replace(r'^[\w]+\.','', regex=True)
                .str.strip()
    )
    return combined_df


@op(required_resource_keys={"target_db"})
def load_dimensions_and_facts(
    context: OpExecutionContext, df: pd.DataFrame, c_meta: List[CustomMeta]
) -> Dict[str, Any]:
    """
    Загружает измерения и факты в целевую БД.
    Возвращает список _id исходных записей (как строки), которые были обработаны.
    Использует: context.resources.target_db
    """
    if df.empty:
        logger.error("load_dimensions_and_facts:\t df is empty")
        return {"c_meta": c_meta, "Writed_fats_count": 0}
    key = context.get_mapping_key()
    target_db = context.resources.target_db
    try:
        ids = target_db.dispatch(key, df)
    except Exception as e:
        logger.error(f"load_dimensions_and_facts:\t {e}")
        raise e
    return {
        "c_meta": c_meta,
        "Writed_fats_count": ids
    }


@op(required_resource_keys={"mongo_client"})
def update_mongo_status(context: OpExecutionContext, processed_ids: Dict) -> None:
    """
    Обновляет статус записей в MongoDB на 'processed' (или другой).
    Использует: context.resources.mongo_client
    """
    mongo = context.resources.mongo_client
    mongo.update_status(processed_ids['c_meta'])


# ==============================
# 3. ГРАФ — обработка одной группы файлов
# ==============================

@graph(ins={"file_group": In(List[Dict[str, Any]])})
def process_file_group(file_group: List[Dict[str, Any]]):
    """
    Последовательная обработка одной группы файлов (по activity).
    """
    combined= download_and_combine_files(file_group)
    cleaned = clean_and_enrich(combined.combined_df)
    ids = load_dimensions_and_facts(cleaned, combined.c_metadata) 
    update_mongo_status(ids)


# ==============================
# 4. JOB — основной pipeline
# ==============================

@job(executor_def=in_process_executor)
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
