from dagster import job, op, DynamicOut, DynamicOutput
from pipelines.ops import concat_files, standardize_columns, split_and_write, update_status
import yaml
from models import MongoDB
from etl.config import CONFIGS_DIR

mongo = MongoDB()

# ----------------------
# Загружаем конфиг схемы
# ----------------------
with open(CONFIGS_DIR/"schema.yaml") as f:
    SCHEMA = yaml.safe_load(f)
with open(CONFIGS_DIR/"amapping.yaml") as f:
    MAPPING = yaml.safe_load(f)


# ----------------------
# Сенсор/оп для получения новых файлов
# ----------------------
@op(out=DynamicOut())
def get_new_files_by_activity():
    """
    Получаем новые файлы из Mongo, группируем по activity.
    Для каждой группы файлов создаём DynamicOutput, чтобы построить отдельную ветку DAG.
    return: list[files: dict], int(mapping_key)
        file in files => {
            "activity_id": activity.get("id"),
            "filename": f"{m.get("prefix",'')}/{m.get("filename","")}",
            "document_id": m.get("_id")
        }
 
    """
    
    new_files = mongo.get_files_by_status("new") 
    # Функция возвращает генератор который возвращает словарь
    # {
    #         "activity_id": activity.get("id"),
    #         "filename": f"{m.get("prefix",'')}/{m.get("filename","")}",
    #         "document_id": m.get("_id")
    # }

    activity_groups = {}
    for f in new_files:
        aid = f["activity_id"]
        activity_groups.setdefault(aid, []).append(f)
    
    # Создаём динамические ветки DAG для каждой activity
    for activity_id, files in activity_groups.items():
        yield DynamicOutput(files, mapping_key=str(activity_id))

# ----------------------
# Основной job
# ----------------------
@job
def process_new_files():
    """
    Job с динамическими ветками для каждой activity.
    Для каждой ветки выполняем цепочку:
        concat -> standardize -> split_and_write -> update_status
    """
    activity_files = get_new_files_by_activity()

    for files in activity_files:
        activity_id = str(files[0].get("activity_id", 0))
        schema = SCHEMA[activity_id]

        # 1. Конкатенация файлов
        concat_result = concat_files(files)

        # 2. Стандартизация заголовков
        standardized = standardize_columns(concat_result, MAPPING)

        # 3. Split на dim/fact и запись в DWH
        processed_activities = split_and_write(standardized, schema)

        # 4. Обновление статуса файлов
        update_status(processed_activities)