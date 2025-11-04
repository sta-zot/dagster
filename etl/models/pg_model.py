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
from dagster import get_dagster_logger

DWH_SCHEMA = PACKAGE_ROOT / "configs/schema.yaml"
MAPPING_SCHEMA = PACKAGE_ROOT / "configs/mapping.yaml"


class DWHModel:
    def __init__(
        self,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_pass: str,
    ):
        self.engine = create_engine(
            f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        )
        self.logger = get_dagster_logger(self.__class__.__name__)

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
        self.HandlerMap: Dict[str, callable] = {
            "1": self.load_events_facts,
            "2": self.load_edu_integrations_facts,
            "3": self.load_im_placements_facts,
            "4": self.load_trainig_facts,
        }

    def dispatch(self, activity_id: str, df: pd.DataFrame) -> None:
        if activity_id not in self.HandlerMap.keys():
            self.logger.error(f"Unknown activity_id: {activity_id}")
            return 0      
        ids = self.HandlerMap[activity_id](df)
        return len(ids)

    def load_to_fact_table(
        self,
        df: pd.DataFrame,
        table: str,
        lookup_fields: List[str],
    ) -> pd.DataFrame:

        # Сопоставляем полям из отчета поля из БД если они есть иначе оставляем как есть
        db_key_mapping = {}
        for field in df.columns:
            if field not in self.mapping.keys():
                db_key_mapping[field] = field
                continue
            db_key_mapping[field] = self.mapping[field]["db_field"]

        # Получаем поле ключ
        key_col = self.schema['facts'][table]['id']
        # создадим новый список имён полей для поиска 
        lookup_fields = [
            db_key_mapping[field] for field in lookup_fields
        ]
        # так как это таблица фактов в ней не может быть одинаковых полей,
        # поэтому создаем копию таблицы с переименованным столбцами
        renamed_df = df.rename(columns=db_key_mapping)
        # target_columns = list(renamed_df.columns)

        found_rows = self.bulk_select(
            df=renamed_df[lookup_fields],
            table=table,
            lookup_fields=lookup_fields,
            key_col=key_col,
        )
        # Получаем набор данных состоящий из lookup_fields + key_col полей
        # объединяем основной набор данных с найденными данными по полям поиска
        merged_df = pd.merge(
            left=renamed_df,
            right=found_rows,
            on=lookup_fields,
            how="left",
            suffixes=("", "_found"),
            indicator=True,
        )
        
        rows_for_insert = merged_df[merged_df[key_col].isna()].drop(columns=[key_col, "_merge"])
            
        if not rows_for_insert.empty:
            inserted_rows = self.bulk_insert(
                df=rows_for_insert,
                table=table,
                target_fields=list(rows_for_insert.columns),
                key_col=key_col,
            )
            rows_for_insert = pd.merge(
                left=rows_for_insert,
                right=inserted_rows,
                on=lookup_fields,
                how="left",
                suffixes=("", "_found"),
            )
        merged_df = merged_df.dropna(subset=[key_col]).copy()
        final_df = pd.concat([merged_df, rows_for_insert], ignore_index=True)
        # Делаем словарь где ключами являются новые имена а значениями старые
        # Переворачиваем словарь db_key_mapping
       
        old_col_names = {
            val: key for key, val in db_key_mapping.items()
        }
        return final_df.rename(columns=old_col_names)

    def process_dims(
        self,
        df: pd.DataFrame,
        table: str,
        lookup_fields: List[str],
        target_fields: List[str],
        key_col: str,
        custom_col: str = "",
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
            field in self.mapping 
                for field in set(target_fields + lookup_fields)
        ):
            raise ValueError(
                "Not all fields are present in the mapping dictionary."
                )

        slice_df = df[target_fields].copy()
        slice_df = slice_df.drop_duplicates().reset_index(drop=True)
        merge_fields = lookup_fields.copy()
        for field in lookup_fields[:]:
            if is_datetime64_any_dtype(slice_df[field]):
                print(
                    f"Converting {field} (dtype={slice_df[field].dtype}) to int date_id"
                )
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
        # print(type(l_fields))
        # for field in l_fields:
        # print(f"\t -- {field} ({type(field)})")

        params = self.get_query_params(
            df=slice_df,
            fields=l_fields,
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
            existing_df = pd.DataFrame(columns=[key_col] + l_fields)

        try:
            # Определяем, какие строки отсутствуют в БД
            merged_df = slice_df.merge(
                existing_df, on=l_fields, how="left", indicator=True
            )
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
                fields=t_fields,
            )
            query = f"""
                INSERT INTO {table} ({params['columns']})
                VALUES {params['placeholders']}
                RETURNING {key_col}, {params['columns']}
            """
            
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

        columns_for_drop = [
            col for col in set(t_fields + target_fields) if col in df_with_ids.columns
        ]

        df = df_with_ids.drop(columns=columns_for_drop)
        if custom_col:
            df = df.rename(columns={key_col: custom_col})
        return df

    def get_query_params(
        self,
        df: pd.DataFrame,
        fields: List[str],
    ):
        """Фнкция генерирует словарь содержащий:
        - строки со списком полей для запроса,
        - Placeholder для запроса в котором список кортежей через запятую 
        - словарь с параметрами для плейсхолдера
        

        Args:
            df (pd.DataFrame): Набор данных на основе которого генерируется плейсхолдер и словарь параметров
            fields (List[str]): Список полей на основе которых должен быть список.

        Returns:
            Dict[str: Any]: словарь содержащий:
        - строки со списком полей для запроса 
        >>> "field_1, field_2, ... field_n"
        - Placeholder для запроса в котором список кортежей через запятую
        >>> [
        >>>     (:val_0_0, :val_0_1, :val_0_2, :val_0_3,),
        >>>     (:val_1_0, :val_1_1, :val_1_2, :val_1_3,),
        >>>     ....
        >>>     (:val_n_0, :val_n_1, :val_n_2, :val_n_3,)
        >>> ]
        - словарь с параметрами для плейсхолдера
        >>>    {
        >>>        "val_0_0": "value",
        >>>        "val_0_1": "value",
        >>>        "val_0_2": "value",
        >>>        "val_0_3": "value",
        >>>        ...,
        >>>        "val_n_n": "value"
        >>>    }
        """
        # Подготовка параметров для запроса
        param_dict = {}
        placeholders_parts = []

        for i, (_, row) in enumerate(df.iterrows()):
            row_placeholders = []
            for j, (col) in enumerate(fields):
                param_name = f"val_{i}_{j}"
                # Преобразуем другие типы в типы python
                value = row[col]
                if hasattr(value, "item"):
                    value = value.item()
                param_dict[param_name] = value
                row_placeholders.append(f":{param_name}")
            placeholders_parts.append(f"({', '.join(row_placeholders)})")
        placeholders = ", ".join(placeholders_parts)
        columns_str = ", ".join(fields)
        return {
            "params": param_dict,
            "placeholders": placeholders,
            "columns": columns_str,
        }

    def bulk_select(
        self, df: pd.DataFrame, table: str, lookup_fields: List[str], key_col: str
    ) -> pd.DataFrame:
        """
            На основе значений из столбцов переданных в параметре `lookup_fields`
            выполняет поиск в таблице `table` и возвращает набор найденных данных
            с дополнительным полем `key_col`
            Args:
                df: pd.DataFrame - набор данных содержащий столбцы по которым надо искать
                table: str - имя таблицы
                lookup_fields: List[str] - список столбцов из набора данных в которых содержатся данные
                key_col: str = Поле которое нужно нойти по соответствующим данным
        """
        params = self.get_query_params(
            df=df, fields=lookup_fields
        )
        query = f"""
            SELECT {key_col}, {params["columns"]}
            FROM {table}
            WHERE ({params["columns"]}) IN ({params["placeholders"]})
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params["params"]).fetchall()
        new_df = pd.DataFrame(result, columns=[key_col] + lookup_fields)
        return new_df

    def bulk_insert(
        self,
        df: pd.DataFrame,
        table: str,
        target_fields: List[str],
        key_col: str,
    ) -> pd.DataFrame:
        """
            На основе значений из столбцов переданных в параметре `lookup_fields`
            выполняет вставку  значений из набора данных в таблицу `table` и возвращает набор 
            данных которые были вставленны с дополнительным полем `key_col`
            Args:
                df: pd.DataFrame - набор данных содержащий столбцы с данными для вставки
                table: str - имя таблицы
                lookup_fields: List[str] - список столбцов из набора данных в которых содержатся данные
                key_col: str = Поле которое нужно вернуть после вставкии соответствующих данных
        """
        if df.empty:
            return pd.DataFrame(columns=[key_col] + target_fields)
        params = self.get_query_params(
            df=df, fields=target_fields
        )
        if key_col:
            query = f"""
                INSERT INTO {table} ({params['columns']})
                VALUES {params['placeholders']}
                RETURNING {key_col}, {params['columns']}
            """
        else:
            query = f"""
                INSERT INTO {table} ({params['columns']})
                VALUES {params['placeholders']}
                RETURNING {params['columns']}
            """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params["params"]).fetchall()
            conn.commit()
        df_columns = ([key_col] + target_fields) if key_col else target_fields
        new_df = pd.DataFrame(result, columns=df_columns)
        return new_df

    def load_events_facts(self, df: pd.DataFrame):
        # Преобразуем измерение dim_location в location_id
        p_df = self.process_dims(
            df=df,
            table="dim_location",
            lookup_fields=["settlement", "municipality", "region"],
            target_fields=["settlement", "municipality", "region"],
            key_col="location_id",
        )
        # Преобразуеми измерение dim_audience в audience_id(Целевая Аудитория)
        p_df = self.process_dims(
            df=p_df,
            table="dim_audience",
            lookup_fields=["age_group", "social_group"],
            target_fields=["age_group", "social_group"],
            key_col="audience_id",
        )
        # Преобразуем измерение dim_event["event_type", "event_format", "event_topic"] в location_id
        p_df = self.process_dims(
            df=p_df,
            table="dim_event",
            lookup_fields=["event_type", "event_format", "event_topic"],
            target_fields=["event_type", "event_format", "event_topic"],
            key_col="event_id",
        )
        # Преобразуеим измерение dim_staff ["organizer_name", "department", "personInCharge"] в staff_id(Организатор)
        p_df = self.process_dims(
            df=p_df,
            table="dim_staff",
            lookup_fields=["organizer_name", "department", "personInCharge"],
            target_fields=["organizer_name", "department", "personInCharge"],
            key_col="staff_id",
            custom_col="organizer_id",
        )
        # Преобразуеми измерение dim_partner в partner_id
        p_df = self.process_dims(
            df=p_df,
            table="dim_partner",
            lookup_fields=["partner_name", "partner_type"],
            target_fields=["partner_name", "partner_type"],
            key_col="partner_id",
        )
        # Преобразуем дату в числоваой формат так как он и есть идентификатор
        p_df["date"] = p_df["date"].apply(
            lambda field: int(field.strftime("%Y%m%d")) if pd.notnull(field) else None
        )

        # Создаём для каждого волонтера из списка отдельную запись
        volunteers = []
        # указываем какие столбцы не будут учитываться при сопоставлении
        excluded_cols = ["volunteers", "volunteers_type"] 
        # Генерируем список по которому будет проводится сопоставление
        merge_columns = [
            col for col in p_df.columns if col not in excluded_cols
            ] 
        for _, row in p_df.iterrows():
            _volunteers = row["volunteers"].split(", ")
            for volunteer in _volunteers:
                record = {
                    "name": re.sub(r"[^а-яА-ЯёЁa-zA-Z\s]", "", volunteer).strip(),
                    "type": re.sub(
                        r"[^а-яА-ЯёЁa-zA-Z\s]", "", row["volunteers_type"]
                    ).strip(),
                }
                for col in merge_columns:
                    record[col] = row[col]
                volunteers.append(record)
        df_volunteers = pd.DataFrame(volunteers)
        # Вставляем данные в таблицу фактов без данных о волонтерах
        fact_id = self.load_to_fact_table(
            p_df[merge_columns],
            "fact_events",
            lookup_fields=[
                "date",
                "location_id",
                "event_id",
                "audience_id",
            ],
        )
        # Набор данных волонтеры состоит и полей name, type fact_id.columns
        df_volunteers = df_volunteers.merge(
            fact_id,
            on=merge_columns,
            how="left",
        )
        # Выбираем ID
        volunteers_with_id = self.bulk_select(
            df_volunteers[["name", "type"]],
            "dim_staff",
            lookup_fields=["name", "type"],
            key_col="staff_id",
        )
        self.logger.info(f"Volunteeres in db {volunteers_with_id.count()}")
        # volunteers_with_id вернёт datafrem c volunteer_id, name, type

        # Добовляем найденных волонтеров в общий датафрейм
        df_volunteers = pd.merge(
            left=df_volunteers,
            right=volunteers_with_id,
            on=["name", "type"],
            how="left",
        )

        # Вставляем волонтеров которых нет в базе
        vol_for_insert = (
            df_volunteers[df_volunteers["staff_id"].isna()][["name", "type"]]
            .drop_duplicates()
        )
        print(f"Volunteeres to insert {vol_for_insert.head()}")
        inserted_volunteers = self.bulk_insert(
                df=vol_for_insert,
                table="dim_staff",
                target_fields=["name", "type"],
                key_col="staff_id",
            )
        print(f"inserted_volunteers {inserted_volunteers.head()}")

        # Объединяем волонтеров которых нет в базе с теми которые были вставленны
        # Устанавливаем в качестве индекса одинаковые поля
        df_volunteers.set_index(["name", "type"], inplace=True)
        inserted_volunteers.set_index(["name", "type"], inplace=True)
        # Выбираем столбец с сериями
        new_id_map = inserted_volunteers["staff_id"]
        df_volunteers.fillna({"staff_id": new_id_map}, inplace=True)
        df_volunteers.reset_index(inplace=True)

        if df_volunteers["staff_id"].isna().any():
            self.logger.warning(f"Некотррых волонтеров не удалось вставить")
        vol_in_events = (
            df_volunteers[["staff_id", "fact_event_id"]]
            .rename(columns={
                "staff_id": "volunteer_id",
                "fact_event_id": "fact_event_id",
            })
        )

        missing_mask = vol_in_events['fact_event_id'].isna()
        if missing_mask.any():
            raise Exception()
        missing_mask = vol_in_events['volunteer_id'].isna()
        if missing_mask.any():
            raise Exception()
        self.bulk_insert(
            df=vol_in_events,
            table="volunteers_in_events",
            target_fields=["volunteer_id", "fact_event_id"],
            key_col="id",
        )
        return fact_id

    def load_im_placements_facts(self, df: pd.DataFrame) -> pd.DataFrame:
        # Преобразуем измерение dim_location в location_id
        temp_df = self.process_dims(
            df=df,
            table="dim_location",
            lookup_fields=["settlement", "municipality", "region"],
            target_fields=["settlement", "municipality", "region"],
            key_col="location_id",
        )
        # Преобразуем дату в числоваой формат так как он и есть идентификатор
        temp_df["placement_date"] = temp_df["placement_date"].apply(
            lambda field: int(field.strftime("%Y%m%d")) if pd.notnull(field) else None
        )
        # Преобразуем измерение Информационные материалы в info_mat_id
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_info_materials",
            lookup_fields=["im_name", "im_type", "im_topic", "im_format"],
            target_fields=["im_name", "im_type", "im_topic", "im_format"],
            key_col="info_materials_id",
            custom_col="info_mat_id",
        )
        # Преобразуем измерение dim_placement_point в placement_point_id
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_placement_point",
            lookup_fields=["pp_name", "pp_type"],
            target_fields=["pp_name", "pp_type"],
            key_col="placement_point_id",
        )
        # Преобразуеми измерение dim_audience в audience_id(Целевая Аудитория)
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_audience",
            lookup_fields=["age_group", "social_group"],
            target_fields=["age_group", "social_group"],
            key_col="audience_id",
        )
        # Преобразуеми измерение dim_partner в partner_id
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_partner",
            lookup_fields=["partner_name", "partner_type"],
            target_fields=["partner_name", "partner_type"],
            key_col="partner_id",
        )
        # Преобразуеим измерение dim_staff в staff_id(Организатор)
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_staff",
            lookup_fields=["organizer_name", "department", "personInCharge"],
            target_fields=["organizer_name", "department", "personInCharge"],
            key_col="staff_id",
            custom_col="organizer_id",
        )

        ids = self.load_to_fact_table(
            temp_df,
            "fact_im_placements",
            [
                "placement_date",
                "info_mat_id",
                "placement_point_id",
            ],
        )
        return ids

    def load_edu_integrations_facts(self, df: pd.DataFrame):
        # Преобразуем измерение dim_location в location_id
        temp_df = self.process_dims(
            df=df,
            table="dim_location",
            lookup_fields=["settlement", "municipality"],
            target_fields=["settlement", "municipality"],
            key_col="location_id",
        )

        # Преобразуеим измерение dim_organization в organization_id(Организатор)
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_organization",
            lookup_fields=[
                "edu_org_name",
                "edu_org_type",
            ],
            target_fields=[
                "edu_org_name",
                "edu_org_type",
            ],
            key_col="organization_id",
        )

        # Выделим измерение образовательной програмы
        if "edu_program_type" not in temp_df.columns:
            temp_df["edu_program_type"] = "Обязательное образование"
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_edu_program",
            lookup_fields=[
                "edu_program_name",
                "edu_program_type",
            ],
            target_fields=[
                "edu_program_name",
                "edu_program_type",
            ],
            key_col="edu_program_id",
        )
        if "teachers_finlit_trained_cnt" not in temp_df.columns:
            temp_df["teachers_finlit_trained_cnt"] = temp_df[
                "teachers_finlit_train_cnt"
            ]
        if "date" not in temp_df.columns:
            from etl.tools import get_random_date
            from datetime import datetime
            temp_df['date'] = None
            temp_df["date"] = temp_df["date"].apply(
                lambda x: get_random_date(
                    datetime.strptime("2022-01-01", "%Y-%m-%d").date(),
                    datetime.strptime("2023-12-31", "%Y-%m-%d").date(),
                )
            )
            
        ids = self.load_to_fact_table(
            temp_df,
            "fact_edu_integrations",
            [
                "edu_program_id",
                "organization_id",
                "location_id",
            ],
        )
        return ids

    def load_trainig_facts(self, df: pd.DataFrame):
        # Get location dimensions id
        temp_df = self.process_dims(
            df=df,
            table="dim_location",
            lookup_fields=["settlement", "municipality", "region"],
            target_fields=["settlement", "municipality", "region"],
            key_col="location_id",
        )
        # Get staff dimension id
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_staff",
            lookup_fields=["fullname", "participants_type", "participants_spec"],
            target_fields=["fullname", "participants_type", "participants_spec"],
            key_col="staff_id",
        )
        # Get organiztions ids
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_organization",
            lookup_fields=["affiliation_org", "affiliation_org_type"],
            target_fields=["affiliation_org", "affiliation_org_type"],
            key_col="organization_id",
            custom_col="affiliation_org_id",
        )
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_organization",
            lookup_fields=["edu_org_name"],
            target_fields=["edu_org_name"],
            key_col="organization_id",
            custom_col="training_provider_id",
        )
        # get trainings programm
        temp_df = self.process_dims(
            df=temp_df,
            table="dim_training_program",
            lookup_fields=["training_program_name", "training_program_provider"],
            target_fields=[
                "training_program_name",
                "training_program_provider",
                "num_hours",
            ],
            key_col="training_program_id",
        )
        # Преобразуем дату в числоваой формат так как он и есть идентификатор
        temp_df["start_date"] = temp_df["start_date"].apply(
            lambda field: int(field.strftime("%Y%m%d")) if pd.notnull(field) else None
        )
        temp_df["end_date"] = temp_df["end_date"].apply(
            lambda field: int(field.strftime("%Y%m%d")) if pd.notnull(field) else None
        )
        print(list(temp_df.columns))

        ids = self.load_to_fact_table(
            temp_df,
            "fact_trainings",
            ["start_date", "end_date", "location_id", "staff_id"],
        )
        return ids
