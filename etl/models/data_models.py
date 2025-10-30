
from dataclasses import dataclass
from typing import Dict, List
import yaml


@dataclass
class Meta():
    document_id: str
    status: str
    reason: str

    def __init__(
            self,
            document_id: str,
            status: str,
            reason: str = None
            ):
        """Простой класс для хранения и передачи метаданных

        Args:
            document_id (str):Идентификатор документа
            status (str): Статус файла
            reason (str, optional): Причичина ошибки если выла. Defaults to None.
        """
        self.document_id = document_id
        self.status = status
        self.reason = reason

    def to_dict(self):
        return {
            "document_id": self.document_id,
            "status": self.status,
            "reason": self.reason
        }

    def __str__(self):
        return (f"document_id: {self.document_id},"
                f"status: {self.status}, reason: {self.reason}")


@dataclass
class Field():
    db_field: str
    report_field: str

@dataclass
class Fact():
    pass

@dataclass
class Dimensions():
    def __init__(self, **kwargs):
        self.columns = [Field] 
@dataclass
class MappingSchema():
    dimensions: list[Dimensions]
    facts: list[Fact]
    
    def __init__(self, columns: List, schema: str):
        """_summary_

        Args:
            columns (List): _description_
            schema (str): path to schema yaml file
        """
        with open(schema, 'r', encoding="UTF.8" ) as f:
            self.schema = yaml.safe_load(f)
        self.columns = columns
        
        def get_dementions():
            dimensions = []
            for dim in self.schema['dimensions']:
                dimensions.append(
                    Field(
                        db_field=db,
                        report_field=report_field
                    ) for db, report_field in dim.items()
                                  )
            return dimensions

        @property
        def dimensions(self):
            return get_dementions() # Список измерений с каждый из котрых содержит словарь с обектами Fied
    
        