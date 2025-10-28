
from dataclasses import dataclass


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
