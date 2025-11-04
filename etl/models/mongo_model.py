from typing import Dict, List
from pymongo import MongoClient
from etl import config
from .data_models import Meta


class MongoDB():

    def __init__(
            self,
            host: str = config.MONGO_HOST,
            port: int = int(config.MONGO_PORT),
            db: str = config.MONGO_DB,
            user: str = config.MONGO_USER,
            password: str = config.MONGO_PASSWORD,
            collection: str = "reports"
            ):
        self.client = MongoClient(
            host=host,
            port=port,
            username=user,
            password=password
            )
        self.db = self.client[db]
        self.collection = self.db[collection]

    def get_files_by_status(self, status: str = 'new'):
        """
        Получает коллекцию файлов с указанным статусом
        :param status: Указывает документы с каким статусом запросить из коллеции
        :return: Возвращает генератор словаря 
        >>> {
        >>>     "activity_id": int,
        >>>     "filename": str(полное имя файла. с префиксом),
        >>>     "document_id": str
        >>> }
        """
        if status == "":
            new_collection = self.collection.find()
            for item in new_collection:
                activity = item.get('activity') or {}
                yield {
                    "activity_id": activity.get("id"),
                    "filename": f"{item.get('prefix','')}/{item.get('filename','')}",
                    "document_id": item.get("_id")
                }

        new_collection = self.collection.find({'status': status})
        for item in new_collection:
            activity = item.get('activity') or {}
            yield {
                "activity_id": activity.get("id"),
                "filename": f"{item.get('prefix','')}/{item.get('filename','')}",
                "document_id": item.get("_id")
            }

    def update_status(self, statuses: List[Meta]) -> None:
        for item in statuses:
            self.collection.update_one(
                {"_id": item.document_id},
                {"$set": {"status": item.status, "reason": item.reason}}
                )


if __name__ == "__main__":
    mongo = MongoDB()
    for item in mongo.get_files_by_status():
        print(item)
    # meta = []
    # for item in mongo.get_files_by_status(""):
    #     meta.append(
    #         Meta(
    #             document_id=item.get("_id"),
    #             status="new",
    #             reason=None
    #         )
    #     )
