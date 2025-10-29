"""_summary_
"""

from io import BytesIO
from typing import BinaryIO
import boto3
from etl import config

class Minio():
    def __init__(
            self,
            endpoint: str = config.S3_ENDPOINT_URL,
            access_key: str = config.S3_ACCESS_KEY,
            secret_key: str = config.S3_SECRET_KEY,
            bucket: str = config.S3_BUCKET,
            ):
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )
        self.bucket_name = bucket
        if not self.__has_bucket():
            raise Exception(f"Bucket {self.bucket_name} does not exist")

    def __has_bucket(self) -> bool:
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            return True
        except self.client.exceptions.NoSuchBucket:
            return False
        except Exception as e:
            print(f"S3_STORAGE Error: {e}")
            return False

    def get(self, file_name: str) -> BytesIO:
        """
        Возвращает объект совместимый с объектом file в виде потока байт
        Args:
            file_name (str): имя файла для скачивания

        Returns:
            BinaryIO: Объект типа StreamingBody имеющий интерфейс как у объекта file
        """
        response = self.client.get_object(
            Bucket=self.bucket_name,
            Key=file_name)
        return BytesIO(response['Body'].read())