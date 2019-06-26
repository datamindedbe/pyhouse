from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame


class DataLink(ABC):
    def __init__(self, environment: str, session: SparkSession):
        self.spark = session
        self.environment = environment

    @abstractmethod
    def read(self) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def write(self, frame: DataFrame) -> None:
        raise NotImplementedError
