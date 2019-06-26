from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame

from .data_link import DataLink


class PathBasedDataLink(DataLink, ABC):
    def __init__(self, environment: str, session: SparkSession, path: str):
        super().__init__(environment, session)
        self.path = path
