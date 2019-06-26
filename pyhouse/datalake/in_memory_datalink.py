from .data_link import DataLink
from pyspark.sql import DataFrame, SparkSession


class InMemoryDataLink(DataLink):
    def __init__(self, environment: str, session: SparkSession, df: DataFrame):
        super(InMemoryDataLink, self).__init__(environment, session)
        self.df = df

    def read(self) -> DataFrame:
        return self.df

    def write(self, frame: DataFrame) -> None:
        self.df = frame
