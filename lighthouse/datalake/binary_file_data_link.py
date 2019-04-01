from pyspark.sql import SparkSession, DataFrame

from .path_based_data_link import PathBasedDataLink


class BinaryFileLink(PathBasedDataLink):
    def __init__(self, environment: str, session: SparkSession, path: str):
        super().__init__(environment, session, path)

    def read(self):
        return self.spark.sparkContext.binaryFiles(self.path)

    def write(self, model):
        raise NotImplementedError  # TODO
