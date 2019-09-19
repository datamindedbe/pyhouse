from typing import Mapping, Sequence, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from .path_based_data_link import PathBasedDataLink


class FileSystemDataLink(PathBasedDataLink):
    def __init__(
        self,
        environment: str,
        session: SparkSession,
        path: str,
        format: str,
        savemode: str,
        partitioned_by: Optional[Sequence[str]] = None,
        options: Optional[Mapping[str, str]] = None,
        schema: Optional[StructType] = None,
    ):
        super().__init__(environment, session, path)
        self.spark_format = format
        self.savemode = savemode
        self.partitioned_by = partitioned_by
        self.options = {} if options is None else options
        self.schema = schema

    def read(self) -> DataFrame:
        if self.schema is not None:
            return (
                self.spark.read.format(self.spark_format)
                .options(**self.options)
                .schema(self.schema)
                .load(self.path)
            )
        else:
            return (
                self.spark.read.format(self.spark_format)
                .options(**self.options)
                .load(self.path)
            )

    def write(self, frame: DataFrame) -> None:
        if self.partitioned_by is not None:
            (
                frame.write.format(self.spark_format)
                .partitionBy(self.partitioned_by)
                .options(**self.options)
                .mode(self.savemode)
                .save(self.path)
            )
        else:
            (
                frame.write.format(self.spark_format)
                .options(**self.options)
                .mode(self.savemode)
                .save(self.path)
            )
