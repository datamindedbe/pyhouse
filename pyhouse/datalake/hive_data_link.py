from typing import Mapping, Sequence, Optional

from pyspark.sql import SparkSession, DataFrame

from .path_based_data_link import PathBasedDataLink


class HiveDataLink(PathBasedDataLink):
    def __init__(
        self,
        environment: str,
        session: SparkSession,
        path: str,
        database: str,
        table: str,
        storage_format: str = "parquet",
        save_mode: str = "overwrite",
        partitioned_by: Optional[Sequence[str]] = None,
        overwrite_behavior: str = "full_overwrite",
        options: Optional[Mapping[str, str]] = None,
    ):
        super().__init__(environment, session, path)
        self.database = database
        self.table = table
        self.format = storage_format
        self.save_mode = save_mode
        self.partitioned_by = partitioned_by
        self.overwrite_behavior = overwrite_behavior
        self.options = {} if options is None else options

    def read(self) -> DataFrame:
        self.spark.catalog.setCurrentDatabase(self.database)
        return self.spark.read.table(self.table)

    def write(self, frame: DataFrame) -> None:
        self.spark.catalog.setCurrentDatabase(self.database)
        # TODO: implement the cases for the overwrite_behavior flag
        # right now, the simplest approach "overwrite all" is used.
        (
            frame.write.saveAsTable(
                name=self.table,
                format=self.format,
                mode=self.save_mode,
                partitionBy=self.partitioned_by,
                path=self.path,
                **self.options,
            )
        )
