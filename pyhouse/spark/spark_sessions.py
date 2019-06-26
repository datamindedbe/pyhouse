from abc import ABC, abstractmethod
from typing import Dict

from pyspark.sql import SparkSession


class SparkSessions(ABC):
    @abstractmethod
    def enable_hive_support(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def spark_options(self) -> Dict[str, str]:
        raise NotImplementedError

    @staticmethod
    def default_configuration() -> Dict[str, str]:
        return {
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.avro.compression.codec": "snappy",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        }

    def spark_session_builder(self) -> SparkSession.Builder:
        builder = SparkSession.Builder()

        joined_config: Dict[str, str] = self.spark_options().copy()
        joined_config.update(self.default_configuration())
        for key, value in joined_config.items():
            builder.config(key, value)

        return builder

    def spark(self) -> SparkSession:
        builder = self.spark_session_builder()

        if self.enable_hive_support():
            builder.enableHiveSupport()

        return builder.getOrCreate()
