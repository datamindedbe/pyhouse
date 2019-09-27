from typing import Mapping, Optional, List

from pyspark.sql import SparkSession, DataFrame

from pyhouse.datalake.data_link import DataLink


class KafkaDataLink(DataLink):
    def __init__(
        self,
        environment: str,
        session: SparkSession,
        topic: str,
        bootstrap_servers: List[str],
        options: Optional[Mapping[str, str]] = None,
    ):
        super().__init__(environment, session)

        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.options = {} if options is None else options

    def read(self) -> DataFrame:
        return (
            self.spark
            .read
            .format("kafka")
            .option("subscribe", self.topic)
            .option("kafka.bootstrap.servers", ",".join(self.bootstrap_servers))
            .options(**self.options)
            .load()
        )

    def write(self, frame: DataFrame) -> None:
        print(self.topic)
        (
            frame
            .write
            .format("kafka")
            .option("topic", self.topic)
            .option("kafka.bootstrap.servers", ",".join(self.bootstrap_servers))
            .options(**self.options)
            .save()
        )
