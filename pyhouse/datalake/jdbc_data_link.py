import pyspark.sql.functions as sfun
from pyspark.sql import SparkSession, DataFrame

from .data_link import DataLink


class JdbcDataLink(DataLink):
    def __init__(
        self,
        environment: str,
        session: SparkSession,
        url: str,
        username: str,
        password: str,
        driver: str,
        table: str,
        save_mode: str = "error",
        number_of_partitions: int = 1,
        partition_column: str = "",
    ):
        super().__init__(environment, session)

        self.number_of_partitions = number_of_partitions
        self.partition_column = partition_column
        self.save_mode = save_mode
        self.connection_properties = {
            "url": url,
            "user": username,
            "password": password,
            "driver": driver,
            "dbtable": table,
        }

    def read(self) -> DataFrame:
        # Partition parameters are only applicable for read operation
        # for now, order is important as some values can be overwritten
        reader = self.spark.read.format("jdbc").options(**self.connection_properties)

        if self.number_of_partitions == 1:
            return reader.load()

        else:
            # We need a partition column
            assert self.partition_column != ""
            # Retrieve lower and upper bound first to
            # determine the degree of parallelism
            lower_bound, upper_bound = (
                reader.load()
                .select(
                    sfun.min(sfun.col(self.partition_column)).alias("mmin"),
                    sfun.max(sfun.col(self.partition_column)).alias("mmax"),
                )
                .collect()[0]
            )

            return (
                reader.option("partitionColumn", self.partition_column)
                .option("numPartitions", str(self.number_of_partitions))
                .option("lowerBound", str(lower_bound))
                .option("upperBound", str(upper_bound))
                .load()
            )

    def write(self, frame: DataFrame) -> None:
        (
            frame.write.format("jdbc")
            .mode(self.save_mode)
            .options(**self.connection_properties)
            .save()
        )
