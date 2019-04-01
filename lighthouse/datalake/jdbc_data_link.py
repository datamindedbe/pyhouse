from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as sfun

from .data_link import DataLink


class JdbcDataLink(DataLink):

    def __init__(self, environment: str, session: SparkSession,
                 url: str, username: str, password: str, driver: str,
                 table: str,  # extraProperties: Map[str, str],
                 partition_column: str = "",
                 number_of_partitions: int = 0,
                 # batchSize: int = 50000,
                 save_mode: str = "error"):
        super().__init__(environment, session)

        self.numberOfPartitions = number_of_partitions
        self.partitionColumn = partition_column
        self.save_mode = save_mode
        self.connection_properties = {
            "url": url,
            "user": username,
            "password": password,
            "driver": driver,
            "dbtable": table,
        }

    def read(self):
        # Partition parameters are only applicable for read operation
        # for now, order is important as some values can be overwritten
        reader = (self.spark.read
                  .format("jdbc")
                  .options(**self.connection_properties))

        # Retrieve lower and upper bound first to
        # determine the degree of parallelism
        lower_bound, upper_bound = (
            reader.load()
                .select(sfun.min(sfun.col(self.partitionColumn)).alias("mmin"),
                        sfun.max(sfun.col(self.partitionColumn)).alias("mmax"))
                .collect()[0])

        return (reader
                .option("partitionColumn", self.partitionColumn)
                .option("numPartitions", str(self.numberOfPartitions))
                .option("lowerBound", str(lower_bound))
                .option("upperBound", str(upper_bound))
                .load())

    def write(self, frame: DataFrame):
        (frame.write
         .format("jdbc")
         .mode(self.save_mode)
         .options(**self.connection_properties)
         .save())