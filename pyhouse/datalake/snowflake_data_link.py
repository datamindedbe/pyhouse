from pyspark.sql import SparkSession, DataFrame

from .data_link import DataLink


class SnowflakeDataLink(DataLink):
    def __init__(
        self,
        environment: str,
        session: SparkSession,
        account: str,
        username: str,
        password: str,
        database: str,
        schema: str,
        table: str,
        warehouse: str,
        role: str,
        parallelism: int = 1,
        save_mode: str = "error",
    ):
        super().__init__(environment, session)

        self.save_mode = save_mode

        self.sfOptions = {
            "sfURL": f"{account}.snowflakecomputing.com",
            "sfUser": username,
            "sfPassword": password,
            "sfDatabase": database,
            "sfSchema": schema,
            "sfWarehouse": warehouse,
            "dbtable": table,
            "parallelism": parallelism,
            "role": role,
        }

        self.connection_format = "net.snowflake.spark.snowflake"

    def read(self) -> DataFrame:
        # Partition parameters are only applicable for read operation
        # for now, order is important as some values can be overwritten
        return self.spark.read.format(self.connection_format).options(**self.sfOptions).load()

    def write(self, frame: DataFrame) -> None:
        (
            frame.write.format(self.connection_format)
            .mode(self.save_mode)
            .options(**self.sfOptions)
            .save()
        )
