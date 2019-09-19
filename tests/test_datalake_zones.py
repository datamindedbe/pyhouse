import unittest

from pyhouse.datalake.file_system_data_link import FileSystemDataLink
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf


def relative_to_file(path: str) -> str:
    import os
    script_dir = os.path.dirname(__file__)
    return str(os.path.join(script_dir, path))


class Ingress:
    def __init__(self, environment: str, session: SparkSession):
        self.customers = FileSystemDataLink(
            environment,
            session,
            path=relative_to_file("data/customers_example.json"),
            format="json",
            savemode="errorifexists",
            options={"mode": "FAILFAST", "encoding": "utf-8"},
        )


class Landing:
    def __init__(self, environment: str, session: SparkSession):
        self.customersParquet = FileSystemDataLink(
            environment,
            session,
            path=relative_to_file("data/customers_example.parquet"),
            format="parquet",
            savemode="overwrite",
            options={"mode": "FAILFAST", "encoding": "utf-8"},
        )


class TestDatalakeZones(unittest.TestCase):
    def test_ingest_sample_data(self) -> None:
        spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

        ingress = Ingress("TEST", spark)
        landing = Landing("TEST", spark)

        ingress.customers.read().sink(landing.customersParquet)
        spark.stop()


if __name__ == "__main__":
    unittest.main()
