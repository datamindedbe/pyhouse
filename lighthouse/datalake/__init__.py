from pyspark.sql import DataFrame
from .data_link import DataLink


def sink(self, dl: DataLink):
    """Write a DataFrame to a DataLink.
    Spark SQL allows ETL jobs to be written using chained transformations, like
    `spark.read.parquet().transform(func1).transform(func2).write(destination)`
    where `destination` is a string. The DataLinks introduced in LightHouse do
    not support this intuitive paradigm, unless DataFrames are monkey-patched.
    """
    dl.write(frame=self)


DataFrame.sink = sink
