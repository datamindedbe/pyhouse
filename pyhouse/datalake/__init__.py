from typing import Callable

from pyspark.sql import DataFrame

from .data_link import DataLink


def sink(self: DataFrame, dl: DataLink) -> None:
    """Write a DataFrame to a DataLink.
    Spark SQL allows ETL jobs to be written using chained transformations, like
    `spark.read.parquet().transform(func1).transform(func2).write(destination)`
    where `destination` is a string. The DataLinks introduced in LightHouse do
    not support this intuitive paradigm, unless DataFrames are monkey-patched.
    """
    dl.write(frame=self)


def transform(self: DataFrame, fun: Callable[[DataFrame], DataFrame]) -> DataFrame:
    """Monkey-patch pyspark DataFrames so they have a `transform`
    method as in (Scala) Spark."""
    return fun(self)


DataFrame.sink = sink
DataFrame.transform = transform
