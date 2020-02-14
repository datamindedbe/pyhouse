# pyhouse

This is a port of [Lighthouse](https://github.com/datamindedbe/lighthouse), 
a library written in Scala, that facilitates the creation of data pipelines that
are based on [Apache Spark](https://spark.apache.org/). It also comes with some
related convenience functions, like integrations to the AWS parameter store.
 
This port is targeted at Python and PySpark. It is not an exact port of the 
Scala code: we add what we need as we go along.

## Usage

One of this library’s main usages is to build a class-based data catalog, that 
supports chaining of sources. For example, if you had a dataset in a text file
that needed to be transformed (clean, derive some statistic, …) then you could
write this as such:

```python
from pyhouse.datalake.file_system_data_link import FileSystemDataLink

link = FileSystemDataLink(
    environment="dev",
    session = get_spark(),
    path = "s3://bucket-foo/file-bar.csv",
    format="csv",
    savemode="errorifexists",
    partitioned_by=("some-key", "another-key"),
    options={"header": True, "sep": "\t"}
)

link.read().groupBy("client").count().show()
```

The advantage of such data links becomes clear when there are multiple of them
that are combined in a module (the “catalog”): there would be one source of 
truth that many scripts can refer to. Hardcoded paths scattered across scripts 
would be a thing of the past.
