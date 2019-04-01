from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "Spark App") -> SparkSession:
    return (SparkSession.builder
            # .enableHiveSupport()
            .appName(app_name)
            # .config('spark.useHiveContext', 'true')
            .getOrCreate())
