from abc import ABC, abstractmethod

import pyspark
from pyspark.sql import SparkSession, DataFrame

from jobs.common.configuration import Configuration
from jobs.common.spark_stream_job import SparkStreamJob


class StreamTable(SparkStreamJob, ABC):

    def __init__(self, config: Configuration, checkpoint):
        super().__init__()
        self.checkpoint = checkpoint
        self.spark = None
        self.config = config
        self.delta_table_path = f"{self.config.prefix}/{self.config.table}"

    def init_spark(self) -> SparkSession:
        return pyspark.sql.SparkSession.builder.appName(f"delta_tbl_stream") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config('spark.databricks.delta.schema.autoMerge.enabled', True) \
            .getOrCreate()

    def process_stream(self, source_df):
        self.write_stream(self.transform(source_df))

    @abstractmethod
    def transform(self, df) -> DataFrame:
        pass

    @abstractmethod
    def write_stream(self, df) -> None:
        pass
