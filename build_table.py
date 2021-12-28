from abc import ABC, abstractmethod

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import functions
from configuration import Configuration
from spark_job import SparkJob


class BuildTable(SparkJob, ABC):

    def __init__(self, config: Configuration):
        super().__init__()
        self.spark = None
        self.config = config
        self.delta_table_path = f"{self.config.prefix}/{self.config.table}"

    def init_spark(self) -> SparkSession:
        return pyspark.sql.SparkSession.builder.appName(f"delta_tbl_initial_load_{self.config.table}") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config('spark.databricks.delta.schema.autoMerge.enabled', True) \
            .getOrCreate()

    def execute(self):
        df = self.transform(self.load_df())
        df.withColumn(self.config.partition_key,
                      functions.get_partition_udf(F.col(self.config.key), F.lit(self.config.partition_keys))) \
            .repartition(self.config.partitions) \
            .write \
            .partitionBy(self.config.partition_key) \
            .mode('overwrite') \
            .format("delta") \
            .save(self.delta_table_path)

    @abstractmethod
    def load_df(self) -> DataFrame:
        pass

    @abstractmethod
    def transform(self, df) -> DataFrame:
        pass
