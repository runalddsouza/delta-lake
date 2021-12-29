import time

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, unix_timestamp, to_timestamp

from jobs.common.configuration import table_stream_config, Configuration
from jobs.common.functions import random_range_udf
from jobs.common.spark_job import SparkJob


class Producer(SparkJob):

    def __init__(self, config: Configuration):
        super().__init__()
        self.config = config

    def generate_data(self) -> DataFrame:
        return self.spark.range(1, 1000) \
            .withColumn("current_price", random_range_udf(lit(500), lit(5000))) \
            .withColumn("profit_percent", random_range_udf(lit(1), lit(100))) \
            .withColumn("selling_price", col("current_price") + (col("current_price") * col("profit_percent") / 100)) \
            .withColumn("time", to_timestamp(unix_timestamp()))

    def execute(self) -> None:
        df = self.generate_data()
        df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.input) \
            .option("kafka.security.protocol", "PLAINTEXT") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.kerberos.service.name", "kafka") \
            .option("topic", self.config.table) \
            .save()

    def init_spark(self) -> SparkSession:
        return pyspark.sql.SparkSession.builder.appName(f"kafka_producer") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
            .master("local[1]").getOrCreate()


if __name__ == '__main__':
    # produce data at regular intervals
    while True:
        timeout = 30
        Producer(table_stream_config()).run()
        print(f"Sleeping for {timeout} seconds")
        time.sleep(timeout)
