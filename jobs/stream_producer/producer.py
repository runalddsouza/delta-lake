import random
import time
from argparse import ArgumentParser

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, lit, col, unix_timestamp, to_timestamp
from pyspark.sql.types import IntegerType

from jobs.common.spark_job import SparkJob


class Producer(SparkJob):

    def __init__(self, config):
        super().__init__()
        self.config = config

    def generate_data(self) -> DataFrame:
        price_udf = udf(lambda s, e: random.randint(int(s), int(e)), IntegerType())

        return self.spark.range(1, 1000) \
            .withColumn("current_price", price_udf(lit(500), lit(5000))) \
            .withColumn("profit_percent", price_udf(lit(1), lit(100))) \
            .withColumn("selling_price", col("current_price") + (col("current_price") * col("profit_percent") / 100)) \
            .withColumn("time", to_timestamp(unix_timestamp()))

    def execute(self) -> None:
        df = self.generate_data()
        df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['bootstrap.servers']) \
            .option("kafka.security.protocol", self.config['security.protocol']) \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.kerberos.service.name", "kafka") \
            .option("topic", self.config['topic']) \
            .save()

    def init_spark(self) -> SparkSession:
        return pyspark.sql.SparkSession.builder.appName(f"kafka_producer") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
            .master("local[*]").getOrCreate()


if __name__ == '__main__':
    # get cli params
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--topic", required=True, help="Kafka topic")
    arg_parser.add_argument("--servers", required=True, help="Bootstrap server")
    args = arg_parser.parse_args()

    # kafka
    topic = args.topic
    producer_conf = {'bootstrap.servers': args.servers, 'security.protocol': 'PLAINTEXT', 'topic': topic}

    # produce data at regular intervals
    while True:
        timeout = 10
        Producer(producer_conf).run()
        print(f"Sleeping for {timeout} seconds")
        time.sleep(timeout)
