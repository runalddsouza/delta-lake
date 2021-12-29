from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, LongType

from jobs.common.configuration import table_stream_config, Configuration
from jobs.common.functions import get_partition_udf
from jobs.delta_table_stream.stream_table import StreamTable


class PricingStream(StreamTable):

    def __init__(self, config: Configuration, checkpoint):
        super().__init__(config, checkpoint)

    def transform(self, df) -> DataFrame:
        schema = StructType([StructField("id", LongType(), True),
                             StructField("current_price", DoubleType(), True),
                             StructField("profit_percent", DoubleType(), True),
                             StructField("selling_price", DoubleType(), True),
                             StructField("time", TimestampType(), True)],
                            )

        return df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value") \
            .select(from_json("value", schema).alias("value")) \
            .select("value.*") \
            .withColumn(self.config.partition_key,
                        get_partition_udf(col(self.config.key), lit(self.config.partition_keys)))

    def upsert_to_delta(self, batch, batch_id):
        DeltaTable.forPath(self.spark, self.delta_table_path).alias("t") \
            .merge(batch.alias("s"),
                   f"s.{self.config.key} = t.{self.config.key} and s.{self.config.partition_key} = t.{self.config.partition_key}") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    def write_stream(self, df) -> None:
        df.writeStream \
            .trigger(processingTime="20 seconds") \
            .format("delta") \
            .option("checkpointLocation", self.checkpoint) \
            .foreachBatch(self.upsert_to_delta) \
            .outputMode("update") \
            .start() \
            .awaitTermination()

    def load_stream(self) -> DataFrame:
        return self.spark.readStream.format("kafka").option("kafka.bootstrap.servers", self.config.input) \
            .option("kafka.security.protocol", "PLAINTEXT") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.kerberos.service.name", "kafka") \
            .option("subscribe", self.config.table) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()


if __name__ == '__main__':
    PricingStream(table_stream_config(), "/usr/checkpoint").run()
