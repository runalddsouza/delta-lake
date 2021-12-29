from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, to_timestamp, unix_timestamp, col

from build_table import BuildTable
from jobs.common.configuration import *
from jobs.common.functions import random_range_udf


class PricingBuildTable(BuildTable):
    def load_df(self) -> DataFrame:
        return self.spark.range(1, 1000) \
            .withColumn("current_price", random_range_udf(lit(500), lit(5000))) \
            .withColumn("profit_percent", random_range_udf(lit(1), lit(100))) \
            .withColumn("selling_price", col("current_price") + (col("current_price") * col("profit_percent") / 100)) \
            .withColumn("time", to_timestamp(unix_timestamp()))

    def transform(self, df) -> DataFrame:
        return df


if __name__ == '__main__':
    PricingBuildTable(table_load_config()).run()
