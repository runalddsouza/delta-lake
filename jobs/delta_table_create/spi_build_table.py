import io

import pandas as pd
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from build_table import BuildTable
from jobs.common.configuration import *


class SpiBuildTable(BuildTable):
    def load_df(self) -> DataFrame:
        return self.spark.createDataFrame(
            pd.read_csv(io.StringIO(requests.get(self.config.input).content.decode('utf-8'))))

    def transform(self, df) -> DataFrame:
        return df.withColumn(self.config.key, expr("uuid()"))


if __name__ == '__main__':
    SpiBuildTable(table_load_config()).run()
