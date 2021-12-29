from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame


class SparkStreamJob(ABC):

    def __init__(self):
        self.spark = None

    def run(self):
        self.spark = self.init_spark()
        self.process_stream(self.load_stream())

    @abstractmethod
    def init_spark(self) -> SparkSession:
        pass

    @abstractmethod
    def process_stream(self, source_df: DataFrame) -> None:
        pass

    @abstractmethod
    def load_stream(self) -> DataFrame:
        pass
