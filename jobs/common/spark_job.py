from abc import ABC, abstractmethod

from pyspark.sql import SparkSession


class SparkJob(ABC):

    def __init__(self):
        self.spark = None

    def run(self):
        self.spark = self.init_spark()
        self.execute()
        self.spark.stop()

    @abstractmethod
    def init_spark(self) -> SparkSession:
        pass

    @abstractmethod
    def execute(self) -> None:
        pass