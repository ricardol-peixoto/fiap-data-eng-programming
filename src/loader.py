from pyspark.sql import SparkSession, DataFrame
from src.log_settings import logger


class Loader:

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def run(self, df: DataFrame):
        logger.info("todo: implementar loader")
        pass # todo: implementar loader

