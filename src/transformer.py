from pyspark.sql import SparkSession, DataFrame
from src.log_settings import logger

class Transformer:

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def run(self, df: DataFrame) -> DataFrame:
        logger.info("todo: implementar transformer")
        return df # todo: implementar transformer