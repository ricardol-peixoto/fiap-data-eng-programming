from pyspark.sql import SparkSession, DataFrame
from src.log_settings import logger


class Extractor:

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def run(self) -> DataFrame:
        logger.info("todo: implementar extrator")
        return self.spark.createDataFrame([('TESTE', 1)]) #todo: implementar extrator