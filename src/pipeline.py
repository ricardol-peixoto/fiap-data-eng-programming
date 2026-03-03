from pyspark.sql import SparkSession

from src.extractor import Extractor
from src.loader import Loader
from src.transformer import Transformer
from src.log_settings import logger

class Pipeline:

    def __init__(self, spark_session: SparkSession):
        self.spark       = spark_session
        self.extractor   = Extractor(spark_session)
        self.transformer = Transformer(spark_session)
        self.loader      = Loader(spark_session)

    def run(self):
        df = self.extractor.run()
        df = self.transformer.run(df)
        self.loader.run(df)