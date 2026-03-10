import logging
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, LongType, BooleanType,
                              ArrayType, DateType, FloatType, TimestampType)

logger = logging.getLogger(__name__)

class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pedidos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pedidos."""
        return StructType([
            StructField("ID_PEDIDO", StringType(), True),
            StructField("PRODUTO", StringType(), True),
            StructField("VALOR_UNITARIO", FloatType(), True),
            StructField("QUANTIDADE", LongType(), True),
            StructField("DATA_CRIACAO", TimestampType(), True),
            StructField("UF", StringType(), True),
            StructField("ID_CLIENTE", LongType(), True),
        ])

    def _get_schema_pagamentos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pagamentos."""
        return StructType([
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score", FloatType(), True),
            ]), True),
            StructField("data_processamento", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("id_pedido", StringType(), True),
            StructField("status", BooleanType(), True),
            StructField("valor_pagamento", FloatType(), True),
        ])

    def load_pagamentos(self, compression: str, path: str) -> DataFrame:
        """Carrega o dataframe de pagamentos a partir de um arquivo JSON."""
        try:
            schema = self._get_schema_pagamentos()
            df = self.spark.read.option("compression", compression).option("mode", "FAILFAST").json(path, schema=schema)
            if df.isEmpty():
                logger.warning(f"ATENÇÃO: O arquivo em '{path}' foi lido mas não contém registros.")
            return df
        except AnalysisException as e:
            logger.error(f"Erro ao ler arquivo: {e}")
            raise e
        except Py4JJavaError as e:
            logger.critical(f"Erro Crítico na JVM (possível arquivo corrompido ou erro de memória): {e}")
            raise e        

    def load_pedidos(self, compression: str, path: str, header:bool, sep:str) -> DataFrame:
        """Carrega o dataframe de pedidos a partir de um arquivo CSV."""
        try:
            schema = self._get_schema_pedidos()
            df = self.spark.read.option("compression", compression).option("mode", "FAILFAST").csv(path, header=header, schema=schema, sep=sep)
            if df.isEmpty():
                logger.warning(f"ATENÇÃO: O arquivo em '{path}' foi lido mas não contém registros.")
            return df
        except AnalysisException as e:
            logger.error(f"Erro ao ler arquivo: {e}")
            raise e
        except Py4JJavaError as e:
            logger.critical(f"Erro Crítico na JVM (possível arquivo corrompido ou erro de memória): {e}")
            raise e        

    def write_parquet(self, df: DataFrame, path: str):
        """
        Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.

        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        """
        df.write.mode("overwrite").parquet(path)
        logger.info(f"Dados salvos com sucesso em: {path}")