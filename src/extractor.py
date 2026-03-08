from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from src.log_settings import logger
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DoubleType, TimestampType
)

PEDIDOS_SCHEMA = StructType([
    StructField("ID_PEDIDO", StringType(), True),
    StructField("PRODUTO", StringType(), True),
    StructField("VALOR_UNITARIO", StringType(), True),
    StructField("QUANTIDADE", DoubleType(), True),
    StructField("DATA_CRIACAO", TimestampType(), True),
    StructField("UF", StringType(), True),
    StructField("ID_CLIENTE", StringType(), True),
])

PAGAMENTOS_SCHEMA = StructType([
    StructField("avaliacao_fraude", StructType([
        StructField("fraude", BooleanType(), True),
        StructField("score", DoubleType(), True),
    ]), True),

    StructField("data_processamento", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("id_pedido", StringType(), True),
    StructField("status", BooleanType(), True),
    StructField("valor_pagamento", DoubleType(), True),
])

class Extractor:

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def run(self) -> DataFrame:

        logger.info("Lendo dados de pedidos para identificar schema")

        df_pagamentos_full = (
                    self.spark.read
                    .option("multiline", True)
                    .schema(PAGAMENTOS_SCHEMA)
                    .json("/Workspace/Users/beatrizpinheiror@gmail.com/fiap-data-eng-programming/data/input/dataset-json-pagamentos/data/pagamentos/*.json.gz")
                )
        
        df_pagamentos = df_pagamentos_full.select(
                    col("id_pedido"),
                    col("status"),
                    col("avaliacao_fraude.fraude").alias("fraude"),
                    col("data_processamento"),
                    col("forma_pagamento"),
                    col("valor_pagamento")
                )

        df_pedidos = (
                    self.spark.read
                    .option("header", True)
                    .schema(PEDIDOS_SCHEMA)
                    .option("sep", ";")
                    .csv("/Workspace/Users/beatrizpinheiror@gmail.com/fiap-data-eng-programming/data/input/datasets-csv-pedidos/data/pedidos/*.csv.gz")
                )
        
        df_final = df_pedidos.join(
        df_pagamentos,
        on="id_pedido",
        how="inner"
    )
        return df_final
    


















