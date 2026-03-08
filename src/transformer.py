from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year
from src.log_settings import logger

class Transformer:

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def run(self, df: DataFrame) -> DataFrame:
        try:
            logger.info("Iniciando Transformer: filtros de negócio (2025, status=false, fraude=false)")

            df_filtrado = df.filter(
                (year(col("DATA_CRIACAO")) == 2025) &
                (col("status") == False) &
                (col("fraude") == False)
            )
            
            df_relatorio = df_filtrado.select(
                col("ID_PEDIDO").alias("id_pedido"),
                col("UF").alias("uf"),
                col("forma_pagamento").alias("forma_pagamento"),
                col("valor_pagamento").alias("valor_pagamento"),
                col("data_processamento").alias("data_processamento")
            )
            
            df_ordenado = df_relatorio.orderBy(
                col("uf"),
                col("forma_pagamento"),
                col("data_processamento")
            )

            logger.info("Transformer: seleção de colunas + ordenação aplicada com sucesso")
            
            return df_ordenado            

        except Exception as e:
            logger.error(f"Erro no Transformer (regras de negócio): {e}", exc_info=True)
            raise