from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """

    def join_pedidos_pagamentos(
        self, pedidos_df: DataFrame, pagamentos_df: DataFrame
    ) -> DataFrame:
        """Faz a junção entre os DataFrames de pedidos e pagamentos"""
        pagamentos_filtered = pagamentos_df.select(
            F.col("id_pedido"),
            F.col("status"),
            F.col("avaliacao_fraude.fraude").alias("fraude"),
            F.col("data_processamento"),
            F.col("forma_pagamento"),
            F.col("valor_pagamento"),
        )
        df_final = pedidos_df.join(pagamentos_filtered, on="id_pedido", how="inner")
        return df_final

    def relatorio(self, df: DataFrame) -> DataFrame:
        try:
            logger.info(
                "Iniciando Transformer: filtros de negócio (2025, status=false, fraude=false)"
            )

            df_filtrado = df.filter(
                (F.year(F.col("DATA_CRIACAO")) == 2025)
                & (F.col("status")==False)
                & (F.col("fraude")==False)
            )

            df_relatorio = df_filtrado.select(
                F.col("ID_PEDIDO").alias("id_pedido"),
                F.col("UF").alias("uf"),
                F.col("forma_pagamento").alias("forma_pagamento"),
                F.col("valor_pagamento").alias("valor_pagamento"),
                F.col("data_processamento").alias("data_processamento"),
            )

            df_ordenado = df_relatorio.orderBy(
                F.col("uf"), F.col("forma_pagamento"), F.col("data_processamento")
            )

            logger.info(
                "Transformer: seleção de colunas + ordenação aplicada com sucesso"
            )

            return df_ordenado

        except Exception as e:
            logger.error(f"Erro no Transformer (regras de negócio): {e}", exc_info=True)
            raise
