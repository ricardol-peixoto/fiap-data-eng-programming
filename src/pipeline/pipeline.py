from pyspark.sql import SparkSession
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
import os
from pathlib import Path

import logging

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_handler = DataHandler(self.spark)
        self.transformer = Transformation()

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        logger.info("Pipeline iniciado...")

        main_root_dir = Path(
            os.getcwd()
        )  # Obtém o caminho atual. Será utilizado para apontar para os inputs/outputs de forma modular e agnóstica.

        logger.info("Abrindo o dataframe de pedidos")
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            path_pedidos = main_root_dir.parent / config["paths"]["pedidos"]
        else:
            path_pedidos = main_root_dir.parent / config["paths"]["pedidos"]

        path_pedidos_str = str(path_pedidos)
        logger.info(f"Obtidos o path de pedidos: {path_pedidos_str}")
        logger.info("Carregando parâmetros de leitura das informações de pedidos")
        compression_pedidos = config["file_options"]["pedidos_csv"]["compression"]
        header_pedidos = config["file_options"]["pedidos_csv"]["header"]
        separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]
        pedidos = self.data_handler.load_pedidos(
            path=path_pedidos_str,
            compression=compression_pedidos,
            header=header_pedidos,
            sep=separator_pedidos,
        )
        logger.info("Dados de pedidos carregados")
        pedidos.show(5, truncate=False)

        logger.info("Abrindo o dataframe de pagamentos")
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            path_pagamentos = main_root_dir.parent / config["paths"]["pagamentos"]
        else:
            path_pagamentos = main_root_dir.parent / config["paths"]["pagamentos"]

        path_pagamentos_str = str(path_pagamentos)
        logger.info(f"Obtidos o path de pagamentos: {path_pedidos_str}")
        logger.info("Carregando parâmetros de leitura das informações de pedidos")
        compression_pagamentos = config["file_options"]["pagamentos_json"][
            "compression"
        ]
        pagamentos = self.data_handler.load_pagamentos(
            path=path_pagamentos_str, compression=compression_pagamentos
        )
        pagamentos.show(5, truncate=False)

        logger.info("Fazendo a junção dos dataframes")
        pedidos_pagamentos = self.transformer.join_pedidos_pagamentos(
            pedidos, pagamentos
        )

        logger.info("Extraindo Relatório Final")
        relatorio_final = self.transformer.relatorio(pedidos_pagamentos)
        relatorio_final.show(10, truncate=False)

        # Hora de Gravar o Relatório Final com os pedidos legítimos e que foram recusados.
        logger.info("Escrevendo o resultado em parquet")
        # Nosso grupo usou mais de uma ferramenta para estruturar o Trabalho Final.
        # Se estiver no Databricks, ele utilizará a variável 'DATABRICKS_RUNTIME_VERSION'
        is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

        if is_databricks:
            # Caminho para o "Storage" gratuito do Databricks.
            # É necessário criar o volume (fiap-data-eng-programming) e o schema (data-programming-trab-final).
            base_out = "/Volumes/workspace/fiap-data-eng-programming/data-programming-trab-final/"
            logger.info(f"Obtido o path de saída: {base_out}")
            self.data_handler.write_parquet(df=relatorio_final, path=base_out)
        else:
            # Caminho para sua máquina local (pasta do projeto)
            base_out = config["paths"]["output"]
            logger.info(f"Obtido o path de saída: {base_out}")
            self.data_handler.write_parquet(df=relatorio_final, path=base_out)

        logger.info("Pipeline concluído com sucesso!")
