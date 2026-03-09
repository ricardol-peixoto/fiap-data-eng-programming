from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

import os
from pathlib import Path

def main():
    config = carregar_config()[0]
    app_name = config['spark']['app_name']
    print(f"Obtido o app name: {app_name}")

    main_root_dir = Path(os.getcwd()) # Obtém o caminho atual. Será utilizado para apontar para os inputs/outputs de forma modular e agnóstica.

    # print("Abrindo a sessao spark")
    spark = SparkSessionManager.get_spark_session(app_name=app_name)

    data_handler = DataHandler(spark)
    transformer = Transformation()
    
    print("Abrindo o dataframe de pedidos")
    path_pedidos = main_root_dir.parent / config['paths']['pedidos']
    path_pedidos_str = str(path_pedidos)
    print(f"Obtidos o path de pedidos: {path_pedidos_str}")
    print("Carregando parâmetros de leitura das informações de pedidos")
    compression_pedidos = config['file_options']['pedidos_csv']['compression']
    header_pedidos = config['file_options']['pedidos_csv']['header']
    separator_pedidos = config['file_options']['pedidos_csv']['sep']
    pedidos = data_handler.load_pedidos(path=path_pedidos_str, compression=compression_pedidos, header=header_pedidos, sep=separator_pedidos)
    print("Dados de pedidos carregados")
    pedidos.show(5, truncate=False)

    print("Abrindo o dataframe de pagamentos")
    path_pagamentos = main_root_dir.parent / config['paths']['pagamentos']
    path_pagamentos_str = str(path_pagamentos)
    print(f"Obtidos o path de pagamentos: {path_pedidos_str}")
    print("Carregando parâmetros de leitura das informações de pedidos")
    compression_pagamentos = config['file_options']['pagamentos_json']['compression']
    pagamentos = data_handler.load_pagamentos(path=path_pagamentos_str, compression=compression_pagamentos)
    pagamentos.show(5, truncate=False)

    print("Fazendo a junção dos dataframes")
    pedidos_pagamentos = transformer.join_pedidos_pagamentos(pedidos, pagamentos)

    print("Extraindo Relatório Final")
    relatorio_final = transformer.relatorio(pedidos_pagamentos)
    relatorio_final.show(10, truncate=False)

    # Hora de Gravar o Relatório Final com os pedidos legítimos e que foram recusados.
    print("Escrevendo o resultado em parquet")
    # Nosso grupo usou mais de uma ferramenta para estruturar o Trabalho Final.
    # Se estiver no Databricks, ele utilizará a variável 'DATABRICKS_RUNTIME_VERSION'
    is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if is_databricks:
        # Caminho para o "Storage" gratuito do Databricks. 
        # É necessário criar o volume (fiap-data-eng-programming) e o schema (data-programming-trab-final).
        base_out = "/Volumes/workspace/fiap-data-eng-programming/data-programming-trab-final/"
        print(f"Obtido o path de saída: {base_out}")
        data_handler.write_parquet(df=relatorio_final, path=base_out)
    else:
        # Caminho para sua máquina local (pasta do projeto)
        base_out = config['paths']['output']
        print(f"Obtido o path de saída: {base_out}")
        data_handler.write_parquet(df=relatorio_final, path=base_out)    
    
    spark.stop()

if __name__ == "__main__":
    main()