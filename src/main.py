from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, ArrayType, DateType, FloatType, BooleanType, TimestampType)
import os
from pathlib import Path
from config.settings import carregar_config
from io_utils.data_handler import DataHandler

config = carregar_config()[0]

main_root_dir = Path(os.getcwd()) 

app_name = config['spark']['app_name']
print(f"Obtido o app name: {app_name}")


# print("Abrindo a sessao spark")
from session.spark_session import SparkSessionManager
spark = SparkSessionManager.get_spark_session(app_name=app_name)

dh = DataHandler(spark)

print("Abrindo o dataframe de pedidos")
path_pedidos = main_root_dir.parent / config['paths']['pedidos']
compression_pedidos = config['file_options']['pedidos_csv']['compression']
header_pedidos = config['file_options']['pedidos_csv']['header']
separator_pedidos = config['file_options']['pedidos_csv']['sep']
print(f"""
Obtidos os seguintes parâmetros de pedidos: 
- path: {path_pedidos}
- compression_pedidos: {compression_pedidos}
- header_pedidos: {header_pedidos}
- separator_pedidos: {separator_pedidos}
""")
path_pedidos_str = str(path_pedidos)
pedidos = dh.load_pedidos(path=path_pedidos_str, compression=compression_pedidos, header=header_pedidos, sep=separator_pedidos)
pedidos.printSchema()
pedidos.show(5, truncate=False)

print("Abrindo o dataframe de pagamentos")
path_pagamentos = main_root_dir.parent / config['paths']['pagamentos']
compression_pagamentos = config['file_options']['pagamentos_json']['compression']
print(f"Obtido o path de pagamentos: {path_pagamentos}")
path_pagamentos_str = str(path_pagamentos)
pagamentos = dh.load_pagamentos(path=path_pagamentos_str, compression=compression_pagamentos)


pagamentos.printSchema()
pagamentos.show(5, truncate=False)

print("Escrevendo o resultado em parquet")
path_output = config['paths']['output']
print(f"Obtido o path de saída: {path_output}")
dh.write_parquet(df=pagamentos, path=path_output)



spark.stop()