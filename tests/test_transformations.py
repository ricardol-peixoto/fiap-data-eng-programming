import pytest
from pyspark.sql import SparkSession
from src.processing.transformations import Transformation
from src.io_utils.data_handler import DataHandler
from pyspark.sql import functions as F, Row
from pyspark.sql.types import *

@pytest.fixture(scope="session")
def spark_session():
    """
    Cria uma SparkSession para ser usada em todos os testes.
    A sessão é finalizada automaticamente ao final da execução dos testes.
    """
    spark = SparkSession.builder \
        .appName("PySpark Unit Tests") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

# Fixture para criar o Mock de Pagamentos
@pytest.fixture
def df_pagamentos_mock(spark_session):
    data_handler = DataHandler()
    schema = data_handler._get_schema_pagamentos()
    data = [
        # Exemplo: Pedido P1, Sem fraude, Score 0.99
        Row(
            avaliacao_fraude={"fraude": False, "score": 0.99},
            data_processamento="2025-05-22",
            forma_pagamento="CARTAO_CREDITO",
            id_pedido="6d864f53-6b6d-4632-9240-1d86fcad4c66",
            status=True,
            valor_pagamento=2375.0
        )
    ]
    return spark.createDataFrame(data, schema)

# TESTE 1: Extração de campo aninhado do JSON (Flattening)
def test_extracao_score_fraude(df_pagamentos_mock):
    df_transformed = df_pagamentos_mock.select(
        F.col("id_pedido"), 
        F.col("avaliacao_fraude.fraude").alias("fraude")
    )
    
    result = df_transformed.collect()[0]
    assert result["fraude"] == False
    assert "avaliacao_fraude" not in df_transformed.columns

# TESTE 2: Validação de Join (Pedidos x Pagamentos)
def test_join_pedidos_pagamentos(spark_session, df_pagamentos_mock):
    data_handler = DataHandler()    
    schema_pedidos = data_handler._get_schema_pedidos()
    data_pedidos = [("6d864f53-6b6d-4632-9240-1d86fcad4c66", "Prod A", 50.0, 2, None, "SP", 1001)] # Total 100.0
    df_pedidos = spark.createDataFrame(data_pedidos, schema_pedidos)
    
    df_final = df_pedidos.join(df_pagamentos_mock, on="id_pedido")
    
    assert df_final.count() == 1
    assert "PRODUTO" in df_final.columns
    assert "forma_pagamento" in df_final.columns

# TESTE 3: Regra de Negócio - Status de Pagamento
def test_filtro_pagamentos_rejeitados(spark_session):
    data_handler = DataHandler()        
    schema = data_handler._get_schema_pagamentos()
    data = [
        Row({"fraude": False, "score": 1.0}, "2023-01-01", "PIX", "P1", True, 50.0), # OK
        Row({"fraude": True, "score": 0.1}, "2023-01-01", "PIX", "P2", False, 50.0) # REJEITADO
    ]
    df_raw = spark.createDataFrame(data, schema)
    
    df_filtrado = df_raw.filter(col("status") == True)
    
    assert df_filtrado.count() == 1
    assert df_filtrado.collect()[0]["id_pedido"] == "P1"


def test_logica_filtros_relatorio(spark_session):
    transformer = Transformation(spark)

    data = [
        # Registro Válido (2025, status=False, fraude=False)
        Row(ID_PEDIDO="P1", UF="SP", DATA_CRIACAO="2025-01-01 10:00:00", 
            status=False, fraude=False, forma_pagamento="PIX", valor_pagamento=100.0, data_processamento="2025-01-01"),
        
        # Filtra por Ano (2024)
        Row(ID_PEDIDO="P2", UF="RJ", DATA_CRIACAO="2024-12-31 23:59:59", 
            status=False, fraude=False, forma_pagamento="Boleto", valor_pagamento=50.0, data_processamento="2025-01-01"),
        
        # Filtra por Status (True)
        Row(ID_PEDIDO="P3", UF="MG", DATA_CRIACAO="2025-02-01 10:00:00", 
            status=True, fraude=False, forma_pagamento="Cartão", valor_pagamento=80.0, data_processamento="2025-02-01"),
        
        # Filtra por Fraude (True)
        Row(ID_PEDIDO="P4", UF="SC", DATA_CRIACAO="2025-03-01 10:00:00", 
            status=False, fraude=True, forma_pagamento="PIX", valor_pagamento=200.0, data_processamento="2025-03-01")
    ]
    
    # Criamos o DF com o schema necessário (convertendo DATA_CRIACAO para Timestamp)
    df_input = spark.createDataFrame(data).withColumn("DATA_CRIACAO", F.to_timestamp("DATA_CRIACAO"))
    
    # Chamamos sua função de filtragem
    df_resultado = transformer.relatorio(df_input)
    
    # Asserts
    assert df_resultado.count() == 1, "O filtro deveria ter retornado apenas 1 registro"
    assert df_resultado.collect()[0]["ID_PEDIDO"] == "P1"