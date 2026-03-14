# Trabalho Final - Data Engineering Programming
# DESAFIO RELATÓRIO DE PAGAMENTOS 
## FIAP - MBA em Data Engineering


**Prof.:** Marcelo Barbosa  
**Turma:** 8ABDR
**Alunos:**
| Nome | RM |
|---|---|
| Fátima Beatriz Pinheiro Rodrigues | RM 368775 |​
| Jean Felipe dos Santos Ertzogue | RM 366352 |​
| Luiz Adelar Soldatelli Neto | RM 367692 |​
| Mauricio Acedo de Aquino | RM 367519 |​
| Ricardo de Lima Peixoto | RM 369113 |
---
 
## 1. Descrição do Projeto
 
Este projeto implementa um pipeline de **ETL (Extract, Transform, Load)** utilizando **Apache PySpark**, desenvolvido como Trabalho Final da disciplina de Data Engineering Programming.
 
O código é organizado seguindo boas práticas de engenharia de software:
 
- **Orientação a Objetos** — todos os componentes encapsulados em classes
- **Injeção de Dependências** — `main.py` como *Aggregation Root*
- **Configurações Centralizadas** — parâmetros em `config/settings.yaml`
- **Schemas Explícitos** — nenhum DataFrame utiliza inferência de schema
- **Logging Estruturado** — rastreabilidade completa das etapas
- **Tratamento de Erros** — `try/except` com logging em toda a lógica de negócio
- **Testes Unitários** — suite com `pytest` cobrindo as regras de negócio
- **Agnóstico à Plataforma** — roda em Docker local ou Databricks sem mudanças no código
 
---
 
## 2. Arquitetura e Estrutura do Repositório
 
### Fluxo do Pipeline
 
```
main.py  (Aggregation Root)
   │
   ├── carregar_config()          ← config/settings.yaml
   ├── SparkSessionManager        ← cria SparkSession
   └── Pipeline.run()
         │
         ├── DataHandler.load_pedidos()     ← CSV (.gz)
         ├── DataHandler.load_pagamentos()  ← JSON (.gz)
         ├── Transformation.join_pedidos_pagamentos()
         ├── Transformation.relatorio()     ← filtros + ordenação
         └── DataHandler.write_parquet()    ← data/output/
```
 
### Estrutura de Diretórios
 
```
fiap-data-eng-programming/
│
├── config/
│   └── settings.yaml               # Parâmetros centralizados (Spark, paths, I/O)
│
├── data/
│   ├── input/
│   │   ├── dataset-json-pagamentos/
│   │   │   └── data/pagamentos/    # Arquivos JSON (.gz) de pagamentos
│   │   └── datasets-csv-pedidos/
│   │       └── data/pedidos/       # Arquivos CSV (.gz) de pedidos
│   └── output/
│       └── pedidos_legit_recusados/ # Parquet gerado pelo pipeline
│
├── src/
│   ├── __init__.py
│   ├── main.py                     # Aggregation Root — ponto de entrada
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py             # Classe de configuração (lê o YAML)
│   ├── session/
│   │   ├── __init__.py
│   │   └── spark_session.py        # SparkSessionManager
│   ├── io_utils/
│   │   ├── __init__.py
│   │   └── data_handler.py         # DataHandler (leitura CSV/JSON + escrita Parquet)
│   ├── processing/
│   │   ├── __init__.py
│   │   └── transformations.py      # Transformation (join + filtros + ordenação)
│   └── pipeline/
│       ├── __init__.py
│       └── pipeline.py             # Pipeline (orquestrador)
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # Configuração do PYTHONPATH para o pytest
│   └── test_transformations.py     # Testes unitários com pytest
│
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
├── MANIFEST.in
└── README.md
```
 
---
 
## 3. Tecnologias Utilizadas
 
| Tecnologia | Versão | Finalidade |
|---|---|---|
| Python | ≥ 3.12 | Linguagem principal |
| Apache Spark / PySpark | 4.0.0 | Processamento distribuído |
| PyYAML | 6.0.3 | Leitura de configurações |
| python-dotenv | ≥ 1.1.1 | Variáveis de ambiente |
| pyarrow | ≥ 21.0.0 | Suporte ao formato Parquet |
| pytest | 8.4.1 | Testes unitários |
| Docker / docker-compose | qualquer | Containerização |
| Databricks Runtime | qualquer | Execução em nuvem (alternativa) |
 
---
## 4. Plataformas de execução
O projeto foi pensado para ser agnóstico à plataforma escolhida.

## 4.1. Instruções de Execução `Linux`

Para rodar o projeto em um ambiente isolado, siga os passos abaixo:

1.  **Construir e iniciar o container:**
    ```bash
    docker-compose up -d
    ```

2.  **Acessar o terminal do container:**
    ```bash
    docker exec -it data_pipeline /bin/bash
    ```

3.  **Executar o pipeline de ETL:**
    ```bash
    python3 main.py
    ```

### 4.2. Para execução via Docker (recomendado)
 
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) ≥ 24.x
- [docker-compose](https://docs.docker.com/compose/) ≥ 2.x
- Mínimo **4 GB de RAM** disponíveis para o container
 
### 4.3. Para execução local (sem Docker)
 
- Python ≥ 3.12
- Java JDK 11 ou 17 (obrigatório para o Spark)
- `JAVA_HOME` configurado corretamente
 
```bash
# Verificar Java
java -version
 
# Verificar JAVA_HOME
echo $JAVA_HOME
```
 
### 4.4. Para execução no Databricks
 
- Conta ativa no [Databricks Community Edition](https://community.cloud.databricks.com/) ou workspace corporativo
- Cluster com Databricks Runtime ≥ 13.x (inclui Spark e Python)
 
---
 
## 5. Configuração do Ambiente
 
O arquivo `config/settings.yaml` centraliza todos os parâmetros da aplicação. **Não é necessário editar o código para mudar caminhos ou recursos Spark** — basta ajustar este arquivo.
 
```yaml
spark:
  app_name: "trab-final-rel-pedidos"
  driver_memory: "2G"
  driver_cores: "2"
  executor_instances: "2"
  executor_memory: "2G"
  executor_cores: "2"
  default_parallelism: "4"
 
paths:
  pagamentos: "data/input/dataset-json-pagamentos/data/pagamentos/"
  pedidos:    "data/input/datasets-csv-pedidos/data/pedidos/"
  output:     "data/output/pedidos_legit_recusados/"
 
file_options:
  pagamentos_json:
    compression: "gzip"
  pedidos_csv:
    compression: "gzip"
    header: true
    sep: ";"
```
 
> **Atenção:** os caminhos em `paths` são relativos à **raiz do projeto** (diretório pai de `src/`). O pipeline resolve os caminhos absolutos automaticamente via `pathlib.Path`.
 
---
## 6. Conclusão
O projeto cumpre os requisitos acadêmicos da disciplina, aplicando técnicas de processamento de dados escaláveis e garantindo a reprodutibilidade do código de maneira agnóstica à ferramenta escolhida pelo usuário.
