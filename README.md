# Trabalho Final - Data Engineering Programming
# DESAFIO RELATÓRIO DE PAGAMENTOS 
## FIAP - MBA em Data Engineering


**Prof.:** Marcelo Barbosa  
**Alunos:** Beatriz Pinheiro Rodrigues, Jean Ertzogue, Luiz Soldatelli, Ricardo Peixoto e Mauricio Aquino
**Turma:** 8ABDR

---

## 1. Descrição do Projeto
Este projeto consiste no desenvolvimento de um pipeline de ETL (Extract, Transform, Load) utilizando **PySpark**. O objetivo principal é demonstrar a aplicação de conceitos de programação voltada para engenharia de dados, incluindo a manipulação de grandes volumes de dados, estruturação de código modular e aplicação de testes automatizados.

O pipeline realiza a leitura de fontes de dados brutas, aplica transformações de limpeza e agregação, e persiste os resultados de forma estruturada.

O objetivo é elaborar um relatório de pedidos de venda que tiveram pagamentos recusados e que na avaliação de fraude foram classificados como legítimos.

---

## 2. Estrutura do Repositório
A organização do projeto segue as boas práticas de desenvolvimento:

* `src/`: Contém os scripts principais e a lógica de processamento Spark.
* `config/`: Arquivos de configuração do ambiente e parâmetros do job.
* `data/input/`: Diretório destinado aos arquivos de origem (raw data).
* `tests/`: Scripts de testes unitários e validações.
* `Dockerfile` & `docker-compose.yml`: Configurações para orquestração do ambiente via containers.
* `requirements.txt`: Dependências das bibliotecas Python utilizadas.

### 2.1. Detalhamento dos pacotes na pasta `src/`:

```
.
└── src/
    ├── __init__.py
    ├── config/
    │   ├── __init__.py
    │   └── settings.py         # <-- Configurações obtidas do arquivo YAML necessárias
    ├── session/
    │   ├── __init__.py
    │   └── spark_session.py    # <-- Classe para gerenciar a sessão Spark
    ├── io_utils/
    │   ├── __init__.py
    │   └── data_handler.py     # <-- Classe para ler e escrever dados (I/O)
    ├── processing/
    │   ├── __init__.py
    │   └── transformations.py  # <-- Classe para a lógica de negócio
    └── main.py                 # <-- Orquestrador principal da aplicação
```

---

## 3. Tecnologias Utilizadas
* **Python 3.x**: Linguagem principal.
* **Apache Spark (PySpark)**: Engine de processamento distribuído.

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

## 4.2. Instruções de Execução `Databricks`

Para rodar o projeto no Databricks, siga os passos abaixo:

1.  **Selecione a opção ` Catalog `**
2.  **Clique no Botão ` CREATE `, e depois selecione a opção ` Create a Volume `**
3.  **Na janela CREATE A NEW VOLUME, preencha o campo VOLUME NAME* com:**
    ```bash
    fiap-data-eng-programming
    ```
4.  **Nos campos da opção - Choose catalog and schema - selecione a opção ` workspace ` como Catalog**
    **e em seguida, ao clicar na opção Select a Schema, selecione a opção ` + CREATE A NEW SCHEMA `**
    **irá surgir uma nova janela e defina o valor abaixo no campo ` SCHEMA NAME `:**
    ```bash
    data-programming-trab-final
    ```
5.  **Localize o arquivo `main.py ` e execute o pipeline**

---

## 6. Conclusão
O projeto cumpre os requisitos acadêmicos da disciplina, aplicando técnicas de processamento de dados escaláveis e garantindo a reprodutibilidade do código de maneira agnóstica à ferramenta escolhida pelo usuário.
