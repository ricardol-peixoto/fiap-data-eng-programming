# Trabalho Final - Projeto Pyspark
## FIAP - Data Engineering Programming
### Turma: 8ABDR​ | Prof. Marcelo Barbosa  

---

## Separação de Tarefas

1. Arquiteto de Software: Foca na estrutura principal e na integração de todos os componentes.
* Criar o main.py como Aggregation Root.
* Implementar a Injeção de Dependências no fluxo principal.
* Garantir que todos os componentes estejam encapsulados em classes.
* Desenvolver o pacote de Orquestração do Pipeline.

Responsável: **Luiz Adelar Soldatelli Neto**

2. Engenheiro de Dados (Lógica de Negócio e Spark): Responsável pelo "coração" do processamento de dados.
* Criar a classe de Lógica de Negócio.
* Implementar os filtros: Pedidos de 2025, pagamentos recusados e não fraude.
* Aplicar a ordenação por UF, forma de pagamento e data.
* Configurar o Logging e o Tratamento de Erros (try/catch) na lógica.

Responsável: **Fátima Beatriz Pinheiro Rodrigues**

3. Especialista em I/O e Infraestrutura: Cuida da entrada, saída e ambiente Spark.
* Definir os Schemas explícitos para os Datasets (sem inferência).
* Criar as classes de leitura (JSON/CSV) e escrita (Parquet).
* Desenvolver a classe de gerenciamento da sessão Spark.
* Configurar o pacote de Configurações Centralizadas.

Responsável: **Jean Felipe dos Santos Ertzogue**

4. Desenvolvedor de Qualidade (QA) e DevOps: Garante que o código funcione e seja entregue corretamente.
* Criar os Testes Unitários utilizando o pacote pytest.
* Preparar o Empacotamento: pyproject.toml, requirements.txt, MANIFEST.in.
* Garantir que o projeto seja agnóstico à plataforma.
* Configurar o repositório público no Github.

Responsável: **Ricardo de Lima Peixoto**

5. Documentador e Integrador de Evidências: Responsável pelo relatório final e documentação do projeto.
* Elaborar o README.md detalhado com instruções de execução.
* Coletar os prints dos códigos-fontes (máximo 20 linhas por evidência).
* Montar o documento final em PDF com capa (Nome da disciplina, nomes e RMs).
* Validar se todos os 12 critérios específicos foram atendidos antes da entrega.

Responsável: **Mauricio Acedo de Aquino**