# Projeto Data Warehouse Novadrive

<img width="1045" height="631" alt="1000000" src="https://github.com/user-attachments/assets/bf43e4de-0248-4b58-bf00-9eddc4662ae0" />


Este projeto é um pipeline completo de Engenharia de Dados para uma montadora fictícia chamada **Novadrive**. O objetivo é extrair dados de vendas em tempo real, transformá-los e disponibilizá-los para análise, utilizando um **Data Warehouse** na nuvem.

---

## 🚀 Objetivo do Projeto

Construir um **Data Warehouse funcional** para análise de vendas, incluindo:

- Integração de dados do PostgreSQL
- Transformações e modelagem com **dbt**
- Orquestração de tarefas com **Apache Airflow**
- Armazenamento em **Snowflake**
- Preparação para visualização de dados (Looker Studio ou BI)

---

## 🛠 Tecnologias utilizadas

- **Python** (scripts, Airflow)
- **Apache Airflow** (orquestração de DAGs)
- **dbt (data build tool)** (modelagem e transformação de dados)
- **Snowflake** (data warehouse)
- **PostgreSQL** (fonte de dados)
- **Docker / Docker Compose** (ambiente de execução)
- **GitHub** (versionamento e portfólio)

---

## 📁 Estrutura do projeto

```
├── airflow/
│   └── dags/ 
│       └── dag1.py
novadrive-dbt/
├── analyses/          # Queries analíticas finais
├── models/
│   ├── stage/         # Staging models (raw -> cleansed)
│   ├── dimensions/    # Tabelas de dimensão
│   ├── facts/         # Tabelas fato
│   ├── analysis/      # Queries finais de análise
│   └── source.yml     # Definição de fontes
├── macros/            # Funções reutilizáveis do dbt
├── seeds/             # Tabelas estáticas
├── snapshots/         # Histórico de dados
├── tests/             # Testes de qualidade de dados
└── dbt_project.yml    # Configuração do projeto dbt
```

O **Airflow** está configurado com DAGs para carregar dados do PostgreSQL para Snowflake diariamente.

---

## ⚙️ Como rodar o projeto

### 1. Airflow

1. Instale Docker e Docker Compose  
2. Inicialize o Airflow:

```bash
docker compose up airflow-init
docker compose up -d
```

3. Acesse o Airflow no navegador:

```
http://<EC2-PUBLIC-IP>:8080
```

As DAGs estão disponíveis em:

```
~/AWS/airflow/dags
```

---

### 2. dbt

1. Clone o projeto:

```bash
git clone https://github.com/ruansoarespy/Projeto-DBT-SnowFlake.git
cd Projeto-DBT-SnowFlake/novadrive-dbt
```

2. Configure sua conexão Snowflake no `profiles.yml`  

3. Execute as transformações:

```bash
dbt run
```

4. Execute os testes:

```bash
dbt test
```

5. Gere a documentação:

```bash
dbt docs generate
dbt docs serve
```

---

## 🔗 Contatos e portfólio

- LinkedIn: [https://www.linkedin.com/in/ruan-soares123/](https://www.linkedin.com/in/ruan-soares123/)  
- GitHub: [https://github.com/ruansoarespy/Projeto-DBT-SnowFlake](https://github.com/ruansoarespy/Projeto-DBT-SnowFlake)
