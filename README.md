# Case iFood — Pipeline NYC Taxi (Databricks + PySpark)

Pipeline de dados com arquitetura **medalhão** sobre dados públicos da NYC TLC
(yellow, green, fhv e fhvhv, jan–mai/2023), construído em **PySpark** no Databricks.

## Estrutura do repositório

```
ifood-case/
├── src/
│   ├── 00_setup.py
│   ├── 01_landing.py
│   ├── 02_dicionarios.py
│   ├── 03_bronze.py
│   ├── 04_silver.py
│   └── 05_gold.py
├── analysis/
│   ├── 01_pergunta1_total_amount_por_mes.sql
│   ├── 02_pergunta2_passageiros_por_hora.sql
│   └── 03_eda_resumo.sql
├── README.md
└── requirements.txt
```

## Execução no Databricks

1. Abra o workspace Databricks e carregue os arquivos exportados em um notebook.
2. Configure o widget `catalog` (padrão: `ifood`).
3. Execute `00_setup.py` até `05_gold.py`, nesta ordem.
4. Envie os 20 parquet para `/Volumes/ifood/landing/raw_files/Ifood_taxi/`.

## Consultas

- `SELECT * FROM ifood.gold.fct_taxi_trips_yellow_green`
- `SELECT * FROM ifood.gold.vw_taxi_trips`
- `SELECT * FROM ifood.landing.dicionario_dados`
