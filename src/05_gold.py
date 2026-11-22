# Estágio 5 — Gold: panorama geral e camada de consumo.

fct = None
for _t in TAXI_TYPES:
    _df = spark.table(f"{CATALOG}.silver.{_t}_trips")
    fct = _df if fct is None else fct.unionByName(_df)

fct.createOrReplaceTempView("_fct_union")
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.fct_taxi_trips
    CLUSTER BY (pickup_year_month, taxi_type)
    AS SELECT * FROM _fct_union
""")

spark.sql(f"CREATE VIEW IF NOT EXISTS {CATALOG}.gold.vw_taxi_trips AS SELECT * FROM {CATALOG}.gold.fct_taxi_trips")
spark.sql(f"ALTER VIEW {CATALOG}.gold.vw_taxi_trips AS SELECT * FROM {CATALOG}.gold.fct_taxi_trips")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.fct_taxi_trips_yellow_green
    CLUSTER BY (pickup_year_month, taxi_type)
    AS SELECT * FROM {CATALOG}.gold.vw_taxi_trips
    WHERE taxi_type IN ('yellow', 'green')
""")

LAST_MONTH = f"{YEAR}-{MONTHS[-1]}"

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.agg_avg_total_amount_by_month AS
    SELECT pickup_year_month,
           COUNT(*) AS corridas,
           ROUND(AVG(total_amount), 2) AS media_por_corrida,
           ROUND(SUM(total_amount), 2) AS receita_mes
    FROM {CATALOG}.gold.fct_taxi_trips_yellow_green
    WHERE taxi_type = 'yellow'
    GROUP BY pickup_year_month
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.agg_avg_passengers_by_hour AS
    SELECT pickup_hour,
           COUNT(*) AS corridas,
           ROUND(AVG(passenger_count), 3) AS media_passageiros
    FROM {CATALOG}.gold.fct_taxi_trips_yellow_green
    WHERE pickup_year_month = '{LAST_MONTH}' AND is_valid_passenger_count
    GROUP BY pickup_hour
""")

REQUIRED = ["pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "total_amount"]
for _tbl in ("fct_taxi_trips", "fct_taxi_trips_yellow_green"):
    _cols = set(spark.table(f"{CATALOG}.gold.{_tbl}").columns)
    missing = [c for c in REQUIRED if c not in _cols]
    assert not missing, f"Colunas obrigatórias ausentes em gold.{_tbl}: {missing}"

display(spark.sql(f"SHOW TABLES IN {CATALOG}.gold"))
