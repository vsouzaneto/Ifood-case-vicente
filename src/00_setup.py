# Estágio 0 do pipeline — Case iFood Data Architecture (Databricks + PySpark)

 dbutils.widgets.text("catalog", "ifood", "Catálogo Unity Catalog de destino")
CATALOG = dbutils.widgets.get("catalog")

for schema in ["landing", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`landing`.`raw_files`")

VOLUME_PATH = f"/Volumes/{CATALOG}/landing/raw_files"
UPLOAD_DIR = f"{VOLUME_PATH}/Ifood_taxi"

import re
_found = [
    m.groups() for f in dbutils.fs.ls(UPLOAD_DIR) if f.name.endswith(".parquet")
    for m in [re.match(r"([a-z]+)_tripdata_(\d{4})-(\d{2})\.parquet$", f.name)] if m
]
TAXI_TYPES = sorted({t for t, _, _ in _found})
YEAR = sorted({int(y) for _, y, _ in _found})[0]
MONTHS = sorted({m for _, _, m in _found})
EXPECTED = sorted({(t, f"{t}_tripdata_{y}-{m}.parquet", f"{y}-{m}") for t, y, m in _found})

print(f"Setup concluído. Uploads em: {UPLOAD_DIR} | Carga: {len(EXPECTED)} arquivos — {YEAR}, meses {MONTHS}, tipos {TAXI_TYPES}")
