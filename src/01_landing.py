# Estágio 1 — Landing: organiza os uploads, valida integridade e grava o controle de ingestão
# (o original permanece no Volume para auditoria e reprocesso)

from pyspark.sql import functions as F

root = {f.name: f for f in dbutils.fs.ls(UPLOAD_DIR) if f.name.endswith(".parquet")}
for taxi_type, file_name, year_month in EXPECTED:
    if file_name in root:
        dbutils.fs.cp(f"{UPLOAD_DIR}/{file_name}", f"{VOLUME_PATH}/nyc_tlc/{taxi_type}/{file_name}")

rows = []
for taxi_type, file_name, year_month in EXPECTED:
    path = f"{VOLUME_PATH}/nyc_tlc/{taxi_type}/{file_name}"
    status, size_bytes, n_rows, error = "missing", None, None, None
    try:
        size_bytes = next(f.size for f in dbutils.fs.ls(f"{VOLUME_PATH}/nyc_tlc/{taxi_type}") if f.name == file_name)
        n_rows = spark.read.parquet(path).count()
        status = "ok"
    except Exception as e:
        status = "invalid" if size_bytes is not None else "missing"
        error = str(e)[:200]
    rows.append((taxi_type, file_name, year_month, status, size_bytes, n_rows, error))

controle_ingestao = spark.createDataFrame(
    rows,
    "taxi_type string, file_name string, year_month string, status string, size_bytes long, n_rows long, error string",
).withColumn("ingested_at", F.current_timestamp())
controle_ingestao.write.mode("overwrite").saveAsTable(f"{CATALOG}.landing.controle_ingestao")

display(spark.table(f"{CATALOG}.landing.controle_ingestao").orderBy("file_name"))

problems = [r[1] for r in rows if r[3] != "ok"]
if problems:
    raise FileNotFoundError(f"Arquivos ausentes ou inválidos no Volume {VOLUME_PATH}: {problems}")
