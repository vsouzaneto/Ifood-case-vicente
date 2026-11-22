# Estágio 2 — Dicionários TLC

DICT_DIR = f"{VOLUME_PATH}/dictionaries"
EXTRACT_SCHEMA = ('{"fields": {"type": "array", "items": {"type": "object", '
                  '"properties": {"name": {"type": "string"}, "description": {"type": "string"}}}}}')

spark.sql(f"""
CREATE OR REPLACE TABLE `{CATALOG}`.landing.dicionario_documentos AS
SELECT _metadata.file_name AS file_name,
  CASE WHEN _metadata.file_name LIKE '%yellow%' THEN 'yellow'
       WHEN _metadata.file_name LIKE '%green%' THEN 'green'
       WHEN _metadata.file_name LIKE '%hvfhs%' THEN 'fhvhv'
       WHEN _metadata.file_name LIKE '%_fhv_%' THEN 'fhv' END AS taxi_type,
  content, ai_parse_document(content, MAP('version', '2.0')) AS parsed_content
FROM READ_FILES('{DICT_DIR}/', format => 'binaryFile')
""")

spark.sql(f"""
CREATE OR REPLACE TABLE `{CATALOG}`.landing.dicionario_dados AS
WITH extracted AS (
  SELECT taxi_type, ai_extract(parsed_content, '{EXTRACT_SCHEMA}',
    MAP('version', '2.1', 'instructions',
      'Extract every exact field name and complete description from this NYC TLC dictionary.')) AS ext
  FROM `{CATALOG}`.landing.dicionario_documentos WHERE taxi_type IS NOT NULL
)
SELECT taxi_type, f:name:value::STRING AS field_name,
       f:description:value::STRING AS description
FROM extracted LATERAL VIEW OUTER explode(try_cast(ext:response:fields AS ARRAY<VARIANT>)) t AS f
""")

display(spark.table(f"{CATALOG}.landing.dicionario_dados").groupBy("taxi_type").count().withColumnRenamed("count", "n_fields"))
