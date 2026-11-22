# Estágio 3 — Bronze: schema canônico por tipo, arquivo a arquivo

from pyspark.sql import functions as F

_COMMON = {
    "pickup_datetime": (["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"], "timestamp_ntz"),
    "dropoff_datetime": (["tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime", "dropOff_datetime"], "timestamp_ntz"),
    "pu_location_id": (["PULocationID", "PUlocationID"], "bigint"),
    "do_location_id": (["DOLocationID", "DOlocationID"], "bigint"),
}
_TAXI = {
    "vendor_id": (["VendorID"], "bigint"), "passenger_count": (["passenger_count"], "double"),
    "trip_distance": (["trip_distance"], "double"), "ratecode_id": (["RatecodeID"], "double"),
    "store_and_fwd_flag": (["store_and_fwd_flag"], "string"), "payment_type": (["payment_type"], "bigint"),
    "fare_amount": (["fare_amount"], "double"), "extra": (["extra"], "double"),
    "mta_tax": (["mta_tax"], "double"), "tip_amount": (["tip_amount"], "double"),
    "tolls_amount": (["tolls_amount"], "double"), "total_amount": (["total_amount"], "double"),
    "congestion_surcharge": (["congestion_surcharge"], "double"),
    "airport_fee": (["airport_fee", "Airport_fee"], "double"),
    "ehail_fee": (["ehail_fee"], "double"), "trip_type": (["trip_type"], "bigint"),
}
CANONICAL = {
    "yellow": {**_COMMON, **_TAXI}, "green": {**_COMMON, **_TAXI},
    "fhv": {**_COMMON, "dispatching_base_num": (["dispatching_base_num"], "string"),
            "affiliated_base_number": (["Affiliated_base_number"], "string"),
            "sr_flag": (["SR_Flag"], "string")},
    "fhvhv": {**_COMMON,
        "hvfhs_license_num": (["hvfhs_license_num"], "string"),
        "dispatching_base_num": (["dispatching_base_num"], "string"),
        "originating_base_num": (["originating_base_num"], "string"),
        "request_datetime": (["request_datetime"], "timestamp_ntz"),
        "on_scene_datetime": (["on_scene_datetime"], "timestamp_ntz"),
        "trip_miles": (["trip_miles"], "double"), "trip_time": (["trip_time"], "bigint"),
        "base_passenger_fare": (["base_passenger_fare"], "double"), "tolls": (["tolls"], "double"),
        "bcf": (["bcf"], "double"), "congestion_surcharge": (["congestion_surcharge"], "double"),
        "airport_fee": (["airport_fee"], "double"), "tips": (["tips"], "double"),
        "driver_pay": (["driver_pay"], "double"), "shared_request_flag": (["shared_request_flag"], "string"),
        "shared_match_flag": (["shared_match_flag"], "string"), "access_a_ride_flag": (["access_a_ride_flag"], "string"),
        "wav_request_flag": (["wav_request_flag"], "string"), "wav_match_flag": (["wav_match_flag"], "string")},
}
UNIFIED = {c: dtype for m in CANONICAL.values() for c, (_, dtype) in m.items()}

def to_bronze(df, taxi_type, file_name, year_month):
    available = {c.lower(): c for c in df.columns}
    mapping = CANONICAL[taxi_type]
    exprs = []
    for target, dtype in UNIFIED.items():
        sources = mapping.get(target, (None, None))[0] or []
        original = next((available[s.lower()] for s in sources if s.lower() in available), None)
        col = F.col(original).cast(dtype) if original else F.lit(None).cast(dtype)
        exprs.append(col.alias(target))
    return (df.select(*exprs).withColumn("taxi_type", F.lit(taxi_type))
            .withColumn("pickup_year_month", F.lit(year_month))
            .withColumn("_source_file", F.lit(file_name)).withColumn("_ingested_at", F.current_timestamp()))

def write_month_partition(df, table, year_month):
    writer = df.write.format("delta").mode("overwrite").option("replaceWhere", f"pickup_year_month = '{year_month}'")
    if not spark.catalog.tableExists(table):
        writer = writer.partitionBy("pickup_year_month")
    writer.saveAsTable(table)

for taxi_type in TAXI_TYPES:
    table = f"{CATALOG}.bronze.{taxi_type}_tripdata"
    for expected_type, file_name, year_month in EXPECTED:
        if expected_type != taxi_type:
            continue
        path = f"{VOLUME_PATH}/nyc_tlc/{taxi_type}/{file_name}"
        write_month_partition(to_bronze(spark.read.parquet(path), taxi_type, file_name, year_month), table, year_month)
    print(f"bronze.{taxi_type}_tripdata: {spark.table(table).count():,} linhas")
