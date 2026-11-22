# Estágio 4 — Silver por tipo

from pyspark.sql import functions as F

COMMON_RULES = [
    ("dropoff anterior ou igual ao pickup", F.col("dropoff_datetime") > F.col("pickup_datetime")),
    ("data da corrida fora do mes do arquivo", F.date_format("pickup_datetime", "yyyy-MM") == F.col("pickup_year_month")),
]
METER_RULES = [
    ("passenger_count fora do intervalo 1-6", F.col("passenger_count").between(1, 6)),
    ("total_amount nao positivo", F.col("total_amount") > 0),
    ("trip_distance nao positiva", F.col("trip_distance") > 0),
]
NATURE_RULES = {"yellow": METER_RULES, "green": METER_RULES, "fhv": [],
                "fhvhv": [("trip_miles nao positiva", F.col("trip_miles") > 0),
                           ("trip_time nao positivo", F.col("trip_time") > 0)]}
DEDUP_KEYS = {
    "yellow": ["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id", "total_amount"],
    "green": ["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id", "total_amount"],
    "fhv": ["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id", "dispatching_base_num"],
    "fhvhv": ["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id", "dispatching_base_num", "driver_pay"],
}

for taxi_type in TAXI_TYPES:
    trips = (spark.table(f"{CATALOG}.bronze.{taxi_type}_tripdata")
             .withColumn("pickup_hour", F.hour("pickup_datetime"))
             .withColumn("trip_duration_min", F.round((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60, 2))
             .withColumn("is_valid_passenger_count", F.col("passenger_count").between(1, 6)))
    rules = COMMON_RULES + NATURE_RULES[taxi_type]
    reasons = [F.when(~condition, F.lit(name)) for name, condition in rules]
    trips = trips.withColumn("rejection_reason", F.concat_ws("; ", *reasons))
    valid = trips.where(F.col("rejection_reason") == "").drop("rejection_reason")
    rejected = trips.where(F.col("rejection_reason") != "").withColumn("rejected_at", F.current_timestamp())
    valid = valid.dropDuplicates(DEDUP_KEYS[taxi_type])

    valid.write.mode("overwrite").partitionBy("pickup_year_month").saveAsTable(f"{CATALOG}.silver.{taxi_type}_trips")
    rejected.write.mode("overwrite").partitionBy("pickup_year_month").saveAsTable(f"{CATALOG}.silver.{taxi_type}_trips_rejected")
    print(f"silver.{taxi_type}_trips: {spark.table(f'{CATALOG}.silver.{taxi_type}_trips').count():,} válidas")
