-- EDA resumida
SELECT 'bronze' AS camada, taxi_type, COUNT(*) AS linhas FROM ifood.bronze.yellow_tripdata GROUP BY 1, 'yellow'
UNION ALL SELECT 'bronze', 'green', COUNT(*) FROM ifood.bronze.green_tripdata
UNION ALL SELECT 'bronze', 'fhv', COUNT(*) FROM ifood.bronze.fhv_tripdata
UNION ALL SELECT 'bronze', 'fhvhv', COUNT(*) FROM ifood.bronze.fhvhv_tripdata
UNION ALL SELECT 'silver', 'yellow', COUNT(*) FROM ifood.silver.yellow_trips
UNION ALL SELECT 'silver', 'green', COUNT(*) FROM ifood.silver.green_trips
UNION ALL SELECT 'silver', 'fhv', COUNT(*) FROM ifood.silver.fhv_trips
UNION ALL SELECT 'silver', 'fhvhv', COUNT(*) FROM ifood.silver.fhvhv_trips
UNION ALL SELECT 'quarentena', 'yellow', COUNT(*) FROM ifood.silver.yellow_trips_rejected
UNION ALL SELECT 'quarentena', 'green', COUNT(*) FROM ifood.silver.green_trips_rejected
UNION ALL SELECT 'quarentena', 'fhv', COUNT(*) FROM ifood.silver.fhv_trips_rejected
UNION ALL SELECT 'quarentena', 'fhvhv', COUNT(*) FROM ifood.silver.fhvhv_trips_rejected
ORDER BY camada, taxi_type;

SELECT COUNT(*) AS gold_rows,
       ROUND(100 * (118557800 - COUNT(*)) / 118557800, 2) AS pct_descartado
FROM ifood.gold.fct_taxi_trips;

SELECT rejection_reason AS motivo, COUNT(*) AS linhas
FROM (
    SELECT rejection_reason FROM ifood.silver.yellow_trips_rejected
    UNION ALL SELECT rejection_reason FROM ifood.silver.green_trips_rejected
    UNION ALL SELECT rejection_reason FROM ifood.silver.fhv_trips_rejected
    UNION ALL SELECT rejection_reason FROM ifood.silver.fhvhv_trips_rejected
)
GROUP BY rejection_reason
ORDER BY linhas DESC;
