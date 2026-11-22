-- Pergunta 2 do case: média de passenger_count por hora do dia, maio/2023.
SELECT pickup_hour AS hora,
       COUNT(*) AS corridas,
       ROUND(AVG(passenger_count), 3) AS media_passageiros
FROM ifood.gold.fct_taxi_trips_yellow_green
WHERE pickup_year_month = '2023-05' AND is_valid_passenger_count
GROUP BY pickup_hour
ORDER BY hora;

SELECT taxi_type,
       pickup_hour AS hora,
       COUNT(*) AS corridas,
       ROUND(AVG(passenger_count), 3) AS media_passageiros
FROM ifood.gold.fct_taxi_trips_yellow_green
WHERE pickup_year_month = '2023-05' AND is_valid_passenger_count
GROUP BY taxi_type, pickup_hour
ORDER BY taxi_type, hora;
