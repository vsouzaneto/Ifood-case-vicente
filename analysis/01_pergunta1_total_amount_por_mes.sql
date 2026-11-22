-- Pergunta 1 do case: média de total_amount por mês (yellow), jan–mai/2023.
SELECT pickup_year_month AS mes,
       COUNT(*) AS corridas,
       ROUND(AVG(total_amount), 2) AS media_por_corrida,
       ROUND(SUM(total_amount), 2) AS receita_mes
FROM ifood.gold.fct_taxi_trips_yellow_green
WHERE taxi_type = 'yellow'
GROUP BY pickup_year_month
ORDER BY mes;

SELECT ROUND(AVG(receita), 2) AS receita_media_mensal
FROM (
    SELECT SUM(total_amount) AS receita
    FROM ifood.gold.fct_taxi_trips_yellow_green
    WHERE taxi_type = 'yellow'
    GROUP BY pickup_year_month
);
