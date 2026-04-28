-- SPRINT 4
-- NIVEL 1

-- EJERCICIO 1 - ventas Alemania del 12/3

SELECT 
  t.*,
  c.name,
  c.country
FROM `sprint3-analytics-aherrezuelo.sprint3_silver.transactions_clean` t
JOIN `sprint3-analytics-aherrezuelo.sprint3_silver.companies_clean` c
ON t.company_id = c.company_id
WHERE c.country = 'Germany'
AND t.timestamp >= '2022-03-12' 
AND t.timestamp < '2022-03-13';

-- Ejercicio 2 - transacciones recientes

CREATE OR REPLACE TABLE `sprint3_silver.transactions_recent` AS
SELECT 
  * EXCEPT(timestamp),
  TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(),
    INTERVAL CAST(RAND() * 50 AS INT64) DAY
  ) AS timestamp
FROM `sprint3_silver.transactions_clean`;

-- Transacionts optimized

CREATE OR REPLACE TABLE `sprint3_gold.fact_transactions_optimized`
PARTITION BY DATE(timestamp)
CLUSTER BY company_id AS
SELECT *
FROM `sprint3_silver.transactions_recent`;

-- Ejercicio 3
-- Comparacion tabla optimizada o no

SELECT *
FROM `sprint3_silver.transactions_recent`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);

SELECT *
FROM `sprint3_gold.fact_transactions_optimized`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);

-- Ejercicio 4 - Daily Sales

CREATE MATERIALIZED VIEW `sprint3_gold.mv_daily_sales` AS
SELECT 
  DATE(timestamp) AS date,
  SUM(amount) AS total_sales
FROM `sprint3_gold.fact_transactions_optimized`
WHERE declined = 0
GROUP BY date;

select *
FROM `sprint3_gold.mv_daily_sales`;


-- NIVEL 2
-- Ejercicio 1 - VIP

WITH VIP_Stats AS (
  SELECT 
    user_id,
    SUM(amount) AS total_gastado,
    COUNT(*) AS num_compras,
    ROUND(AVG(amount), 2) AS tiquet_medio,
    MAX(amount) AS max_compra
  FROM `sprint3_gold.fact_transactions_optimized`
  GROUP BY user_id
  HAVING total_gastado > 500
)
SELECT 
  v.user_id,
  u.name,
  u.surname,
  u.email,
  v.num_compras,
  v.tiquet_medio,
  v.max_compra,
  v.total_gastado
FROM VIP_Stats v
JOIN `sprint3_silver.users_combined` u
ON v.user_id = u.user_id
ORDER BY v.total_gastado DESC;

-- Ejercicio 2 - Comparación con el dia anterior

SELECT 
  date,
  total_sales AS ventas_hoy,

  LAG(total_sales) OVER (ORDER BY date) AS ventas_ayer,

  ROUND(
    (total_sales - LAG(total_sales) OVER (ORDER BY date)) 
    / LAG(total_sales) OVER (ORDER BY date) * 100,
    2
  ) AS dif_percentual

FROM `sprint3_gold.mv_daily_sales`
ORDER BY date;

-- Ejercicio 3 - Ventas acumuladas

SELECT 
  date,

  ROUND(total_sales, 2) AS ventas_dia,

  ROUND(
    SUM(total_sales) OVER (
      PARTITION BY EXTRACT(YEAR FROM date)
      ORDER BY date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    2
  ) AS ventas_acumuladas_ytd

FROM `sprint3_gold.mv_daily_sales`

ORDER BY date;

-- Ejercicio 4 - Fidelización

WITH primeras_compras AS (
  SELECT 
    user_id,
    timestamp,
    amount,

    ROW_NUMBER() OVER (
      PARTITION BY user_id 
      ORDER BY timestamp
    ) AS num_compra

  FROM `sprint3_gold.fact_transactions_optimized`

  QUALIFY num_compra <= 3
),

media_por_usuario AS (
  SELECT 
    user_id,
    ROUND(AVG(amount), 2) AS media_3_primeras
  FROM primeras_compras
  GROUP BY user_id
)

SELECT 
  p.user_id,
  p.timestamp,
  p.amount,
  m.media_3_primeras

FROM primeras_compras p
JOIN media_por_usuario m
ON p.user_id = m.user_id

WHERE p.num_compra = 3;

-- NIVEL 3
-- Ejercicio 1 - Aplanamiento de datos

CREATE OR REPLACE TABLE `sprint3_gold.dim_transactions_flat` AS
SELECT 
  t.transaction_id,
  t.timestamp,
  t.amount AS total_ticket,

  product_id,

  p.name,
  p.price

FROM `sprint3_silver.transactions_clean` t

CROSS JOIN UNNEST(t.product_ids) AS product_id

JOIN `sprint3_silver.products_clean` p 
ON product_id = p.product_id

WHERE t.declined = 0

-- Ejercicio 2

SELECT 
  product_id,
  product_name,
  COUNT(*) AS total_vendidos

FROM `sprint3_gold.dim_transactions_flat`
GROUP BY product_id, product_name
ORDER BY total_vendidos DESC
LIMIT 5;

-- Ejercicio 3

-- IVA

CREATE OR REPLACE FUNCTION `sprint3_gold.calculate_tax`(amount FLOAT64)
RETURNS FLOAT64
AS (
  amount * 1.21
);

-- Integració i Orquestració:
CREATE OR REPLACE TABLE `sprint3_gold.dim_transactions_flat` AS

SELECT
  t.transaction_id,
  t.timestamp,
  t.amount AS total_ticket,

  product_id,

  p.name AS product_name,
  p.price AS product_price,

  `sprint3_gold.calculate_tax`(p.price) AS product_price_tax_inc

FROM `sprint3_silver.transactions_clean` t

CROSS JOIN UNNEST(t.product_ids) AS product_id

JOIN `sprint3_silver.products_clean` p
ON product_id = p.product_id;

