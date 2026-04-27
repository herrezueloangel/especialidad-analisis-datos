-- SPRINT 3
-- NIVEL 1 
-- EJERCICIO 1

-- silver
CREATE SCHEMA `sprint3-analytics-aherrezuelo.sprint3_silver`
OPTIONS(location="EU");

-- gold 

bq --location=EU mk --dataset sprint3-analytics-aherrezuelo:sprint3_gold

-- Ejercicio 2
-- Tabla transactions_raw

CREATE OR REPLACE EXTERNAL TABLE `sprint3-analytics-aherrezuelo.sprint3_bronze.transactions_raw`
OPTIONS (
  format = 'CSV',
  uris = ['gs://bootcamp-data-analytics-public/ERP/transactions.csv'],
  field_delimiter = ',',
  skip_leading_rows = 1
);

-- Tabla companies_raw

CREATE OR REPLACE EXTERNAL TABLE `sprint3-analytics-aherrezuelo.sprint3_bronze.companies_raw`
(
  id STRING,
  name STRING,
  phone STRING,
  email STRING,
  country STRING,
  website STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://bootcamp-data-analytics-public/ERP/companies.csv'],
  skip_leading_rows = 1
);

-- Tablas amererican y european users

CREATE OR REPLACE EXTERNAL TABLE `sprint3-analytics-aherrezuelo.sprint3_bronze.ameriecan_users_raw`
OPTIONS (
  format = 'CSV',
  uris = ['gs://bootcamp-data-analytics-public/CRM/american_users.csv'],
  skip_leading_rows = 1
);

-- Comprobacion de que los datos se han importado bien
SELECT *
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.american_users_raw`
LIMIT 5;

-- 

CREATE OR REPLACE EXTERNAL TABLE `sprint3-analytics-aherrezuelo.sprint3_bronze.european_users_raw`
OPTIONS (
  format = 'CSV',
  uris = ['gs://bootcamp-data-analytics-public/CRM/european_users.csv'],
  skip_leading_rows = 1
);

-- Comprobacion de que los datos se han importado bien
SELECT *
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.european_users_raw`
LIMIT 5;

-- Tabla Credit_cards_raw

CREATE OR REPLACE EXTERNAL TABLE `sprint3-analytics-aherrezuelo.sprint3_bronze.credit_cards_raw` (
  id STRING,
  user_id INT64,
  iban STRING,
  pan STRING,
  cvv STRING,
  pin STRING,
  track1 STRING,
  track2 STRING,
  expiring_date DATE
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://bootcamp-data-analytics-public/CRM/credit_cards.csv'],
  skip_leading_rows = 1
);

-- Ejercicio 3

-- Tabla products_raw

SELECT *
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.products_raw`
LIMIT 5;

-- Ejercicio 4

-- Tabla con IA

CREATE OR REPLACE TABLE `sprint3-analytics-aherrezuelo.sprint3_bronze.transactions_raw_native` AS
SELECT *
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.transactions_raw`;

-- Bytes procesados

SELECT id
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.transactions_raw`;

SELECT id
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.transactions_raw_native`;


-- Ejercicio 5

SELECT
  DATE(timestamp) AS date,
  ROUND(SUM(amount), 2) AS total_revenue
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.transactions_raw_native`
WHERE EXTRACT(YEAR FROM timestamp) = 2021
GROUP BY date
ORDER BY total_revenue DESC
LIMIT 5;

-- Ejercicio 6 - Consultas complexes

SELECT
  c.name,
  c.country,
  DATE(t.timestamp) AS date
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.transactions_raw_native` t
JOIN `sprint3-analytics-aherrezuelo.sprint3_bronze.companies_raw` c
ON t.business_id = c.id
WHERE t.amount BETWEEN 100 AND 200
  AND DATE(t.timestamp) IN ('2015-04-29', '2018-07-20', '2024-03-13');
  
  --  NIVEL 2
  -- Ejercicio 1 - Neteja de Productes (Data Quality)
  
  CREATE OR REPLACE TABLE `sprint3-analytics-aherrezuelo.sprint3_silver.products_clean` AS

SELECT 
  id AS product_id,
  product_name AS name,
  price,
  colour,
  weight,
  warehouse_id,  
  SAFE_CAST(REGEXP_REPLACE(warehouse_id, r'[^0-9]', '') AS INT64) AS warehouse_id_clean
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.products_raw`;

-- Ejercicio 2
-- Tabla transactions_clean
CREATE OR REPLACE TABLE `sprint3-analytics-aherrezuelo.sprint3_silver.transactions_clean` AS
SELECT 
  id AS transaction_id,
  card_id,
  business_id AS company_id,
  SAFE_CAST(timestamp AS TIMESTAMP) AS timestamp,
  IFNULL(SAFE_CAST(amount AS FLOAT64), 0) AS amount,
  declined,  
  ARRAY(
    SELECT CAST(x AS INT64)
    FROM UNNEST(SPLIT(product_ids)) AS x
  ) AS product_ids,
  user_id,
  SAFE_CAST(lat AS FLOAT64) AS lat,
  SAFE_CAST(longitude AS FLOAT64) AS longitude
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.transactions_raw_native`;

-- Ejercicio 3

CREATE OR REPLACE TABLE `sprint3-analytics-aherrezuelo.sprint3_silver.users_combined` AS

SELECT 
  id AS user_id,
  name,
  surname,
  phone,
  email,
  PARSE_DATE('%b %d, %Y', birth_date) AS birth_date,
  country,
  city,
  postal_code,
  address,
  'EUA' AS origin 

FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.american_users_raw`

UNION ALL 

SELECT 
  id AS user_id,
  name,
  surname,
  phone,
  email,
  PARSE_DATE('%b %d, %Y', birth_date) AS birth_date,
  country,
  city,
  postal_code,
  address,
  'Europa' AS origin 

FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.european_users_raw`;

-- Ejercicio 4
-- Tabla companies_clean y credit_cards_clean

CREATE OR REPLACE TABLE `sprint3-analytics-aherrezuelo.sprint3_silver.companies_clean` 
AS
SELECT 
  id AS company_id,
  *
EXCEPT(id)
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.companies_raw`;

CREATE OR REPLACE TABLE `sprint3-analytics-aherrezuelo.sprint3_silver.credit_cards_clean` 
AS
SELECT 
  id AS card_id,
  *
EXCEPT(id)
FROM `sprint3-analytics-aherrezuelo.sprint3_bronze.credit_cards_raw`;

-- NIVEL 3
-- Ejercicio 1 - Vista KPIs

CREATE OR REPLACE VIEW `sprint3-analytics-aherrezuelo.sprint3_gold.v_marketing_kpis` AS
SELECT 
  c.name,
  c.phone,
  c.country,
  ROUND(AVG(t.amount),2) AS avg_compra,
  CASE 
    WHEN AVG(t.amount) > 260 THEN 'Premium'
    ELSE 'Standard'
  END AS client_categoria
FROM `sprint3-analytics-aherrezuelo.sprint3_silver.companies_clean` c
JOIN `sprint3-analytics-aherrezuelo.sprint3_silver.transactions_clean` t
ON c.company_id = t.company_id
GROUP BY c.name, c.phone, c.country;

-- Ver view

SELECT *
FROM `sprint3-analytics-aherrezuelo.sprint3_gold.v_marketing_kpis`
ORDER BY 
  client_categoria ASC, 
  avg_compra DESC;
  
-- Ejercicio 2

-- Tabla ranking sales

CREATE OR REPLACE TABLE `sprint3-analytics-aherrezuelo.sprint3_gold.product_sales_ranking` AS
SELECT 
  p.product_id,
  p.name,
  p.price,
  p.colour,
  COUNT(t.product_id) AS total_vendido
FROM `sprint3-analytics-aherrezuelo.sprint3_silver.products_clean` p
LEFT JOIN (
  SELECT 
    product_id
  FROM `sprint3-analytics-aherrezuelo.sprint3_silver.transactions_clean`,
  UNNEST(product_ids) AS product_id
) t
ON p.product_id = t.product_id
GROUP BY p.product_id, p.name, p.price, p.colour;

-- Ejercicio 3 - Exportacion

SELECT *
FROM `sprint3-analytics-aherrezuelo.sprint3_gold.product_sales_ranking`
ORDER BY total_vendido DESC;




  


