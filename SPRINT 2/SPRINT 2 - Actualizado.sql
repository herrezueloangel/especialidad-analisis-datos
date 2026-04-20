-- SPRINT 1
-- NIVEL 1 
-- EXERCICI 2

USE transactions;
-- Llistat dels països que estan generant vendes.

SELECT DISTINCT c.country AS Paises
FROM company c
JOIN `transaction` t ON t.company_id = c.id;

-- Des de quants països es generen les vendes.

SELECT COUNT(DISTINCT c.country) AS Paises
FROM company c
JOIN `transaction` t ON t.company_id = c.id;

-- Identifica la companyia amb la mitjana més gran de vendes.

SELECT c.id, c.company_name, ROUND(AVG(t.amount),2) AS Media_Ventas
FROM company c
JOIN `transaction` t ON t.company_id = c.id
WHERE t.declined = 0
GROUP BY c.id
ORDER BY Media_Ventas DESC
LIMIT 1;

-- EXERCICI 3 - SUBCONSULTAS

-- Mostra totes les transaccions realitzades per empreses d'Alemanya.

SELECT t.*
FROM `transaction` t
WHERE EXISTS (
    SELECT 1
    FROM company c
    WHERE c.id = t.company_id
    AND country = 'Germany'
);

-- Llista les empreses que han realitzat transaccions per un amount superior a la mitjana de totes les transaccions.

SELECT c.id, c.company_name
FROM company c
WHERE EXISTS (
	SELECT 1
	FROM `transaction` t
	WHERE t.company_id = c.id 
    AND t.amount > (
		SELECT AVG(amount)
		FROM `transaction`)
);

-- Eliminaran del sistema les empreses que no tenen transaccions registrades, entrega el llistat d'aquestes empreses.

SELECT c.company_name
FROM company c
WHERE NOT EXISTS (
    SELECT 1 -- <- Se usa como buena practica, no tiene siginifaco en si. Se usa para verificar si devuelve o no alguna fila.
    FROM `transaction` t
    WHERE t.company_id = c.id
);

-- EXERCICI 4

CREATE TABLE credit_card (
    id VARCHAR(60) PRIMARY KEY,
    iban VARCHAR(60)NOT NULL,
    pan VARCHAR(60) NOT NULL,
    pin VARCHAR(60) NOT NULL,
    cvv VARCHAR(60) NOT NULL,
    expiring_date VARCHAR(60) NOT NULL
);

UPDATE credit_card
SET expiring_date = STR_TO_DATE(expiring_date, '%m/%d/%y');

ALTER TABLE credit_card
MODIFY expiring_date DATE;

ALTER TABLE `transaction`
ADD FOREIGN KEY (credit_card_id) REFERENCES credit_card(id);

-- EXERCICI 5
SELECT *
FROM credit_card
WHERE id = 'CcU-2938';

UPDATE credit_card
SET iban = 'TR323456312213576817699999'
WHERE id = 'CcU-2938';

SELECT * 
FROM credit_card
WHERE id = 'CcU-2938';

-- EXERCICI 6

-- Para agregar una nueva fila en transaction antes debo crear el id en company porque es FK de transaction. También he modificado la tabla
-- credit card y he puesto algunas series para que aceptaran nulos ya que no tengo el resto de los datos.

INSERT INTO company (id)
VALUES (
    'b-9999');

ALTER TABLE credit_card 
MODIFY iban VARCHAR(34) NULL,
MODIFY pan VARCHAR(20) NULL,
MODIFY pin VARCHAR(10) NULL,
MODIFY cvv VARCHAR(4) NULL,
MODIFY expiring_date DATE NULL;

INSERT INTO credit_card(id)
VALUES ('CcU-9999');

INSERT INTO `transaction`(id, credit_card_id, company_id, user_id, lat, longitude, timestamp, amount, declined)
VALUES (
	'108B1D1D-5B23-A76C-55EF-C568E49A99DD',
    'CcU-9999',
    'b-9999',
    '9999',
    829.999,
    -117.999 ,
    NULL,
    111.11,
    0
);

-- EXERCICI 7

ALTER TABLE credit_card DROP column pan;

SELECT *
FROM credit_card;

-- EXERCICI 8

CREATE DATABASE transactions2;
USE transactions2;


CREATE TABLE transactions (
    id VARCHAR(255) PRIMARY KEY,
    card_id VARCHAR(15),
    business_id VARCHAR(20),
    timestamp TIMESTAMP,
    amount DECIMAL(10,2),
    declined TINYINT(1),
    product_ids VARCHAR(255),
    user_id VARCHAR(15),
    lat FLOAT,
    longitude FLOAT
);


CREATE TABLE companies (
    company_id VARCHAR(15) PRIMARY KEY,
    company_name VARCHAR(255),
    phone VARCHAR(255),
    email VARCHAR(255),
	country VARCHAR(255),
	website VARCHAR(255)
);


CREATE TABLE credit_cards (
    id VARCHAR(15) PRIMARY KEY,
    user_id VARCHAR(15),
    iban VARCHAR(34),
    pan VARCHAR(20),
    pin VARCHAR(4),
    cvv VARCHAR(4),
    track1 VARCHAR(255),
    track2 VARCHAR(255),
    expiring_date DATE
);

CREATE TABLE users (
	id VARCHAR(15) PRIMARY KEY,
    name VARCHAR(25),
    surname VARCHAR(25),
    phone VARCHAR(20),
    email VARCHAR(255),
    birth_date DATE,
	country VARCHAR(100),
    city VARCHAR(25),
    postal_code VARCHAR(10),
    address VARCHAR(100),
    Region VARCHAR(100)
);

LOAD DATA LOCAL INFILE "C:/Users/herre/OneDrive/Desktop/IT ACADEMY/ESPECIALIDAD/EJERCICICIOS/SPRINT 1/N1.Ex.8__ transactions.csv"
INTO TABLE transactions
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'C:/Users/herre/OneDrive/Desktop/IT ACADEMY/ESPECIALIDAD/EJERCICICIOS/SPRINT 1/N1.Ex.8__ companies.csv'
INTO TABLE companies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'C:/Users/herre/OneDrive/Desktop/IT ACADEMY/ESPECIALIDAD/EJERCICICIOS/SPRINT 1/N1.Ex.8__ credit_cards.csv'
INTO TABLE credit_cards
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, user_id, iban, pan, pin, cvv, track1, track2, @exp_date)
SET expiring_date = STR_TO_DATE(@exp_date, '%m/%d/%y');

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/N1-Ex.8__ american_users.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
SET region = 'America';

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/N1.Ex.8__ european_users.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
SET region = 'Europa';

ALTER TABLE transactions
ADD CONSTRAINT fk_business
    FOREIGN KEY (business_id) REFERENCES companies(company_id),
ADD CONSTRAINT fk_card
    FOREIGN KEY (card_id) REFERENCES credit_cards(id),
ADD CONSTRAINT fk_user
    FOREIGN KEY (user_id) REFERENCES users(id);

-- EXERCICI 9

SELECT *
FROM users u
WHERE EXISTS (
    SELECT 1
    FROM transactions t
    WHERE t.user_id = u.id
    GROUP BY t.user_id
    HAVING COUNT(*) > 80
);

-- EXERCICI 10

SELECT c.company_id, c.company_name, cc.iban, ROUND(AVG(t.amount),2) AS media_importe
FROM companies c
JOIN transactions t ON t.business_id = c.company_id
JOIN credit_cards cc ON cc.id = t.card_id
WHERE c.company_name = 'Donec Ltd'
GROUP BY c.company_id, c.company_name, cc.iban
ORDER BY media_importe DESC;

-- NIVEL 2
-- EXERCICI 1

SELECT DATE(t.timestamp), SUM(t.amount) AS total_importe
FROM transactions t
GROUP BY DATE(t.timestamp)
ORDER BY total_importe DESC
LIMIT 5;

-- EXERCICI 2

SELECT c.company_name, c.phone,c.country, DATE(t.timestamp) AS Fecha, t.amount
FROM companies c
JOIN transactions t ON t.business_id = c.company_id
WHERE t.amount BETWEEN 350 AND 400 
AND DATE(t.timestamp) IN ('2015-04-29','2018-07-20', '2024-03-13')
ORDER BY t.amount DESC;

-- EXERCICI 3

SELECT 
	c.company_id,
    c.company_name,
    COUNT(t.id) AS total_op,
    CASE 
        WHEN COUNT(t.id) > 400 THEN 'Alta'
        ELSE 'Baja'
    END AS clasificacion
FROM companies c
JOIN transactions t ON c.company_id = t.business_id
GROUP BY c.company_id
ORDER BY total_op DESC;

-- EXERCICI 4

DELETE FROM transactions
WHERE id ='000447FE-B650-4DCF-85DE-C7ED0EE1CAAD';

SELECT *
FROM transactions
WHERE id = '000447FE-B650-4DCF-85DE-C7ED0EE1CAAD';

-- EXERCICI 5

CREATE OR REPLACE VIEW VistaMarketing AS
SELECT c.company_name, c.phone, c.country, ROUND(AVG(t.amount),2) AS media_compra
FROM companies c
JOIN transactions t ON t.business_id = c.company_id
GROUP BY c.company_name, c.phone, c.country;

SELECT * FROM VistaMarketing
ORDER BY media_compra DESC;

-- NIVEL 3
-- EXERCICI 1

CREATE TABLE estado_tarjeta AS
SELECT card_id,
	CASE 
        WHEN SUM(declined) <3 THEN 'Active'
        ELSE 'Inactive'
	END AS estado
FROM (SELECT  card_id, timestamp, declined,
			ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) as orden_operaciones
			FROM transactions) t
WHERE orden_operaciones <= 3
GROUP BY card_id;


SELECT estado, COUNT(*)
FROM estado_tarjeta
GROUP BY estado;

-- EXERCICI 2

CREATE TABLE products (
	id VARCHAR(15) PRIMARY KEY,
    product_name VARCHAR(255),
    price DECIMAL(10,2),
    colour VARCHAR(20),
    weight FLOAT,
	warehouse_id VARCHAR(10)
);

LOAD DATA LOCAL INFILE 'C:/Users/herre/OneDrive/Desktop/IT ACADEMY/ESPECIALIDAD/EJERCICICIOS/SPRINT 1/N3.Ex.2__ products.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@id, @product_name, @price, @colour, @weight, @warehouse_id)
SET
    id = @id,
    product_name = @product_name,
    price =CASE 
		WHEN @price = '' THEN NULL
        ELSE CAST(REPLACE(@price, '$', '') AS DECIMAL(10,2))
	END,
    colour = @colour,
    weight = @weight,
    warehouse_id = @warehouse_id;

CREATE TABLE op_products AS
SELECT 
    t.id AS transaction_id,
    pi.product_id
FROM transactions t,
JSON_TABLE(
    CONCAT('[',t.product_ids, ']'),
    '$[*]'
    COLUMNS (product_id VARCHAR(5) PATH '$')
) pi;

SELECT product_id, COUNT(product_id) AS total_vendido
FROM op_products
GROUP BY product_id;


