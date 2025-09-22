-- INSERT
INSERT INTO customer (customer_id, store_id, first_name, 
last_name, email, address_id, active)
VALUES 
	(5000, 1, 'Bart', 'Simpson', 'bart.simpson@example.com', 1, 1)
RETURNING *;

SELECT *
FROM customer
WHERE customer_id = 5000


INSERT INTO customer (customer_id, store_id, first_name, 
last_name, email, address_id, active)
VALUES 
	(5001, 1, 'Marge', 'Simpson', 'marge.simpson@example.com', 1, 1),
	(5002, 1, 'Lisa', 'Simpson', 'lisa.simpson@example.com', 1, 1)
RETURNING *;


SELECT *
FROM customer
WHERE last_name = 'Simpson'


INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id)
VALUES 
	(current_date, 1, (SELECT customer_id FROM customer WHERE first_name = 'Homer' AND last_name = 'Simpson'), '2025-09-23 05:10:14.996 +0300', 1)
RETURNING *;

SELECT *
FROM rental
WHERE customer_id = 5000


INSERT INTO customer (customer_id, store_id, first_name, 
last_name, email, address_id, active)
SELECT 5000, 1, 'Bart', 'Simpson', 'bart.simpson@example.com', 1, 1
WHERE NOT EXISTS (
SELECT 1 FROM customer WHERE email = 'bart.simpson@example.com')
RETURNING *;

--UPDATE
UPDATE customer
SET email = 'homer.simpson@example.com',
	first_name = 'Homer'
WHERE first_name = 'Bart'
RETURNING *;

UPDATE film
SET rental_rate = rental_rate + 1
WHERE length > 180
RETURNING *;


UPDATE customer c 
SET last_update = max_rental_date
FROM (
	 SELECT r.customer_id, max(r.rental_date) AS max_rental_date
	 FROM rental r 
	 GROUP BY r.customer_id
) r
WHERE c.customer_id  = r.customer_id 
RETURNING *;


--DELETE / TRUNCATE
DELETE FROM rental
WHERE customer_id = 5000;

DELETE FROM customer 
WHERE first_name = 'Homer'
RETURNING *;


CREATE TABLE rental_copy AS
SELECT * FROM rental;

SELECT *
FROM rental_copy

DELETE FROM rental_copy
RETURNING *;


TRUNCATE TABLE rental_copy; 


--TRANSACTIONS

BEGIN TRANSACTION;

DELETE FROM rental
WHERE customer_id = 5000;

DELETE FROM customer 
WHERE first_name = 'Homer'
RETURNING *;

COMMIT;


BEGIN TRANSACTION;

DELETE FROM rental
WHERE customer_id = 5000;

DELETE FROM customer 
WHERE first_name = 'Homer'
RETURNING *;

ROLLBACK;


SELECT *
FROM customer 
WHERE first_name = 'Homer'



-- Добавьте в таблицу film 5 фильмов одним INSERT … VALUES, указав название, язык (English), длительность и цену аренды; выведите film_id и названия.
WITH cte AS (
SELECT 
'Data Dreams', (SELECT language_id FROM "language" l WHERE lower(l."name") = 'english'), 95, 2.99, ARRAY['Trailers','Commentaries']
UNION ALL
SELECT
'Skyfalling', (SELECT language_id FROM "language" l WHERE lower(l."name") = 'english'), 120, 3.99, ARRAY['Deleted Scenes']
UNION ALL
SELECT
'Ocean Shadows', (SELECT language_id FROM "language" l WHERE lower(l."name") = 'english'), 88, 1.99, ARRAY['Trailers']
UNION ALL
SELECT
'Silent Code', (SELECT language_id FROM "language" l WHERE lower(l."name") = 'english'), 55, 0.99, ARRAY['Behind the Scenes']
UNION ALL
SELECT
'Future Echoes', (SELECT language_id FROM "language" l WHERE lower(l."name") = 'english'), 150, 4.99, ARRAY['Trailers','Deleted Scenes']

)

INSERT INTO film (title, language_id, length, rental_rate, special_features)
SELECT *
FROM cte 
WHERE NOT EXISTS (
	SELECT 1 FROM film WHERE title in ('Data Dreams', 'Skyfalling', 'Ocean Shadows', 'Silent Code', 'Future Echoes'))
RETURNING *;  
  

INSERT INTO film (title, language_id, length, rental_rate, special_features)
VALUES
  ('Data Dreams', 1, 95, 2.99, ARRAY['Trailers','Commentaries']),
  ('Skyfalling', 1, 120, 3.99, ARRAY['Deleted Scenes']),
  ('Ocean Shadows', 1, 88, 1.99, ARRAY['Trailers']),
  ('Silent Code', 1, 55, 0.99, ARRAY['Behind the Scenes']),
  ('Future Echoes', 1, 150, 4.99, ARRAY['Trailers','Deleted Scenes']);


--В таблице rental установите return_date = NOW() для аренд старше 10 дней без возврата (return_date IS NULL); выведите 10 обновлённых строк.
UPDATE rental
SET return_date = NOW()
WHERE return_date IS NULL
  AND rental_date < NOW() - INTERVAL '10 days'
RETURNING rental_id, rental_date, return_date;

--Удалите из payment все транзакции < 1.00 до 2006 года и выведите количество удалённых строк.
WITH deleted AS (
  DELETE FROM payment
  WHERE amount < 1.00
    AND payment_date < '2017-01-26'
  RETURNING payment_id
)
SELECT count(*) AS deleted_rows FROM deleted;


--Напишите транзакцию: вставьте клиента, создайте аренду, затем попытайтесь добавить платёж с 
-- неверным staff_id; из-за ошибки выполните ROLLBACK и убедитесь, что клиент и аренда не сохранились.

BEGIN TRANSACTION;

INSERT INTO customer (customer_id, store_id, first_name, last_name, address_id, active)
VALUES (5005, 1, 'Test', 'Rollback', 1, 1)
RETURNING customer_id;

INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
VALUES (NOW(), 1, 5005, 1)
RETURNING rental_id;

ROLLBACK;


SELECT * FROM customer WHERE first_name = 'Test' AND last_name = 'Rollback';