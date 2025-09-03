SELECT *
FROM film f
WHERE f.release_year <= 2013;

SELECT *
FROM film f 
WHERE f.length > 100 AND f.length < 120;

SELECT *
FROM film f 
WHERE f.release_year != 2013 -- <>

SELECT *
FROM film f
WHERE f.length = 114 OR f.length = 63

SELECT *
FROM film f 
WHERE title LIKE 'A%S';

SELECT *
FROM film f
WHERE f.title LIKE 'A_A%'

SELECT *
FROM film f
WHERE f.release_year IN (1996, 1997, 1998, 2000) 
--WHERE 
--	f.release_year = 1996 
--	OR f.release_year = 1997
--	OR f.release_year = 1998

SELECT *
FROM film f 
WHERE f.release_year BETWEEN 1996 AND 1998
ORDER BY f.release_year DESC, title
LIMIT 5 OFFSET 5;


SELECT DISTINCT a.first_name 
FROM actor a
ORDER BY a.first_name;


--EXPLAIN ANALYZE
--SELECT a.first_name, count (a.first_name)
--FROM actor a 
--GROUP BY a.first_name
--HAVING count (a.first_name) >= 1
--
--
--EXPLAIN ANALYZE
--SELECT DISTINCT 
--	a.first_name,
--	count(first_name)over(PARTITION BY first_name) cnt
--FROM actor a
--ORDER BY first_name


SELECT *
FROM payment p
WHERE p.amount >= 5.00 AND (p.payment_date >= '2017-06-01' AND p.payment_date <= '2017-06-30') -- p.payment_date between 2017-06-01' and '2017-06-30'
ORDER BY p.amount DESC, p.payment_date ASC, p.payment_id 
LIMIT 30 OFFSET 30;

SELECT EXTRACT(YEAR FROM CURRENT_DATE), EXTRACT(MONTH FROM CURRENT_DATE);

SELECT *
FROM payment p
WHERE EXTRACT (YEAR FROM payment_date) = 2017 AND EXTRACT (MONTH FROM payment_date) = 6;

SELECT date_trunc('month',current_date) - INTERVAL '2' MONTH; 
SELECT date_trunc('day',current_date) + INTERVAL '1' DAY;
