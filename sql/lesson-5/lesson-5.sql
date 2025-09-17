-- Скалярные подзапросы
SELECT *
FROM film f 
WHERE f.language_id = (SELECT language_id
					   FROM "language" l 
					   WHERE lower(l."name") = 'english')
					   
					   					  

SELECT *
FROM film f 
INNER JOIN film_category fc ON f.film_id = fc.film_id 
WHERE fc.category_id = (SELECT c.category_id  
						FROM category c 
						WHERE lower(c."name") = 'family')
					   
SELECT *
FROM customer c 
INNER JOIN payment p ON c.customer_id = p.customer_id
WHERE p.amount = (SELECT max(amount)
				  FROM payment p)

				  
-- Многозначные подзапросы
SELECT *
FROM film f 
WHERE f.language_id IN (SELECT language_id
					   FROM "language" l 
					   WHERE lower(l."name") = 'english' 
					   OR lower(l."name") = 'italian')				  
				  
				   
					   
SELECT *
FROM film f 
INNER JOIN film_category fc ON f.film_id = fc.film_id 
WHERE fc.category_id IN (SELECT c.category_id  
						FROM category c 
						WHERE lower(c."name") = 'family'
						OR lower(c."name") = 'sports')
						
-- Коррелированные подзапросы
SELECT *
FROM film f
WHERE f.rental_rate > (
		SELECT avg(rental_rate)
		FROM film);

SELECT f.title
FROM film f
WHERE f.film_id IN (
	SELECT i.film_id
    FROM inventory i
    WHERE f.film_id = i.film_id)

SELECT f.title
FROM film f
INNER JOIN inventory i ON f.film_id = i.film_id
    
    
SELECT f.title
FROM film f
WHERE f.film_id IN (
    SELECT i.film_id
    FROM inventory i
    JOIN rental r ON i.inventory_id = r.inventory_id
    WHERE r.return_date IS NOT NULL
    GROUP BY i.film_id
    HAVING AVG(r.return_date - r.rental_date) = (
        SELECT MAX(avg_duration)
        FROM (
            SELECT AVG(r2.return_date - r2.rental_date) AS avg_duration
            FROM inventory i2
            JOIN rental r2 ON i2.inventory_id = r2.inventory_id
            WHERE r2.return_date IS NOT NULL
            GROUP BY i2.film_id
        ) x
    )
);
	

-- можем писать подзапросы в...
-- in FROM
SELECT *
FROM (SELECT * FROM film f WHERE f.length > 100) t 
WHERE t.release_year > 2005
ORDER BY t.release_year, t.length 


--in SELECT
SELECT f.title, (SELECT MAX(length) FROM film) max_length
FROM film f 


SELECT *
FROM (SELECT * FROM film f WHERE f.length > 100) t 
WHERE t.release_year > (SELECT avg(film.release_year) FROM film)
ORDER BY t.release_year, t.length 
	

--CTE = common table expression  
WITH category_with_family AS (
	SELECT *
	FROM category c 
	WHERE lower(c."name") = 'family'
)


SELECT *
FROM film f 
INNER JOIN film_category fc ON f.film_id = fc.film_id 
WHERE fc.category_id = (SELECT category_id FROM category_with_family)


SELECT *
FROM film f 
INNER JOIN film_category fc ON f.film_id = fc.film_id 
JOIN cat c ON fc.category_id = c.category_id







--Пример
-- 1. Найти всех клиентов, которые арендовали хотя бы один фильм категории Comedy.
SELECT DISTINCT c.first_name, c.last_name
FROM customer c
JOIN rental r ON c.customer_id = r.customer_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film_category fc ON i.film_id = fc.film_id
JOIN category cat ON fc.category_id = cat.category_id
WHERE lower(cat.name) = 'comedy';


SELECT DISTINCT c.first_name, c.last_name
FROM customer c
WHERE c.customer_id IN (SELECT r.customer_id 
						FROM rental r 
						JOIN inventory i ON r.inventory_id = i.inventory_id
						JOIN film_category fc ON i.film_id = fc.film_id
						JOIN category cat ON fc.category_id = cat.category_id
						WHERE lower(cat.name) = 'comedy')
						
						
WITH list_of_customers AS(
	SELECT r.customer_id 
	FROM rental r 
	JOIN inventory i ON r.inventory_id = i.inventory_id
	JOIN film_category fc ON i.film_id = fc.film_id
	JOIN category cat ON fc.category_id = cat.category_id
	WHERE lower(cat.name) = 'comedy'
)						


SELECT DISTINCT c.first_name, c.last_name
FROM customer c
WHERE c.customer_id IN (SELECT customer_id FROM list_of_customers)


SELECT DISTINCT c.first_name, c.last_name
FROM customer c
JOIN list_of_customers loc ON c.customer_id = loc.customer_id


-- 2. Вывести название фильма и количество раз, которое его брали в аренду.
EXPLAIN ANALYZE 
SELECT f.title, COUNT(r.rental_id) AS rental_count
FROM film f
JOIN inventory i ON f.film_id = i.film_id
JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY f.film_id
ORDER BY rental_count DESC;


EXPLAIN ANALYZE
SELECT f.title, (SELECT count(i.film_id)
				 FROM inventory i 
				 JOIN rental r ON i.inventory_id = r.inventory_id
				 WHERE i.film_id = f.film_id )	rental_count
FROM film f
ORDER BY rental_count DESC;


--3.Найти клиентов, которые не брали ни одного фильма категории Horror.
EXPLAIN ANALYZE
SELECT DISTINCT c.first_name, c.last_name
FROM customer c
LEFT JOIN rental r ON c.customer_id = r.customer_id
LEFT JOIN inventory i ON r.inventory_id = i.inventory_id
LEFT JOIN film_category fc ON i.film_id = fc.film_id
LEFT JOIN category cat ON fc.category_id = cat.category_id AND lower(cat.name) = 'horror'
WHERE cat.category_id IS NULL;



EXPLAIN ANALYZE
SELECT c.first_name, c.last_name
FROM customer c
WHERE c.customer_id NOT IN (
    SELECT DISTINCT r.customer_id
    FROM rental r
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film_category fc ON i.film_id = fc.film_id
    JOIN category cat ON fc.category_id = cat.category_id
    WHERE lower(cat.name) = 'horror'
);



