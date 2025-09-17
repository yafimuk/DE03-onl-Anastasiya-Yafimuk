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