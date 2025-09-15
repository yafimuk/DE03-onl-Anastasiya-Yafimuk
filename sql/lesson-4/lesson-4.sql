SELECT c.customer_id, c.first_name, c.last_name, avg(p.amount) avg_amount
FROM public.customer c
INNER JOIN public.payment p ON c.customer_id = p.customer_id 
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY avg_amount;


SELECT c."name", count(f.title) cnt_films
FROM film f 
INNER JOIN film_category fc ON f.film_id = fc.film_id 
INNER JOIN category c ON fc.category_id = c.category_id
GROUP BY c.category_id
HAVING count(f.title) > 60
ORDER BY cnt_films DESC;

SELECT c."name", count(f.title) cnt_films
FROM film f 
INNER JOIN film_category fc ON f.film_id = fc.film_id 
INNER JOIN category c ON fc.category_id = c.category_id
WHERE f.release_year >= 2000
GROUP BY c.category_id
HAVING count(f.title) > 40
ORDER BY cnt_films DESC
LIMIT 2;

--Общая выручка
SELECT sum(p.amount) total_revenue
FROM payment p;

-- Максимальный и минимальный платеж
SELECT min(p.amount), max(p.amount)
FROM payment p;

-- Средняя длительность по рейтингу
SELECT f.rating, avg(f.length) avg_length
FROM film f 
GROUP BY f.rating;

-- Выручка и число платежей по магазинам
SELECT s.store_id, count(payment_id) cnt_payment, sum(p.amount) sum_amount
FROM payment p 
INNER JOIN staff s ON p.staff_id = p.staff_id 
GROUP BY s.store_id
ORDER BY sum_amount DESC;

-- Топ-5 клиентов по тратам
SELECT c.customer_id, c.first_name, c.last_name, count(p.payment_id) cnt_payment, sum(amount) sum_amount
FROM customer c 
INNER JOIN payment p ON c.customer_id = p.customer_id 
GROUP BY c.customer_id 
ORDER BY cnt_payment desc, sum_amount DESC
LIMIT 5;


SELECT c.customer_id, c.first_name, c.last_name, s.store_id, ad.address, a.address , ct.city 
FROM customer c 
JOIN store s ON c.store_id = s.store_id  
JOIN address ad ON s.address_id = ad.address_id 
JOIN address a ON c.address_id = a.address_id 
JOIN city ct ON a.city_id = ct.city_id 
WHERE ct.city LIKE 'A%';




CREATE OR REPLACE VIEW public.customer_list
AS 
SELECT cu.customer_id AS id,
    (cu.first_name || ' '::text) || cu.last_name AS name,
    a.address,
    a.postal_code AS "zip code",
    a.phone,
    city.city,
    country.country,
        CASE
            WHEN cu.activebool THEN 'active'::text
            ELSE ''::text
        END AS notes,
    cu.store_id AS sid
   FROM customer cu
     JOIN address a ON cu.address_id = a.address_id
     JOIN city ON a.city_id = city.city_id
     JOIN country ON city.country_id = country.country_id;


SELECT *
FROM public.customer_list



SELECT *
FROM (
	SELECT cu.customer_id AS id,
    (cu.first_name || ' '::text) || cu.last_name AS name,
    a.address,
    a.postal_code AS "zip code",
    a.phone,
    city.city,
    country.country,
        CASE
            WHEN cu.activebool THEN 'active'::text
            ELSE ''::text
        END AS notes,
    cu.store_id AS sid
   FROM customer cu
     JOIN address a ON cu.address_id = a.address_id
     JOIN city ON a.city_id = city.city_id
     JOIN country ON city.country_id = country.country_id
) t


WITH cte_customer_list AS (
	SELECT cu.customer_id AS id,
	    (cu.first_name || ' '::text) || cu.last_name AS name,
	    a.address,
	    a.postal_code AS "zip code",
	    a.phone,
	    city.city,
	    country.country,
	        CASE
	            WHEN cu.activebool THEN 'active'::text
	            ELSE ''::text
	        END AS notes,
	    cu.store_id AS sid
	   FROM customer cu
	     JOIN address a ON cu.address_id = a.address_id
	     JOIN city ON a.city_id = city.city_id
	     JOIN country ON city.country_id = country.country_id
)


SELECT *
FROM cte_customer_list

