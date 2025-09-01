select *
from film f 
where f.release_year = 2008

SELECT *
FROM film f
WHERE f.release_year >= 2008 AND f.release_year <= 2010


SELECT *
FROM actor a 
WHERE a.first_name = 'JOHNNY'


SELECT *
FROM address a 
WHERE a.district = 'California'


SELECT *
FROM address a 
WHERE a.district LIKE 'C%'

SELECT *
FROM address a 
WHERE a.district LIKE '%c'

SELECT *
FROM address a 
WHERE a.district LIKE '%lif%'