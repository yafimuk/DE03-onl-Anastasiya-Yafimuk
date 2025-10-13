SELECT pid, usename, application_name, state
FROM pg_stat_activity
WHERE datname = 'shop';

SELECT *
FROM pg_database;

DROP DATABASE IF EXISTS shop;

CREATE DATABASE shop
	WITH OWNER = 'postgres'
	ENCODING = 'UTF8';


CREATE TABLE IF NOT EXISTS public.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name  VARCHAR(50),
    last_name   VARCHAR(50),
    email       VARCHAR(100) UNIQUE,
    created_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id    SERIAL PRIMARY KEY,
    customer_id INT REFERENCES public.customers(customer_id),
    product     VARCHAR(100),
    amount      NUMERIC(10,2),
    created_at  TIMESTAMP DEFAULT now()
);

DROP DATABASE shop;


DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'test_user') THEN
      CREATE USER test_user WITH PASSWORD '12345';
   END IF;
END$$;

GRANT SELECT, INSERT ON public.customers TO test_user;
REVOKE SELECT, INSERT ON public.customers FROM test_user;


SELECT current_user;

SET ROLE 'test_user';

SELECT *
FROM public.customers;

DELETE FROM public.customers;


SET ROLE 'postgres';



INSERT INTO public.customers(first_name,last_name,email)
VALUES
('Alice','Moore','alice@example.com'),
('Bob','Stone','bob@example.com')
ON CONFLICT (email) DO NOTHING;



DROP TABLE public.customers CASCADE;
DELETE FROM public.customers;


ALTER TABLE customers ADD COLUMN phone varchar(13)

SELECT *
FROM public.customers

UPDATE public.customers
SET phone = '+111165651'
WHERE last_name = 'Stone'


ALTER TABLE customers DROP COLUMN phone

ALTER TABLE customers RENAME COLUMN email TO email_address




