-- Создаем таблицу клиентов
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу заказов
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL DEFAULT CURRENT_DATE,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Заполняем таблицу customers
INSERT INTO customers (first_name, last_name, email)
VALUES 
('Ivan', 'Ivanov', 'ivan@example.com'),
('Petr', 'Petrov', 'petr@example.com'),
('Anna', 'Sidorova', 'anna@example.com'),
('Maria', 'Petrova', 'maria@example.com');

-- Заполняем таблицу orders
INSERT INTO orders (customer_id, order_date, amount, status)
VALUES
((SELECT customer_id FROM customers WHERE lower(last_name) = 'ivanov'), '2025-09-01', 1500.00, 'paid'),
((SELECT customer_id FROM customers WHERE lower(last_name) = 'petrov'), '2025-09-05', 2300.50, 'pending'),
((SELECT customer_id FROM customers WHERE lower(last_name) = 'petrov'), '2025-09-07', 500.00, 'cancelled'),
((SELECT customer_id FROM customers WHERE lower(last_name) = 'sidorova'), '2025-09-08', 1200.75, 'paid');
((SELECT customer_id FROM customers WHERE lower(last_name) = 'sidorova'), '2025-09-08', 1200.75, 'paid'))

SELECT *
FROM customers

SELECT *
FROM orders


SELECT c.first_name, c.last_name, o.order_date, o.status
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id


SELECT c.first_name, c.last_name, o.order_date, o.status
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;


SELECT c.first_name, c.last_name, o.order_date, o.status
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date IS NULL;


SELECT c.first_name, c.last_name, o.order_date, o.status
FROM customers c
RIGHT JOIN orders o ON c.customer_id = o.customer_id;



SELECT c.first_name, c.last_name, o.order_date, o.status
FROM customers c
FULL JOIN orders o ON c.customer_id = o.customer_id;


A  B
id id
1  1	
2 
3  3
4  4
5 
   6

  
SELECT *
FROM A
FULL join b ON A.id = B.id

1	1
2 NULL
3	3
4	4
5	NULL
NULL 6


SELECT c.first_name, c.last_name, o.order_date, o.status
FROM customers c
CROSS JOIN orders o



SELECT *
FROM public.actor

SELECT *
FROM public.customer

SELECT first_name, last_name
FROM actor a 
UNION --union all
SELECT first_name, last_name
FROM customer c



