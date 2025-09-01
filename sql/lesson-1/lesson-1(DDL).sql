CREATE TABLE IF NOT EXISTS customers (
	customer_id serial PRIMARY KEY,
	full_name text
);

CREATE TABLE IF NOT EXISTS orders (
	order_id int PRIMARY KEY, 
	order_date date NOT NULL, 
	customer_id int,
	FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);


INSERT INTO customers (full_name)
VALUES 
	('Иванова Иван'),
	('Петров Николай'),
	('Сидоров Василий')


INSERT INTO orders (order_id, order_date, customer_id)
VALUES 
	(1, CURRENT_DATE, 1),
	(2, CURRENT_DATE, 3),
	(3, '2025-08-30', 5)	
	
	
