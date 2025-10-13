CREATE TABLE IF NOT EXISTS public.groups (
    id SERIAL PRIMARY KEY,
    group_name VARCHAR(50) NOT NULL
);


CREATE TABLE IF NOT EXISTS public.students (
    student_id SERIAL PRIMARY KEY, 
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,     
    group_id INT,
    CONSTRAINT fk_group
        FOREIGN KEY (group_id)       
        REFERENCES groups(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.enrollments (
    student_id INT,
    group_id INT,
    enrollment_date DATE,
    PRIMARY KEY (student_id, group_id),     
    FOREIGN KEY (student_id) REFERENCES students(student_id),
    FOREIGN KEY (group_id) REFERENCES groups(id)
);

DROP INDEX idx_students_lastname;
CREATE INDEX idx_students_lastname
ON students(last_name);


INSERT INTO public.students (first_name,last_name,email, group_id)
VALUES ('Ivan', 'Ivanov', 'ivan@gmail.com', NULL)


CREATE TABLE IF NOT EXISTS public.customers (
    student_id SERIAL PRIMARY KEY, 
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL
);


INSERT INTO students (first_name, last_name)
SELECT
    'Name' || i,
    CASE WHEN i % 5 = 0 THEN 'Ivanov' ELSE 'Other' || i END
FROM generate_series(1, 1000000) AS s(i);


EXPLAIN ANALYZE
SELECT *
FROM public.students
WHERE last_name = 'Ivanov'

----INSERT/UPDATE
DROP TABLE IF EXISTS students;
CREATE TABLE students (
    id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name  VARCHAR(50)
) WITH (fillfactor = 70);  -- оставим свободное место в страницах для HOT

-- 1 000 000 строк: каждая 5-я — 'Ivanov', остальные 'OtherXXXXX'
INSERT INTO students (first_name, last_name)
SELECT
    'Name' || i,
    CASE WHEN i % 5 = 0 THEN 'Ivanov' ELSE 'Other' || i END
FROM generate_series(1, 1000000) AS s(i);

-- индекс на last_name (его обновление будем «стоить» дороже)
CREATE INDEX idx_students_lastname ON students(last_name);

VACUUM ANALYZE students;  -- освежим статистику


-- Сбросим счётчики для чистоты эксперимента (необязательно, но наглядно):
SELECT pg_stat_reset();

-- Обновим НЕиндексируемый столбец у ~80% строк
EXPLAIN ANALYZE
UPDATE students
SET first_name = first_name || '_x'
WHERE last_name LIKE 'Other%';



SELECT pg_stat_reset();

EXPLAIN ANALYZE
UPDATE students
SET last_name = last_name || '_upd'
WHERE last_name = 'Ivanov';