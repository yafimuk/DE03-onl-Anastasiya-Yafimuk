CREATE OR REPLACE FUNCTION spotify.update_table_column(
    table_name TEXT,
    pk_column TEXT,
    pk_value INT,
    column_name TEXT,
    new_value TEXT
)
RETURNS VOID AS
$$
DECLARE
    sql_query TEXT;
BEGIN
    -- Construct the dynamic SQL query
    sql_query := format(
        'UPDATE %I SET %I = %L WHERE %I = %L',
        table_name, column_name, new_value, pk_column, pk_value
    );

    -- Execute the dynamic SQL query
    EXECUTE sql_query;
END;
$$
LANGUAGE plpgsql;


SELECT spotify.update_table_column('artists', 'artist_id', 10, 'artist_popularity', '95');

select *
from artists
where artist_id = 10