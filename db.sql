#checker
SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin 
FROM pg_roles;

#check table
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public';

# check columns
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;


CREATE TABLE IF NOT EXISTS price_mt5_m1 (
    symbol TEXT NOT NULL,
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    op DOUBLE PRECISION,
    hi DOUBLE PRECISION,
    lo DOUBLE PRECISION,
    cl DOUBLE PRECISION,
    tick_volume BIGINT,
    spread INTEGER,
    da DATE,
    hms TEXT,
    PRIMARY KEY (symbol, time)
);

CREATE TABLE IF NOT EXISTS price_mt5_m5 (LIKE price_mt5_m1 INCLUDING ALL);
CREATE TABLE IF NOT EXISTS price_mt5_m15 (LIKE price_mt5_m1 INCLUDING ALL);
CREATE TABLE IF NOT EXISTS price_mt5_d1 (LIKE price_mt5_m1 INCLUDING ALL);


GRANT SELECT, INSERT, UPDATE, DELETE ON
    price_mt5_m1,
    price_mt5_m5,
    price_mt5_m15,
    price_mt5_d1
TO PUBLIC;