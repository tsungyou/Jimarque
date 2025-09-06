#windows
.env/Script/activate
#
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

#temp
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process


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



CREATE TABLE IF NOT EXISTS public.zacks_rank (
  symbol            TEXT            NOT NULL,         -- 股票代號
  cl                NUMERIC(18,4),                    -- 收盤價 close
  volatility        NUMERIC(12,6),                    -- 波動度（比例）
  cap               NUMERIC(20,2),                    -- 市值
  pe_ratio          NUMERIC(18,6),                    -- 本益比
  earnings          NUMERIC(18,6),                    -- 每股盈餘等數值
  vol               NUMERIC(20,0),                    -- 交易量（用數值整數欄位以容納大數）
  yrhigh            NUMERIC(18,4),                    -- 52週高
  yrlow             NUMERIC(18,4),                    -- 52週低
  hi                NUMERIC(18,4),                    -- 當日高
  lo                NUMERIC(18,4),                    -- 當日低
  op                NUMERIC(18,4),                    -- 開盤價
  zacks_rank        INTEGER,                          -- 1~5
  zacks_rank_text   TEXT,                             -- 對應文字
  da                DATE            NOT NULL,         -- previous_close_date 轉 DATE
  PRIMARY KEY (symbol, da)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON
    public.zacks_rank
TO PUBLIC;