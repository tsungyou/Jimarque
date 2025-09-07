
import yfinance as yf
import pandas as pd
import psycopg2 # type: ignore
import time
import random
from datetime import datetime

db_params = {
    "dbname": "jimarque",
    "user": "postgres",
    "password": "buddyrich134",
    "host": "localhost",
    "port": 5432
}

main_start = '2015-01-01'
twse_price_fetch_based_on = '1101'
tpex_price_fetch_based_on = '00679B'

def get_trade_cal_from_postgres():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(f"""
                select da from public.price where code = '{twse_price_fetch_based_on}' order by da asc;
                """)
    return [i[0] for i in cursor.fetchall()]

def get_latest_da(table_name):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(f"""
                select max(da) from public.{table_name};
                """)
    return cursor.fetchone()[0]

def get_init_period_trade_cal(table_name, t_='postgres'):
    if t_ == 'postgres':
        trade_cal = get_trade_cal_from_postgres()
    else:
        trade_cal = get_trade_cal_from_postgres()
    latest_da = get_latest_da(table_name)
    if latest_da is None:
        return trade_cal
    try:
        acal = pd.to_datetime(pd.Series(trade_cal))
        pivot = pd.to_datetime(latest_da)
        new_trade_cal = acal[acal > pivot]
        return new_trade_cal
    except TypeError as e:
        print(e)
        return trade_cal 

def get_all_codes():
    '''
    get all codes list from postgres ( from twse.price )
    '''
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(f"""
                select distinct code from public.maincode where exchange = 'twse' order by code asc;
                """)
    codes = [i[0] for i in cursor.fetchall()]
    return codes

def create_table(table_name):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    if table_name == 'block_trading':
        sql_create = """
        CREATE TABLE IF NOT EXISTS public.block_trading (
        da               DATE                NOT NULL,
        code             TEXT                NOT NULL,
        cname            TEXT,
        type_of_trade    TEXT,              -- 交易別
        price_of_trade   NUMERIC(18,4),      -- 成交價
        amount_of_trade   NUMERIC(18,4),      -- 成交股數
        transaction_amount   NUMERIC(18,4),      -- 成交金額
        PRIMARY KEY (da, code, cname, type_of_trade, price_of_trade, amount_of_trade, transaction_amount)
        );
        GRANT SELECT, INSERT, UPDATE, DELETE ON
            public.block_trading
        TO PUBLIC;
        """
    elif table_name == 'stock_yield_pe_pb':
        sql_create = """
        CREATE TABLE IF NOT EXISTS public.stock_yield_pe_pb (
        da               DATE                NOT NULL,
        code             TEXT                NOT NULL,
        cname            TEXT,
        cl               NUMERIC(18,4),      -- 收盤價
        yield            NUMERIC(10,4),      -- 殖利率（百分比，例如 4.32）
        year_of_dividend INTEGER,            -- 配息所屬年度，例如 2024
        pe_ratio         NUMERIC(18,6),
        pb_ratio         NUMERIC(18,6),
        year_season      TEXT,               -- 例如 '113/2'
        PRIMARY KEY (da, code)
        );
        GRANT SELECT, INSERT, UPDATE, DELETE ON
            public.stock_yield_pe_pb
        TO PUBLIC;
        """
    elif table_name == 'maincode':
        sql_create = """
        CREATE TABLE IF NOT EXISTS public.maincode (
        code TEXT PRIMARY KEY,
        cname TEXT,
        exchange TEXT
        );
        GRANT SELECT, INSERT, UPDATE, DELETE ON
            public.maincode
        TO PUBLIC;
        """
    elif table_name == 'price':
        sql_create = """
        CREATE TABLE IF NOT EXISTS public.price (
        da DATE NOT NULL,
        code TEXT NOT NULL,
        stock_volume BIGINT,
        cash_volume BIGINT,
        op NUMERIC(18,4),
        hi NUMERIC(18,4),
        lo NUMERIC(18,4),
        cl NUMERIC(18,4),
        number_of_trades BIGINT,
        PRIMARY KEY (da, code)
        );
        GRANT SELECT, INSERT, UPDATE, DELETE ON
            public.price
        TO PUBLIC;
        """
    elif table_name == 'rynek_timing':
        sql_create = '''
        CREATE TABLE IF NOT EXISTS public.rynek_timing
        (
            da DATE NOT NULL,
            timing_name TEXT NOT NULL
            cl double precision,
            signal TEXT NOT NULL,
            market_type TEXT,
            PRIMARY KEY (da, timing_name)
        )
        GRANT SELECT, INSERT, UPDATE, DELETE ON
            public.rynek_timing
        TO PUBLIC;
        '''
        # tpex
    elif table_name == 'tpex_peratio':
        sql_create = """
        CREATE TABLE IF NOT EXISTS public.tpex_peratio (
        da               DATE                NOT NULL,
        code             TEXT                NOT NULL,
        cname            TEXT,
        cl               NUMERIC(18,4),      -- 收盤價
        eps            NUMERIC(10,4),      -- 殖利率（百分比，例如 4.32）
        pe_ratio         NUMERIC(18,6),
        PRIMARY KEY (da, code)
        );
        GRANT SELECT, INSERT, UPDATE, DELETE ON
            public.tpex_peratio
        TO PUBLIC;
        """
    elif table_name == 'turnover':
        sql_create = """
        create table if not exists public.turnover (
            da DATE NOT NULL,
            code TEXT NOT NULL,
            shares_volume bigint,
            shares_issued bigint,
            turnover double precision,
            PRIMARY KEY (da, code)
        );
        GRANT SELECT, INSERT, UPDATE, DELETE ON
            public.turnover
        TO PUBLIC;
        """
    else:
        raise ValueError(f"table_name {table_name} not found")
    cursor.execute(sql_create)
    conn.commit()
    print(f"create {table_name}")
    return None

def sleeper(t=100):
    time.sleep(random.randint(1, t) * 0.1)
    
def get_ts():
    return str(int(datetime.now().timestamp()))

def test():
    import sys, ssl, requests, urllib3
    try:
        import certifi
        cafile = certifi.where()
    except Exception:
        cafile = None

    print("PY:", sys.executable)
    print("VER:", sys.version)
    print("SSL:", ssl.OPENSSL_VERSION)
    print("requests:", requests.__version__, "urllib3:", urllib3.__version__)
    print("CA file:", cafile)
