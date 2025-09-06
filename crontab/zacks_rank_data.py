import requests
import psycopg2 # type: ignore
from datetime import datetime
from config import db_params, base_url
import pandas as pd


'''
npm install zacks-api --global
zacks-api TSLA
'''

columns_to_save = [
    "zacks_rank", "zacks_rank_text", 
]

da_to_save = ["previous_close_date"]

columns_source_sungard = [
    "close", "volatility", "market_cap", "pe_ratio", "earnings", "volume",
    "yrhigh", "yrlow", "day_high", "day_low", "open"
]

postgres_col_names = [
    "symbol", "cl", "volatility", "cap", "pe_ratio", "earnings", "vol",
    "yrhigh", "yrlow", "hi", "lo", "op", "zacks_rank", "zacks_rank_text", "da"
]

def init_zacks_rank_table():
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    sql_str = """

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
    """
    cur.execute(sql_str)
    conn.commit()
    cur.close()
    conn.close()

def get_zacks_data(init=False):
    if init:
        init_zacks_rank_table()
    
    sym_csv = pd.read_csv("new_us_symbols.csv")
    syms = sym_csv["code"].tolist()
    # sql_str = ""

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    for symbol in syms:
        url = f"{base_url}{symbol}"
        print(url)
        res = requests.get(url)
        js = res.json()[symbol]
        sungard = js['source']['sungard']

        # 把 symbol + sungard 欄位 + 其他欄位拼成 list
        list_symbol = [symbol] + [sungard[i] for i in columns_source_sungard] + [
            js["zacks_rank"], js["zacks_rank_text"], datetime.strptime(js["previous_close_date"], "%m/%d/%Y").strftime("%Y-%m-%d")
        ]

        # 把值轉成 SQL 字串（文字要加引號，數字不用）
        values_sql = []
        for v in list_symbol:
            if v is None or v == "":
                values_sql.append("NULL")
            elif isinstance(v, (int, float)):
                values_sql.append(str(v))
            else:
                safe_v = str(v).replace("'", "''")
                values_sql.append(f"'{safe_v}'")

        values_str = ", ".join(values_sql)
        sql_str = f"""
        INSERT INTO public.zacks_rank ({", ".join(postgres_col_names)})
        VALUES ({values_str}) on conflict (symbol, da) do nothing;
        """
        cur.execute(sql_str)
        conn.commit()
