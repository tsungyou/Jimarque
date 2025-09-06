import requests
import psycopg2 # type: ignore
from datetime import datetime
from config import db_params, base_url


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

syms = ['TSLA']
sql_str = ""

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
    print(list_symbol)

    # 把值轉成 SQL 字串（文字要加引號，數字不用）
    values_sql = []
    for v in list_symbol:
        if v is None:
            values_sql.append("NULL")
        elif isinstance(v, (int, float)):
            values_sql.append(str(v))
        else:
            safe_v = str(v).replace("'", "''")
            values_sql.append(f"'{safe_v}'")

    values_str = ", ".join(values_sql)
    sql_str += f"""
    INSERT INTO public.zacks_rank ({", ".join(postgres_col_names)})
    VALUES ({values_str});
    """
    print(sql_str)
    cur.execute(sql_str)
    conn.commit()
