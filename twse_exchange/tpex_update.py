import requests # type: ignore
import pandas as pd # type: ignore
import psycopg2 # type: ignore
from psycopg2 import sql # type: ignore
from tqdm import tqdm # type: ignore
from datetime import datetime, timedelta
import random
import time
from util import get_init_period_trade_cal, db_params, \
    create_table, get_all_codes, sleeper, \
    get_ts, get_trade_cal_from_postgres, get_latest_da, test, \
    tpex_price_fetch_based_on

def get_tpex_maincode():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # tpex maincode
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date=2025%2F09%2F04&response=json"
    res = requests.get(url)
    data = res.json()
    data_copy = data.copy()
    data_copy = data_copy['tables'][0]['data']

    data_filtered = []
    for i in range(len(data_copy)):
        if "購" in data_copy[i][1]:
            continue
        if "售0" in data_copy[i][1]:
            continue
        if "U" in data_copy[i][0]:
            continue
        for j in range(2, 7):
            if data_copy[i][j] == "---":
                continue
        
        a = data_copy[i][0:3]
        b = data_copy[i][4:7]
        c = data_copy[i][8:11]
        d = data_copy[i][15]
        data_filtered.append(a + b + c + [d])

    for i in tqdm(data_filtered):
        sql_str = f"""
        INSERT INTO public.maincode (code, cname, exchange)
        VALUES ('{i[0]}', '{i[1]}', 'tpex') on conflict (code) do nothing;
        """
        cursor.execute(sql_str)
        conn.commit()

def get_tpex_peratio():
    # tpex pe_ratio

    '''
    uri = "https://www.tpex.org.tw/www/zh-tw/afterTrading/PERatio"
    uri = "https://www.tpex.org.tw/www/zh-tw/afterTrading/PERatio?date=2025%2F09%2F03&response=json"
    date=2025%2F09%2F04&id=&response=json
    '''

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    trade_cal = get_init_period_trade_cal('tpex_peratio', t_='postgres')

    if len(trade_cal) > 0:
        for dat in tqdm(trade_cal):
            sleeper(50)
            year,month,day = dat.strftime('%Y-%m-%d').split('-')
            uri = f"https://www.tpex.org.tw/www/zh-tw/afterTrading/PERatio?date={year}%2F{month}%2F{day}&response=json"
            
            res = requests.get(uri)
            data = res.json()['tables'][0]['data']
            sql_str = ""
            da = f"{year}{month}{day}"
            for i in data:
                i[5] = i[5].replace('N/A', "0")
                i[3] = i[3].replace(',', '')
                i[4] = i[4].replace(',', '')
                i[5] = i[5].replace(',', '')
                sql_str += f"""INSERT INTO public.tpex_peratio (da, code, cname, cl, eps, pe_ratio)
                VALUES ('{da}', '{i[1]}', '{i[2]}', '{i[3]}', '{i[4]}', '{i[5]}') on conflict (da, code) do nothing;"""
            cursor.execute(sql_str)
            conn.commit()
    else:
        print("Error: get_tpex_peratio No new data")

def _tpex_price_insert(year, month, day, cursor, conn):
    sleeper(100)
    url = f"https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date={year}%2F{month}%2F{day}&response=json"
    res = requests.get(url)
    data = res.json()['tables'][0]['data']
    data_copy = data.copy()
    da = res.json()['date']

    data_filtered = []
    for i in range(len(data_copy)):
        if "購" in data_copy[i][1]:
            continue
        if "售0" in data_copy[i][1]:
            continue
        if "U" in data_copy[i][0]:
            continue
        if '櫃檯買賣' in data_copy[i][0]:
            continue
        if data_copy[i][2].strip() == "---":
            continue
        if data_copy[i][3] == "---":
            continue
        if data_copy[i][4] == "---":
            continue
        if data_copy[i][5] == "---":
            continue
        if data_copy[i][6] == "---":
            continue
        a = data_copy[i][0]
        b = data_copy[i][2]
        e = data_copy[i][4:7]
        c = data_copy[i][8:11]
        d = data_copy[i][15]
        data_filtered.append([a] + [b] + e + c + [d])
    sql_str = ""
    for i in data_filtered:
        for j in range(len(i)):
            if isinstance(i[j], str):
                i[j] = i[j].replace(',', '')
            if i[j] == '':
                i[j] = None
        sql_str += f"""
        INSERT INTO public.price (da, code, cl, op, hi, lo, stock_volume, cash_volume, number_of_trades)
        VALUES ('{da}', '{i[0]}', '{i[1]}', '{i[2]}', '{i[3]}', '{i[4]}', '{i[5]}', '{i[6]}', '{i[7]}') on conflict (da, code) do nothing;
        """
    cursor.execute(sql_str)
    conn.commit()
    # # shares issued
    # sql_str = f"""
    # INSERT INTO public.shares_issued (code, shares_issued)
    # VALUES ('{i[0]}', {i[2]}) on conflict (code) do nothing;
    # """
    # cursor.execute(sql_str)
    # conn.commit()

    # # shares_issued_change

def update_tpex_price():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(f"""
        select max(da) from price where code = '{tpex_price_fetch_based_on}';
    """)
    da = cursor.fetchone()[0]
    trade_cal_all = get_trade_cal_from_postgres()
    acal = pd.to_datetime(pd.Series(trade_cal_all))
    pivot = pd.to_datetime(da)
    trade_cal = acal[acal > pivot]
    for da in tqdm(trade_cal, desc="tpex price update"):
        year, month, day = da.strftime('%Y-%m-%d').split('-')
        _tpex_price_insert(year, month, day, cursor, conn)

def init_tpex_price():
    '''
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date=2025%2F09%2F04&response=json"
    '''
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(f"""
                   select max(da) from price where code = '{tpex_price_fetch_based_on}';
                   """)
    da = cursor.fetchone()[0]
    trade_cal_all = get_trade_cal_from_postgres()
    if da is None:
        trade_cal = trade_cal_all
    else:
        acal = pd.to_datetime(pd.Series(trade_cal_all))
        pivot = pd.to_datetime(da)
        trade_cal = acal[acal > pivot]
    a = datetime.now()
    print("init_tpex_price start at ", a.strftime("%Y-%m-%d %H:%M:%S"), trade_cal.iloc[0])
    for da in tqdm(trade_cal, desc="tpex price initialization"):
        year, month, day = da.strftime('%Y-%m-%d').split('-')
        _tpex_price_insert(year, month, day, cursor, conn)
    print("init_tpex_price end at ", datetime.now())
    print("init_tpex_price cost ", datetime.now() - a)

if __name__ == "__main__":
    test()
    init_tpex_price()