import requests # type: ignore
import pandas as pd # type: ignore
import psycopg2 # type: ignore
from psycopg2 import sql # type: ignore
from tqdm import tqdm # type: ignore
from datetime import datetime, timedelta
import random
import time
from util import get_init_period_trade_cal, db_params, \
    create_table, get_all_codes, sleeper, get_ts, test

__all__ = [
    'get_stock_yield_pe_pb_from_twse',
    'build_twse_maincode_from_postgres_stock_yield_pe_pb',
    'get_price_from_twse',
    'get_block_trading_from_twse',
]

def get_stock_yield_pe_pb_from_twse():
    table_name = 'stock_yield_pe_pb'
    '''
    https://www.twse.com.tw/zh/trading/historical/bwibbu-day.html   
    https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date=20240809&selectType=ALL&response=json
    '''

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    trade_cal = get_init_period_trade_cal(table_name, t_='postgres')
    # create_table(table_name)
    a = datetime.now()
    print("get_stock_yield_pe_pb_from_twse start at ", a.strftime("%Y-%m-%d %H:%M:%S"), trade_cal.iloc[0])
    for da in tqdm(trade_cal):
        sql_str = ""
        formatted_da = da.strftime("%Y%m%d")
        sleeper(70)
        try:
            _ts = str(int(datetime.now().timestamp()))
            uri = f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date={formatted_da}&selectType=ALL&response=json&_={_ts}"
            res = requests.get(uri)
            data = res.json()['data']
            if len(data[0]) == 8:
                for i in data:
                    i = [j if (j != '-') else 0.0 for j in i]
                    i = [j.replace(',', '') if isinstance(j, str) else j for j in i]
                    sql_str += f"""INSERT INTO public.stock_yield_pe_pb (da, code, cname, cl, yield, year_of_dividend, pe_ratio, pb_ratio, year_season)
                    VALUES ('{formatted_da}', '{i[0]}', '{i[1]}', '{i[2]}', '{i[3]}', '{i[4]}', '{i[5]}', '{i[6]}', '{i[7]}') on conflict (da, code) do nothing;"""
            elif len(data[0]) == 5:
                for i in data:
                    i = [j if (j != '-') else 0.0 for j in i]
                    i = [j.replace(',', '') if isinstance(j, str) else j for j in i]
                    sql_str += f"""INSERT INTO public.stock_yield_pe_pb (da, code, cname, pe_ratio, yield, pb_ratio)
                    VALUES ('{formatted_da}', '{i[0]}', '{i[1]}', '{i[2]}', '{i[3]}', '{i[4]}') on conflict (da, code) do nothing;"""
            else:
                print(formatted_da, data[0], len(data[0]))
                pass;
            cursor.execute(sql_str)
            conn.commit()
        except KeyError as e:
            print(e, formatted_da)
            pass;
        except Exception as e:
            print(e)
            pass;
    print("get_stock_yield_pe_pb_from_twse end at ", datetime.now())
    print("get_stock_yield_pe_pb_from_twse cost ", datetime.now() - a)

def build_twse_maincode_from_postgres_stock_yield_pe_pb():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute("""
    select distinct code, cname from public.stock_yield_pe_pb;
    """)
    data = [i for i in cursor.fetchall()]

    sql_str = ""
    for i in data:
        sql_str += f"""
        insert into public.maincode (code, cname, exchange) values ('{i[0]}', '{i[1]}', 'twse') on conflict (code) do nothing;
        """
    cursor.execute(sql_str)
    conn.commit()
    return None 
def _price_insert(formatted_da, code, cursor, conn):
    sleeper(30)
    _ts = get_ts()
    try:
        uri = f"https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date={formatted_da}&stockNo={code}&response=json&_={_ts}"
        sql_str = ""
        res = requests.get(uri)
        data = res.json()['data']
        data_list = data.copy()
        for i in data_list:
            list_a = i[0].split('/')
            i = [j if (j != '--') else 0.0 for j in i]
            i[0] = f"{int(list_a[0]) + 1911}{list_a[1]}{list_a[2]}"
            for j in [1, 2, 3, 4, 5, 6, -1]:
                if isinstance(i[j], str):
                    i[j] = i[j].replace(',', '')
            sql_str += f"""INSERT INTO public.price (da, code, stock_volume, cash_volume, op, hi, lo, cl, number_of_trades)
                VALUES ('{i[0]}', '{code}', '{i[1]}', '{i[2]}', '{i[3]}', '{i[4]}', '{i[5]}', '{i[6]}', '{i[8]}') on conflict (da, code) do nothing;"""
        cursor.execute(sql_str)
        conn.commit()
    except Exception as e:
        print(f"Error get_price - {formatted_da} {code} {e}")
        pass;

def _get_existing_codes():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # get tuples from price table
    sql_str = """SELECT code, MAX(da) AS max_da
    FROM public.price where code in (select distinct code from public.maincode where exchange = 'twse')
    GROUP BY code
    ORDER BY code;"""

    cursor.execute(sql_str)
    existing_codes = cursor.fetchall()
    return existing_codes

def init_price_from_twse():
    table_name = 'price' # stock price from twse
    '''
    ts = datetime.now().timestamp()
    _ts = str(int(ts))
    https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20250906&stockNo=1234&response=json&_=1757157975
    '''
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    current_year = datetime.now().year
    success_count = 0;

    codes = get_all_codes()
    existing_codes = _get_existing_codes()

    # initing from price table
    for i in existing_codes:
        sleeper(50)
        code, da = i
        codes.remove(code)
        start_year = int(da.strftime("%Y"))
        start_month = int(da.strftime("%m"))
        for year in range(start_year, current_year+1):
            sleeper(70)
            # start_month
            if year == start_year: month = start_month
            else: month = 1
            
            # end_month
            if year == current_year: end_month = datetime.now().month + 1
            else: end_month = 13
            for month in range(month, end_month):
                # sleep
                try:
                    formatted_da = f"{year}{month:02d}01"
                    _price_insert(formatted_da, code, cursor, conn)
                    success_count += 1
                except Exception as e:
                    print(f"Error - {formatted_da} {code} {e}")
                    pass;
            print(f"Succeed - {year} {code} {success_count}")

    print("================", "start all init", "================")
    start_year = 2015
    start_month = 1
    print(start_year, start_month)
    print('len of inited codes: ', len(codes))
    # inited init
    for code in codes:
        sleeper(50)
        for year in tqdm(range(start_year, current_year+1), desc=f"{code}"):
            # start_month
            if year == start_year: month = start_month
            else: month = 1
            
            # end_month
            if year == current_year: end_month = datetime.now().month + 1
            else: end_month = 13
            for month in range(month, end_month):
                try:
                    success_count += 1
                    formatted_da = f"{year}{month:02d}01"
                    _price_insert(formatted_da, code, cursor, conn)
                except Exception as e:
                    print(f"Error - {formatted_da} {code} {e}")
                    pass;    

def get_price_from_twse():
    a = datetime.now()
    print("get_price_from_twse start at ", a.strftime("%Y-%m-%d %H:%M:%S"))
    table_name = 'price' # stock price from twse
    '''
    _ts = str(int(datetime.now().timestamp()))
    https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20250906&stockNo=1234&response=json&_=1757157975
    '''
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # get tuples from price table
    year, month, day = datetime.now().year, datetime.now().month, datetime.now().day
    formatted_da = f"{year}{month:02d}{day:02d}"
    _ts = str(int(datetime.now().timestamp()))
    cursor.execute("""
    select distinct code from price 
    where da = (select max(da) from price) order by code asc;
    """)
    codes = [i[0] for i in cursor.fetchall()]
    # or from maincode works
    
    for code in tqdm(codes):
        try:
            _price_insert(formatted_da, code, cursor, conn)
        except Exception as e:
            print(f"Error get_price - {formatted_da} {code} {e}")
            pass;
    print("get_price_from_twse end at ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("get_price_from_twse cost ", datetime.now() - a)
    return None

def get_block_trading_from_twse():    
    table_name = 'block_trading'
    '''
    https://www.twse.com.tw/zh/trading/block/bfiauu.html 
    https://www.twse.com.tw/rwd/zh/block/BFIAUU?date=20250905&selectType=S&response=json
    '''
            
    db_params = {
        "dbname": "jimarque",
        "user": "postgres",
        "password": "buddyrich134",
        "host": "localhost",
        "port": 5432
    }

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    trade_cal = get_init_period_trade_cal(table_name)
    sql_str = ""
    iter = 0
    for da in tqdm(trade_cal):
        formatted_da = da.strftime("%Y%m%d")
        a = random.randint(1, 30) * 0.1
        time.sleep(a)
        try:
            uri = f"https://www.twse.com.tw/rwd/zh/block/BFIAUU?date={date}&selectType=S&response=json"
            res = requests.get(uri)
            data = res.json()['data']
            for i in data:
                i = [j if (j != '-') else 0.0 for j in i]
                i = [j.replace(',', '') if isinstance(j, str) else j for j in i]
                sql_str += f"""INSERT INTO public.{table_name} (da, code, cname, type_of_trade, price_of_trade, amount_of_trade, transaction_amount)
                VALUES ('{formatted_da}', '{i[0]}', '{i[1]}', '{i[2]}', '{i[3]}', '{i[4]}', '{i[5]}') on conflict (da, code, cname, type_of_trade, price_of_trade, amount_of_trade, transaction_amount) do nothing;"""

            iter += 1
            if iter > 1:
                cursor.execute(sql_str)
                conn.commit()
                sql_str = ""
                iter = 0
        except Exception as e:
            print(e)
            pass


if __name__ == "__main__":
    get_stock_yield_pe_pb_from_twse()
    # init_price_from_twse()
    # get_block_trading_from_twse()