from tqdm import tqdm
import pandas as pd
import MetaTrader5 as mt5 # type: ignore
from datetime import datetime, timedelta
import pytz
import psycopg2 # type: ignore
from config import db_params, add_logging, error_logging
import logging

class MT5Inserter:
    def __init__(self, db_params):
        self.db_params = db_params
        if not mt5.initialize():
            raise Exception("MT5 initialize() failed")

    def fetch_data(self, symbol: str, timeframe, days: int) -> pd.DataFrame:
        rates = mt5.copy_rates_range(
            symbol,
            timeframe,
            datetime.now() - timedelta(days=days),
            datetime.now()
        )

        if rates is None or len(rates) == 0:
            print(f"⚠️ No data returned for {symbol}")
            return pd.DataFrame()

        df = pd.DataFrame(rates)
        df = df.iloc[:, :7]  # drop real_volume if exists
        df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
        df['da'] = df['time'].dt.date
        df['hms'] = df['time'].dt.strftime('%H:%M:%S')
        df.columns = ["time", "op", "hi", "lo", "cl", "tick_volume", "spread", "da", "hms"]
        df['symbol'] = symbol
        df = df.drop_duplicates(subset=['symbol', 'time'])
        return df

    def insert_to_postgres(self, df: pd.DataFrame, table: str):
        if df.empty:
            print("❌ Empty DataFrame, nothing to insert.")
            return

        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()

        insert_sql = f"""
        INSERT INTO {table} (
            symbol, time, op, hi, lo, cl, tick_volume, spread, da, hms
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (symbol, time) DO NOTHING;
        """

        data = df[[
            'symbol', 'time', 'op', 'hi', 'lo', 'cl',
            'tick_volume', 'spread', 'da', 'hms'
        ]].values.tolist()

        try:
            cur.executemany(insert_sql, data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("❌ Error during insert:", e)
        finally:
            cur.close()
            conn.close()

    def get_symbols(self):
        symbols = mt5.symbols_get()
        return [s.name for s in symbols]

    def shutdown(self):
        mt5.shutdown()

def tester():
    db_params = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "P910317p",
        "host": "localhost",
        "port": 5433
    }

    inserter = MT5Inserter(db_params)
    s = inserter.get_symbols()
    for sy in s:
        print(sy)

def runner(init=False):
    current_mand_timeframes = {
        mt5.TIMEFRAME_M1: 69,
        mt5.TIMEFRAME_M5: 300,
        mt5.TIMEFRAME_M15: 1000,
        mt5.TIMEFRAME_D1: 1200
    }

    tf_tables = {
        mt5.TIMEFRAME_M1: "price_mt5_m1",
        mt5.TIMEFRAME_M5: "price_mt5_m5",
        mt5.TIMEFRAME_M15: "price_mt5_m15",
        mt5.TIMEFRAME_D1: "price_mt5_d1"
    }

    update_mand_timeframes = {
        mt5.TIMEFRAME_M1: 5,
        mt5.TIMEFRAME_M5: 5,
        mt5.TIMEFRAME_M15: 5,
        mt5.TIMEFRAME_D1: 5
    }
    inserter = MT5Inserter(db_params)
    s = inserter.get_symbols()
    if init:
        used_dict = current_mand_timeframes
    else:
        used_dict = update_mand_timeframes
    for symbol in tqdm(s):
        for tf, count in used_dict.items():
            df = inserter.fetch_data(symbol, tf, count)
            inserter.insert_to_postgres(df, tf_tables[tf])
    inserter.shutdown()

def init_metatrader5_data():
    try:
        add_logging("running init_metatrader5_data")
        init=True
        runner(init=init)
    except Exception as e:
        error_logging(f"init_metatrader5_data {e}")

def get_metatrader5_data():
    try:
        add_logging("running get_metatrader5_data")
        init=False
        runner(init=init)
    except Exception as e:
        error_logging(f"get_metatrader5_data {e}")

if __name__ == "__main__":
    # get_metatrader5_data()
    tester()