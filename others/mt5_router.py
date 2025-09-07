# pip install fastapi uvicorn psycopg2-binary python-dotenv
import os
from typing import List
from datetime import datetime, timezone
from flask import Flask, request, send_file, abort, render_template_string, send_from_directory

import psycopg2
from psycopg2.extras import execute_values


db_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "P910317p",
    "host": "localhost",
    "port": 5433
}

app = Flask(__name__)
app.run(host="0.0.0.0", port=8000)

def get_conn():
    return psycopg2.connect(db_params)

@app.post("/insert_daily")
def ingest_rates(payload: str):
    print(payload)
    # sql = """
    # INSERT INTO mt5_rates
    #   (symbol, timeframe, ts, open, high, low, close, tick_volume, spread, real_volume)
    # VALUES %s
    # ON CONFLICT (symbol, timeframe, ts) DO UPDATE
    #   SET open=EXCLUDED.open,
    #       high=EXCLUDED.high,
    #       low =EXCLUDED.low,
    #       close=EXCLUDED.close,
    #       tick_volume=EXCLUDED.tick_volume,
    #       spread=EXCLUDED.spread,
    #       real_volume=EXCLUDED.real_volume;
    # """

    # with get_conn() as conn, conn.cursor() as cur:
    #     execute_values(cur, sql, rows)
    # return {"inserted": len(rows)}
    return {"inserted": "123"}
