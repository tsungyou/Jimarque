import logging 
from datetime import datetime
db_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "P910317p",
    "host": "localhost",
    "port": 5433
}

logging_config = {
    "filename": "app.log",
    "filemode": "a", 
    "level": logging.INFO,
    "format": "%(asctime)s - %(levelname)s - %(message)s"
}

base_url = "https://quote-feed.zacks.com/"

def add_logging(funcName):
    logging.basicConfig(**logging_config)
    logging.info(funcName)

def error_logging(funcName):
    logging.basicConfig(**logging_config)
    logging.error(funcName)
