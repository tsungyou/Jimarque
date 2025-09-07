import logging
import schedule # tye: ignore
from datetime import datetime
import threading
import time
from tpex_update import get_tpex_peratio, update_tpex_price
from twse_update import get_price_from_twse, get_stock_yield_pe_pb_from_twse, \
    get_block_trading_from_twse

def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

if __name__ == '__main__':
    print(datetime.now())
    get_tpex_peratio()
    update_tpex_price()
    get_price_from_twse()
    get_stock_yield_pe_pb_from_twse()
    get_block_trading_from_twse()
    logging.info('開始排程執行')
    # TW
    weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
    for day in weekdays:
        # price update
        schedule.every().day.at('18:00').do(run_threaded, get_price_from_twse)
        schedule.every().day.at('19:00').do(run_threaded, update_tpex_price)
        
        schedule.every().day.at('19:15').do(run_threaded, get_tpex_peratio)
        schedule.every().day.at('19:30').do(run_threaded, get_stock_yield_pe_pb_from_twse)
        schedule.every().day.at('19:45').do(run_threaded, get_block_trading_from_twse)

    # US, Metatrader5

    try:
        while True:
            schedule.run_pending()

            time.sleep(0.1)
    except KeyboardInterrupt:
        logging.info('排程已手動停止')
    except Exception as e:
        logging.error(f'排程執行時發生錯誤: {e}')
