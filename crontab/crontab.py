import logging
import schedule # tye: ignore
from datetime import datetime
import threading
import time
from zacks_rank_data import get_zacks_data
# from tw_breakouts import get_breakouts
# from rynek.v1.us_strategies import us_routine
def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

if __name__ == '__main__':
    print(datetime.now())
    get_zacks_data()   
    logging.info('開始排程執行')
    # TW
    weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
    for day in weekdays:
        pass

    # US, Metatrader5
    weekdays_us = ['tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
    for day in weekdays:
        getattr(schedule.every(), day).at('09:30').do(run_threaded, get_zacks_data)

    try:
        while True:
            schedule.run_pending()

            time.sleep(0.1)
    except KeyboardInterrupt:
        logging.info('排程已手動停止')
    except Exception as e:
        logging.error(f'排程執行時發生錯誤: {e}')
