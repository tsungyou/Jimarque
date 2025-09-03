from crontab.config_c import db_params
import psycopg2 # type: ignore
import pandas


strategy_list = [
    "XAUUSD_rangeBreakout_6amto9am_21pm_revPos_true",
    "USDJPY_rangeReverse_9amto12am_21pm_revPos_true",
    "GER30_rangeBreakout_15pmto1530pm_21pm_revPos_true",
    "GER30_candleReverse_15pmto1515pm_1517pm_oneOrder",
    "JPN225_rangeBreakout_8amto815am_12am_revPos_true",
    "NAS100_trendFollowing",
    "US30_candleReverse_2130pmto2135pm_2140pm_oneOrder",
]