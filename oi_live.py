import http.client
import json
import pandas
import pandas as pd
from time import sleep
from nsetools import Nse
import json, math, sys
import datetime as dt
from datetime import datetime, timedelta
import requests
import time
import os
import shutil
import logging
import glob
import schedule
import threading
import yfinance as yf
# import locale
from telegram import *
from telegram.ext import *
import matplotlib.pyplot as plt
# from libs import raju_index_lib as my_lib
from my_libs import raju_index_lib as my_lib
from conf import config_kite
from conf import config_icici
from conf import config_live
from utils import gen_utils as my_utils
from kite_trade import *
import traceback

# locale.setlocale(locale.LC_ALL, 'en_IN.UTF-8')

today = dt.date.today()
trading_day = None
# Check if today is a trading day
if today.weekday() in [5, 6]: # If today is Saturday or Sunday
    days_to_subtract = today.weekday() - 4 # Subtract 4 or 5 days to get the previous Friday
    trading_day = today - dt.timedelta(days=days_to_subtract)
else:
    trading_day = today

# print("The previous trading day was:", previous_trading_day)

# today_str = dt.datetime.today().strftime('%Y-%m-%d')
today_str = trading_day.strftime('%Y-%m-%d')
# today_str = '2023-03-24' # RAJU, comment this for live-trading day
if config_live.today_str != '':
     today_str = config_live.today_str

use_kite = config_live.use_kite
use_both = config_live.use_both
symbol = config_live.symbol
# expiry_str = config_kite.exp_str #"23309" # 3 - means MAR month
expiry_str = my_utils.get_kite_exp_str(today_str)

prev_signal_time  = ''
max_retries = 3
retry_interval = 5
tkrs = ['^nsebank']
each_strike = 100
atm_strk = 0
if symbol != 'CNXBAN':
    each_strike = 50
    tkrs = ['^nsei']

# API_KEY = "5460434526:AAEZ3YIaPSER_dSqt-CbNQVjdiIgJ7edLC4" #bnf823bot


print(f"{dt.datetime.now()}\tstarting ...")
pid = os.getpid()
# print the process ID
print("\tProcess ID:", pid)
with open('logs/oi_insights_pid.txt', 'w') as f:
                f.write(f'"Process ID:", {pid}\n')
                f.flush()

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# formatter = logging.Formatter(
#         '%(asctime)s.%(msecs)03d %(process)d::%(thread)d %(levelname)s %(filename)s:%(lineno)d [%(module)s]:[%(funcName)s] %(message)s')

logging.basicConfig(filename = 'logs/trending_analysis.log', 
                    # format='%(asctime)s.%(msecs)03d %(process)d::%(thread)d %(levelname)s {%(module)s} [%(funcName)s] %(message)s', 
                    format='%(asctime)s.%(msecs)03d %(process)d::%(thread)d %(levelname)s %(filename)s:%(lineno)d {%(module)s} [%(funcName)s] %(message)s', 
                    # format = formatter, 
                    datefmt = '%Y-%m-%d %H:%M:%S', level=logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("PngImagePlugin").setLevel(logging.WARNING)
logging.getLogger("font_manager").setLevel(logging.WARNING)

logging.info(f"{dt.datetime.now()}-- starting ...")

raju_account = '{"appKey" : "i87W5a354o436C172$1954914Uj1s54m","apiSecret" : "5B2M47139O29953437a)O37`91h6f3l2"}'
swaran_account = '{"appKey" : "F005#727V890J99965G632z9h9285q23","apiSecret" : "z26A3(w9C635&2F89402L24k420b6209"}'
moses_account = '{"appKey" : "396C!u2213c1a3z2x743467h*G8^y&+7","apiSecret" : "77911&198C6!86bt72Io8tid27222789"}'

# inv_account = f'{"my_userId":{config_icici.my_userId}, "my_passwd" : {config_icici.my_passwd}, "my_dob" : {config_icici.my_dob},"appKey" : {config_icici.appKey},"apiSecret" :{config_icici.apiSecret}}'
login_dtls_dict = {"appKey" : config_icici.appKey,"apiSecret" :config_icici.apiSecret}
inv_account = json.dumps(login_dtls_dict)

#use_account = json.loads(raju_account)
#session_key = config_icici.session_key_raju

use_account = json.loads(swaran_account)
session_key = config_icici.session_key_swaran

#use_account = json.loads(moses_account)
#session_key = config_icici.session_key_moses

# use_account = json.loads(inv_account)
# session_key = config_icici.session_key_inv

kite = None
isec = None
# broker_obj = None
broker_obj = []

if use_both:
    # isec = my_lib.breeze_login( use_account['my_userId'], use_account['my_passwd'], use_account['my_dob'], use_account['appKey'], use_account['apiSecret'] )
    isec = my_lib.breeze_login_new(use_account['appKey'], use_account['apiSecret'], session_key)
    broker_obj.append(isec)
    if use_kite:
        # Use this below for creating Kite object with hack-code
        kite = my_lib.get_kite_trade_obj() 

        # Use this below for creating Kite object with official API 
        # kite = my_lib.get_kite_api_obj()

        broker_obj.append(kite)
        print(f"\tData Providers :: ICICI-BREEZE && KITE-ZERODHA")
    else:
        broker_obj.append(None) 
        print(f"\tData Provider :: ICICI-BREEZE")

else:
    if use_kite:
        # print(f"Using Kite as the Data provider")
        print(f"\tData Provider :: KITE-ZERODHA")
        kite = my_lib.get_kite_trade_obj()
        # kite = my_lib.get_kite_trade_obj()
        # broker_obj.append('empty-breeze')
        broker_obj.append(None)
        # broker_obj = kite
        broker_obj.append(kite)
    else :
        # print(f"Using ICICI-direct as the Data provider")
        print(f"\tData Provider :: ICICI-BREEZE")
        # isec = my_lib.breeze_login( use_account['my_userId'], use_account['my_passwd'], use_account['my_dob'], use_account['appKey'], use_account['apiSecret'] )     
        isec = my_lib.breeze_login_new(use_account['appKey'], use_account['apiSecret'], session_key)
        # broker_obj = isec
        broker_obj.append(isec)
        # broker_obj.append('empty-kite')
        broker_obj.append(None)


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

date_str = today_str
today_date = datetime.strptime(date_str, '%Y-%m-%d')
# print(f"processing for Date = {today_date}")
print(f"***************************\n\t processing for the date:{today_str}\n***************************")
# wk_exp = my_lib.find_nse_bn_weekly_expiry_date(today_date)
# print(f"\t==>weekly-expiry = {wk_exp}")
# currExp_str = wk_exp.strftime('%Y-%m-%d')

wk_exp_str = my_utils.get_breeze_exp_str(date_str)
print(f"\t==>wk_exp_str={wk_exp_str}")
currExp_str = wk_exp_str

month_exp = my_lib.find_monthly_expiry(today_date)
print(f"\t==>Monthly-expiry = {month_exp}")


start_date = datetime.strptime(date_str, '%Y-%m-%d')
next_date_str = (start_date + timedelta(days=1)).strftime('%Y-%m-%d')

data = pd.DataFrame()
# data = yf.download(tickers = tkrs, start= date_str, end = next_date_str, period= '1d', interval= '1d')

# atm_strk = 39800
if (len(data) == 1):
    day_open = int(data.iloc[0]['Open'])
    atm_strk = int(math.floor(float(day_open)/each_strike)*each_strike)


monthExp_str = month_exp.strftime('%Y-%m-%d')
now = datetime.now()
next_min = now + timedelta(minutes=1)
times = '{:02}'.format(int(next_min.hour)) + ":" + '{:02}'.format(int(next_min.minute ))
interval_str = '1minute'

def run_threaded_new(job_func, args):
    job_thread = threading.Thread(target=job_func, args=(args))
    job_thread.start()

def schedule_every_5_min__(job_func, args):
    # global kite
    global broker_obj
    global use_kite
    logging.info(f"{dt.datetime.now()} calling schedule_every_5_min__ for the 1st time ...")
    
    args_list = list(args)

    now = dt.datetime.now()
    current_weekday = dt.datetime.now().weekday()
   
    if ((current_weekday <= 4) and ((now.hour < 9) or (now.hour == 9 and now.minute <= 17))):
    # if False:
        logging.info(f"Skipping now, will execute in live mkt, from 09:18" )
    else:
        # Check if the input has valid atm_strk, if not get from mkt and set it
        if args_list[4] == 0:
        # if True:
            logging.info(f"setting the atm_strk")
            data = yf.download(tickers = tkrs, start= date_str, end = next_date_str, period= '1d', interval= '1d')
            if (len(data) == 1):
                day_open = int(data.iloc[0]['Open'])
                atm_strk = int(math.floor(float(day_open)/each_strike)*each_strike)
                logging.info(f"set atm_strk={atm_strk}")
                args_list[4] = atm_strk
                logging.info(f"\tafter setting, atm_strk={args_list[4]}")
                args = tuple(args_list)
                logging.info(f"Successfully set the args {args}")

        kite = my_lib.build_trending_oi (broker_obj, symbol, date_str, currExp_str, atm_strk, expiry_str, interval_str, is_kite=use_kite)

    # min_interval = 5
    min_interval = 1

    schedule.every(min_interval).minutes.do(run_threaded_new, job_func, args)
    # return schedule.CancelJob
    # if use_kite:
    #     # set the expiry string based on the kite's config
    #     currExp_str = expiry_str

params = (broker_obj, symbol, date_str, currExp_str, atm_strk, expiry_str, interval_str, use_kite)

now = dt.datetime.now()
current_weekday = dt.datetime.now().weekday()

if (current_weekday > 4):
    schedule.every().day.at(times).do(schedule_every_5_min__, my_lib.build_trending_oi, params)
elif (now.hour < 9) or (now.hour == 9 and now.minute <= 17):
# if False:
    logging.info(f"Skipping now, will execute in live mkt, from 09:18" )    
    schedule.every().day.at("09:18").do(schedule_every_5_min__, my_lib.build_trending_oi, params)
else:
    schedule.every().day.at(times).do(schedule_every_5_min__, my_lib.build_trending_oi, params)

while True:
    try:
        schedule.run_pending()
        sleep(10) 
    except Exception as e:
        print(f"exception in polling {e}")
        logging.error(f"\t{dt.datetime.now()} exception in polling {e}")
        sleep(5)
        continue
    except (KeyboardInterrupt, SystemExit) as e:
        # traceback.print_exc() # it prints the exception to the console
        logging.fatal(f"received exception, quitting ...")
        logging.exception("Exception occurred")
        quit()
        # pass

print(f"{dt.datetime.now()}-- end ...")
logging.info(f"{dt.datetime.now()}-- end ...")
quit()