# File for initial Historical Daily Price Data for stocks in database
# Will also add only new data after last stored date if existing price data.
#
# Tweaked to be able to have multiple servers running command without overlap.
# Use Instance_Count and Instance_Num to utilize

from tda import auth, client
from copy import copy
from numpy import float16
from tqdm import tqdm
#import sqlite3
#import pandas
#import csv
import psycopg2
import psycopg2.extras
import settings.config as conf
import utilities.error as err
import utilities.dbutils as dbu
import alpaca_trade_api as aapi
import json
from os.path import exists
from datetime import datetime, timezone, timedelta
import datetime as d
import time
import linecache
import sys

# Initial Variables
instance_num = 1
instance_count = 2
new_stock_count = 0
updated_stock_count = 0
dbasset_num = 0
query_time = 0
i = 0
on = 1
yskipcount = 0
fskipcount = 0
badcount = 0
stock_symbol = ""


def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    # Errors to logfile
    with open(conf.IssuesFile, 'a+') as f:
        msg = 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(
            filename, lineno, line.strip(), exc_obj)
        f.write(msg)
        f.write('\n')
    AddToProblemFile()
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(
        filename, lineno, line.strip(), exc_obj))

# Symbols that are trouble


def AddToProblemFile():
    with open(conf.ProblemFile, 'a+') as problemfile:
        problemfile.write(stock_symbol)
        problemfile.write('\n')

# Rounding that might be error prone and need removed*********


def proper_round(num, dec=0):
    num = str(num)[:str(num).index('.')+dec+2]
    if num[-1] >= '5':
        return float(num[:-2-(not dec)]+str(int(num[-2-(not dec)])+1))
    return float(num[:-1])

# Self Explanatory


def current_milli_time():
    return round(time.time() * 1000)

# Declare class to store JSON data into a python dictionary


class read_data(object):
    def __init__(self, jdata):
        self.__dict__ = json.loads(jdata)

# ***********************************


# Variables
today = d.datetime.now()
startmilli = (current_milli_time()/1000)
prog_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(startmilli))
today_date = time.strftime('%Y-%m-%d', time.localtime(startmilli))

print(f"Adding Historical Daily Stock Prices started at: {prog_start}")

# Open the local stock db
# sconn = sqlite3.connect(conf.db_file)
# sconn.row_factory = sqlite3.Row
# scur = sconn.cursor()

# Establish Postgresql connection based on settings in config.py
dbconn = psycopg2.connect(
    dbname=conf.dbname,
    user=conf.dbuser,
    password=conf.dbpass,
    host=conf.dbhost,
    port=conf.dbport,
    sslmode=conf.dbsslmode
)

# Check for log files
file_exists = exists(conf.ProblemFile)
if not file_exists:
    f = open(conf.ProblemFile, 'w')
    f.close()
file_exists = exists(conf.IssuesFile)
if not file_exists:
    f = open(conf.IssuesFile, 'w')
    f.close()
file_exists = exists(conf.ErrorsFile)
if not file_exists:
    f = open(conf.ErrorsFile, 'w')
    f.close()

# Get token from TDA
try:
    c = auth.client_from_token_file(conf.tda_token_path, conf.tda_apikey)
except FileNotFoundError:
    # Need to modify for no X Windows server side.  This part should only update from a linux desktop
    # meaning this whould send an alert.  perhaps get ahead of this expiring with time alert.
    from selenium import webdriver
    with webdriver.Chrome() as driver:
        c = auth.client_from_login_flow(
            driver, conf.tda_apikey, conf.tda_redirect_uri, conf.tda_token_path)
except Exception:
    # If the file is expired?
    print(e)

# With Connection to Database active, run main set of commands
with dbconn:
    dbcur = dbconn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Grab current symbols in Stock Table from database
    try:
        dbcur.execute(
            "SELECT id, symbol, name, table_daily, table_daily_milli, status FROM stocks")
        rows = dbcur.fetchall()
        stored_symbols = [row['symbol'] for row in rows]
        stock_total = len(stored_symbols)
    except Exception as e:
        badcount += 1
        PrintException()
        exit()

    # initializing list to hold query times
    qtime_list = []
    lct = round(120/instance_count)
    for value in range(0, lct):
        qtime_list.append(0)

    milli_1yr = startmilli - 35000000
    milli_30yr = startmilli - 1000000000

    for chunk in range (0, 2):
        stock_num = 0
        
        # Read all Problem Symbols into a variable to avoid
        with open(conf.ProblemFile, 'r') as f:
            bad_symbols = f.read().split("\n")

        # Cycle through all symbols grabbed from DB
        for cur_id in rows:
            stock_num += 1

            # added this when I split the initial download to multiple systems
            # need to add a method to divide past 2 systems downloading....  **********
            if stock_num >= stock_total/instance_count:
                continue
            starttime_dbasset = (current_milli_time()/1000)
            dbasset_starttime = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime(starttime_dbasset))

            stock_symbol = cur_id[1]
            if stock_symbol in bad_symbols:
                if chunk == 0:
                    badcount += 1
                print(f"{stock_num}/{stock_total} - {chunk} - Avoiding {stock_symbol} because it has had problems before and is blacklisted.")
                continue

            stock_status = cur_id[5]
            if stock_status == 'inactive':
                continue

            daily_milli = cur_id[4]
            # (If daily milli is blank, set it to something)
            if not daily_milli:
                daily_milli = milli_1yr

            daily_updated = time.strftime('%Y-%m-%d', time.localtime(daily_milli))
            daily_state = cur_id[3]
            if chunk == 0:
                if ((daily_updated == today_date) and (daily_state == 1)):
                    yskipcount += 1
                    print(f"Current Year Update: {stock_num}/{stock_total} - Skipping {stock_symbol} because it seems to have already been updated today.")
                    continue
            else:
                if ((daily_updated == today_date) and (daily_state == 2)):
                    fskipcount += 1
                    print(f"Full Update: {stock_num}/{stock_total} - Skipping {stock_symbol} because it seems to have already been updated today.")
                    continue

            id = cur_id[0]
            stock_name = cur_id[2]
            dbasset_num += 1

            try:
                dbcur.execute("UPDATE stocks SET table_daily = %s where id = %s;", (chunk, id))  
            except Exception as e:
                badcount += 1
                PrintException()
                continue

            #     Speed Enforcement Portion based on number of instances  ####################################################
            lct = round(120/instance_count) - 1
            thresh = (60 * instance_count) + 6
            i += 1
            query_time = (current_milli_time()/1000)
            chunkXtime = query_time - qtime_list[0]
            chunk1time = query_time - qtime_list[lct]
            qtime_list.pop(0)
            qtime_list.append(query_time)
            print(
                f"  +++++ Time to perform last {lct + 1} TDA queries: {chunkXtime} seconds")
            if ((chunkXtime < thresh) and (i > 120)):
                sleeptime = ((thresh + 1) - chunkXtime)
                print(
                    f"{i} - {chunkXtime}:                  {lct + 1} Marker --------->  Sleeping for {sleeptime} seconds.")
                time.sleep(sleeptime)
            if ((chunk1time < .55) and (i > 2)):
                sleeptime = (.6 - chunk1time)
                print(
                    f"{i} - {chunk1time}:                    1 Marker --------->  Sleeping for {sleeptime} seconds.")
                time.sleep(sleeptime)
            ##################################################################################################################

    #        cur.execute("SELECT datetime FROM prices_daily where stock_id = %s;", (id,))
    #        price_rows = cur.fetchall()
    #        price_count = len(price_rows)

            print(f"{stock_num}/{stock_total} -  {chunk} -Retrieving New Daily Stock Prices for {stock_symbol}.  Starting at {dbasset_starttime}")
            dbcur.execute("SELECT COUNT(datetime) FROM prices_daily where stock_id = %s and hist_source = 0;", (id,))
            result = dbcur.fetchall()
            price_count_live = result[0][0]
            if price_count_live > 0:
                dbcur.execute("SELECT MIN(datetime) AS minimum FROM prices_daily where stock_id = %s and hist_source = 0;", (id,))
                result = dbcur.fetchall()
                price_mindate_live = result[0][0]
                dbcur.execute("SELECT MAX(datetime) AS maximum FROM prices_daily where stock_id = %s and hist_source = 0;", (id,))
                result = dbcur.fetchall()
                price_maxdate_live = result[0][0]
                print(f"Most recent Live Daily Price data found in stockbot database: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(price_maxdate_live))}   {price_count} total price records")
                print(f"Oldest Live Daily Price data found in stockbot database: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(price_mindate_live))}")
                try:
                    dbcur.execute("DELETE FROM prices_daily where stock_id = %s and hist_source = 0 and datetime BETWEEN %s and %s;", (id, price_mindate_live, price_maxdate_live))
                    print(f"   ->>>>  All live data removed.")
                except Exception as e:
                    badcount += 1
                    PrintException()
            dbcur.execute("SELECT COUNT(datetime) FROM prices_daily where stock_id = %s;", (id,))
            result = dbcur.fetchall()
            price_count = result[0][0]
            if price_count > 0:
                dbcur.execute("SELECT MAX(datetime) AS maximum FROM prices_daily where stock_id = %s and hist_source = 1;", (id,))
                result = dbcur.fetchall()
                price_maxdate_hist = result[0][0]
                dbcur.execute("SELECT MIN(datetime) AS minimum FROM prices_daily where stock_id = %s and hist_source = 1;", (id,))
                result = dbcur.fetchall()
                price_mindate_hist = result[0][0]
                print(f"Most recent Historical Daily Price data found in stockbot database: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(price_maxdate_hist))}   {price_count} total price records")
                print(f"Oldest Historical Daily Price data found in stockbot database: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(price_mindate_hist))}")
                if chunk == 0:
                    srewind = round((query_time - price_maxdate_hist)/86400)
                    erewind = 0
                else:
                    srewind = round((query_time - milli_30yr)/86400)
                    erewind = round((query_time - price_mindate_hist)/86400)
            else:
                print(f"No Daily Price data found in stockbot database")
                srewind = round((query_time - milli_1yr)/86400)
                erewind = 0
            hist_start = datetime.now(timezone.utc) - timedelta(days=srewind)
            hist_end = datetime.now(timezone.utc) - timedelta(days=erewind)
            print(f"Pulling data from {hist_start} to {hist_end}")

            ###################################################
            # Last X years of daily price data for stock  #
            ###################################################
            try:
                price_data = c.get_price_history_every_day(stock_symbol, start_datetime=hist_start, end_datetime=hist_end)
                assert price_data.status_code == 200, price_data.raise_for_status()
                # print(json.dumps(price_data.json(), indent=4))
            except Exception as e:
                badcount += 1
                PrintException()
                continue

            prices = price_data.json()
            if json.dumps(price_data.json(), indent=4) == '{}':
                print(
                    f"Couldn't find any new price data for symbol: {stock_symbol}    Adding to Problem File for later review.")
                AddToProblemFile()
                continue
            p1 = 'candles'
            p2 = prices[p1]
            # print(f">>> Error checking to see if price data variable is empty: {prices['empty']}")
            if (prices['empty'] == True):
                print(f"No new data for stock {stock_symbol}")
                continue
            print(
                f"- Number of new daily price records found for {stock_symbol} from TDA: {len(p2)-1}")
            fromdate = time.localtime((prices['candles'][0]['datetime'])/1000)
            todate = time.localtime(
                (prices['candles'][(len(prices['candles'])-1)]['datetime'])/1000)
            print(
                f"- TDA Price Data Date Range from: {time.strftime('%Y-%m-%d %H:%M:%S', fromdate)} to {time.strftime('%Y-%m-%d %H:%M:%S', todate)}")

            bar_skip = 0
            bar_update = 0
            bar_add = 0
            for p5bar in tqdm(range(0, (len(prices['candles']))), ncols=100, ascii=True, desc=stock_symbol):
                barX = (p2[p5bar])
                tname = 'prices_daily'
                dbu.DBAddNewBar(tname, id, stock_symbol, bar_update, bar_add, badcount, **barX)

            print(
                f"Bars skipped: {bar_skip}    Updated: {bar_update}    Added: {bar_add}")

            try:
                dbcur.execute(
                    "UPDATE stocks SET table_daily = %s, table_daily_milli = %s where id = %s;", (chunk+1, query_time, id))
            except Exception as e:
                badcount += 1
                PrintException()
                continue

            # Commit database changes
            dbconn.commit()

            endtime_dbasset = (current_milli_time()/1000)
            dbasset_endtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endtime_dbasset))
            answer = str(round((endtime_dbasset - starttime_dbasset), 2))
            print(f"{dbasset_endtime} - Elapsed Time for {stock_symbol} Daily Price Update: {answer} seconds")
            print(f"\n")



# Commit changes to DB
dbconn.commit()

endmilli = (current_milli_time()/1000)
prog_end = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endmilli))
print(f"Add Stocks ended at: {prog_end}")
print(
    f"Total elapsed time to add stocks: {proper_round((endmilli - startmilli),1)} seconds")
print(
    f"Last Year - Skipped: {yskipcount}   New: {new_stock_count}   Updated: {updated_stock_count}")
print(
    f"Full Update - Skipped: {fskipcount}   New: {new_stock_count}   Updated: {updated_stock_count}")
with open(conf.ProblemFile, 'r') as f:
    bad_symbols = f.read().split("\n")
    print(f"Blackedlisted: {badcount}   ({bad_symbols})")
