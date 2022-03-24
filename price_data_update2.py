# File for initial Historical Daily Price Data for stocks in database
# Will also add only new data after last stored date if existing price data.
#
# Tweaked to be able to have multiple servers running command without overlap.
# Use Instance_Count and Instance_Num to utilize

from tda import auth, client
from copy import copy
from numpy import float16
from tqdm import tqdm
import psycopg2
import psycopg2.extras
import settings.config as conf
import utilities.error as err
import utilities.base as b
import utilities.tda_actions as td
import utilities.dbutils as dbu
import alpaca_trade_api as aapi
import json
from pushbullet import Pushbullet
from os.path import exists
from datetime import datetime, timezone, timedelta
import datetime as d
import time
import linecache
import sys

# Initial Variables
tdaquery = "1day"
tname = 'prices_daily'
tokenfile = ""
instance_num = 2
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
stock_symbol = "None"
today = d.datetime.now()
startmilli = (b.current_milli_time()/1000)
prog_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(startmilli))
today_date = time.strftime('%Y-%m-%d', time.localtime(startmilli))

print(f"Adding Historical Daily Stock Prices started at: {prog_start}")

# Establish Postgresql connection based on settings in config.py
dbconn = psycopg2.connect(
    dbname=conf.dbname,
    user=conf.dbuser,
    password=conf.dbpass,
    host=conf.dbhost,
    port=conf.dbport,
    sslmode=conf.dbsslmode
)

# # Check for log files
b.CheckFileMake(conf.ProblemFile)
b.CheckFileMake(conf.IssuesFile)
b.CheckFileMake(conf.ErrorsFile)

# # Get token from TDA
try:
    c = auth.client_from_token_file(conf.tda_token_path, conf.tda_apikey)
except FileNotFoundError:
    tokenfile = "invalid"
except Exception as e:
    # If the file is expired?
    print(e)

if tokenfile == "invalid":
    file_exists = exists('/snap/bin/chromium')
    if file_exists:
        try:
            from selenium import webdriver
            with webdriver.Chrome() as driver:
                c = auth.client_from_login_flow(
                    driver, conf.tda_apikey, conf.tda_redirect_uri, conf.tda_token_path)
        except Exception as e:
            print(e)
    else:
        # Send out a notification of need to refresh token.
        pb = Pushbullet(conf.pbapi)
        push = pb.push_note('TD Ameritrade Token Failure', 'You must refresh token from terminal with Chromium webdriver.  Stockbot will be halted until completed.')

# With Connection to Database active, run main set of commands
with dbconn:
    dbcur = dbconn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # initializing list to hold query times
    qtime_list = []
    lct = round(120/instance_count)
    for value in range(0, lct):
        qtime_list.append(0)

    milli_1d = startmilli - 86400
    milli_45d = startmilli - 3900000
    milli_1mo = startmilli - 2666666
    milli_9mo = startmilli - 16000000
    milli_1yr = startmilli - 32000000
    milli_30yr = startmilli - 946000000

    # searches = ['daily', '1min', '5min', '15min', '30min', '1hr', '2hr', '4hr']
    searches = ['daily', '1min', '5min', '15min', '30min']
    searchtbl = ['prices_daily', 'prices_1min', 'prices_5min', 'prices_15min', 'prices_30min', 'prices_1hr', 'prices_2hr', 'prices_4hr']
    searcht = ['table_daily', 'table_1min', 'table_5min', 'table_15min', 'table_30min', 'table_1hr', 'table_2hr', 'table_4hr']
    searchtm = ['table_daily_milli', 'table_1min_milli', 'table_5min_milli', 'table_15min_milli', 'table_30min_milli', 'table_1hr_milli', 'table_2hr_milli', 'table_4r_milli']
    searchs1 = [milli_1yr, milli_1d, milli_1mo, milli_1mo, milli_1mo]
    searchs2 = [milli_30yr, milli_45d, milli_9mo, milli_9mo, milli_9mo]
    # freq = ['DAILY', 'EVERY_MINUTE', 'EVERY_FIVE_MINUTES', 'EVERY_FIFTEEN_MINUTES', 'EVERY_THIRTY_MINUTES']
    # freqtype = ['DAILY', 'MINUTE', 'MINUTE', 'MINUTE', 'MINUTE']

    for chunk in range (0, 2):
        d = -1
        for search in searches:
            d += 1
            stock_num = 0

            # Grab current symbols in Stock Table from database
            try:
                dbcur.execute(
                    "SELECT id, symbol, name, {}, {}, status FROM stocks;".format(searcht[d], searchtm[d]))
                rows = dbcur.fetchall()
                stored_symbols = [row['symbol'] for row in rows]
                stock_total = len(stored_symbols)
            except Exception as e:
                badcount += 1
                err.PrintException(stock_symbol, "Setup")
                exit()

            # Read all Problem Symbols into a variable to avoid
            with open(conf.ProblemFile, 'r') as f:
                bad_symbols = f.read().split("\n")

            # Cycle through all symbols grabbed from DB
            for cur_id in rows:
                stock_num += 1

                # added this when I split the initial download to multiple systems
                # need to add a method to divide past 2 systems downloading....  **********
                if stock_num < stock_total/instance_count:
                    continue
                starttime_dbasset = (b.current_milli_time()/1000)
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

                period_milli = cur_id[4]
                # (If daily milli is blank, set it to something)
                if not period_milli:
                    period_milli = searchs1[d]

                period_updated = time.strftime('%Y-%m-%d', time.localtime(period_milli))
                period_state = cur_id[3]
                if chunk == 0:
                    if ((period_updated == today_date) and (period_state == 1)):
                        yskipcount += 1
                        print(f"Current limited {searches[d]} update: {stock_num}/{stock_total} - Skipping {stock_symbol} because it seems to have already been updated today.")
                        continue
                else:
                    if ((period_updated == today_date) and (period_state == 2)):
                        fskipcount += 1
                        print(f"Full {searches[d]} update: {stock_num}/{stock_total} - Skipping {stock_symbol} because it seems to have already been updated today.")
                        continue

                id = cur_id[0]
                stock_name = cur_id[2]
                dbasset_num += 1

                try:
                    dbcur.execute("UPDATE stocks SET {} = %s where id = %s;".format(searcht[d]), (chunk, id))  
                except Exception as e:
                    badcount += 1
                    err.PrintException(stock_symbol, "Setup")
                    continue

                #     Speed Enforcement Portion based on number of instances  ####################################################
                lct = round(120/instance_count) - 1
                thresh = (60 * instance_count) + 6
                i += 1
                query_time = (b.current_milli_time()/1000)
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

        #        cur.execute("SELECT datetime FROM {} where stock_id = %s;", (id,))
        #        price_rows = cur.fetchall()
        #        price_count = len(price_rows)

                print(f"{stock_num}/{stock_total} -  {chunk} -Retrieving New {searches[d]} Stock Prices for {stock_symbol}.  Starting at {dbasset_starttime}")
                dbcur.execute("SELECT COUNT(datetime) FROM {} where stock_id = %s and hist_source = 0;".format(searchtbl[d]), (id,))
                result = dbcur.fetchall()
                price_count_live = result[0][0]
                if price_count_live > 0:
                    dbcur.execute("SELECT MIN(datetime) AS minimum FROM {} where stock_id = %s and hist_source = 0;".format(searchtbl[d]), (id,))
                    result = dbcur.fetchall()
                    price_mindate_live = result[0][0]
                    dbcur.execute("SELECT MAX(datetime) AS maximum FROM {} where stock_id = %s and hist_source = 0;".format(searchtbl[d]), (id,))
                    result = dbcur.fetchall()
                    price_maxdate_live = result[0][0]
                    print(f"Most recent Live {searches[d]} Price data found in stockbot database: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(price_maxdate_live))}   {price_count} total price records")
                    print(f"Oldest Live {searches[d]} Price data found in stockbot database: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(price_mindate_live))}")
                    try:
                        dbcur.execute("DELETE FROM {} where stock_id = %s and hist_source = 0 and datetime BETWEEN %s and %s;".format(searchtbl[d]), (id, price_mindate_live, price_maxdate_live))
                        print(f"   ->>>>  All live data removed.")
                    except Exception as e:
                        badcount += 1
                        err.PrintException(stock_symbol, "Setup")
                dbcur.execute("SELECT COUNT(datetime) FROM {} where stock_id = %s;".format(searchtbl[d]), (id,))
                result = dbcur.fetchall()
                price_count = result[0][0]
                if price_count > 0:
                    dbcur.execute("SELECT MAX(datetime) AS maximum FROM {} where stock_id = %s and hist_source = 1;".format(searchtbl[d]), (id,))
                    result = dbcur.fetchall()
                    price_maxdate_hist = result[0][0]
                    dbcur.execute("SELECT MIN(datetime) AS minimum FROM {} where stock_id = %s and hist_source = 1;".format(searchtbl[d]), (id,))
                    result = dbcur.fetchall()
                    price_mindate_hist = result[0][0]
                    print(f"Most recent Historical {searches[d]} Price data found in stockbot database: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(price_maxdate_hist))}   {price_count} total price records")
                    print(f"Oldest Historical {searches[d]} Price data found in stockbot database: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(price_mindate_hist))}")

    # **************************************
                    if chunk == 0:
                        srewind = round((query_time - price_maxdate_hist)/86400) 
                        erewind = 0
                    else:
                        srewind = round((query_time - searchs2[d])/86400)
                        erewind = round((query_time - price_mindate_hist)/86400)
                else:
                    print(f"No {searches[d]} price data found in stockbot database")
                    srewind = round((query_time - searchs1[d])/86400)
                    erewind = 0
                hist_start = datetime.now(timezone.utc) - timedelta(days=srewind)
                hist_end = datetime.now(timezone.utc) - timedelta(days=erewind)
                print(f"Pulling data from {hist_start} to {hist_end}")

                ###################################################
                # Last X time period of price data for stock  #
                ###################################################
                prices = td.QueryTDA(c, search, stock_symbol, hist_start, hist_end, badcount)

                p1 = 'candles'
                p2 = prices[p1]
                if (prices['empty'] == True):
                    print(f"No new data for stock {stock_symbol}")
                    continue
                print(
                    f"- Number of new {searches[d]} price records found for {stock_symbol} from TDA: {len(p2)-1}")
                fromdate = time.localtime((prices['candles'][0]['datetime'])/1000)
                todate = time.localtime(
                    (prices['candles'][(len(prices['candles'])-1)]['datetime'])/1000)
                print(
                    f"- TDA {searches[d]} Price Data Date Range from: {time.strftime('%Y-%m-%d %H:%M:%S', fromdate)} to {time.strftime('%Y-%m-%d %H:%M:%S', todate)}")

                bar_skip = 0
                bar_update = 0
                bar_add = 0
                for p5bar in tqdm(range(0, (len(prices['candles']))), ncols=100, ascii=True, desc=stock_symbol):
                    barX = (p2[p5bar])
                    dbu.DBAddNewBar(dbcur, search, searchtbl[d], id, stock_symbol, bar_update, bar_add, badcount, **barX)

                print(
                    f"Bars skipped: {bar_skip}    Updated: {bar_update}    Added: {bar_add}")

                try:
                    # table_daily_milli reference?  ************************************************  !!!!!!!!!!!!!!!!!!!!!!!
                    dbcur.execute(
                        "UPDATE stocks SET {} = %s, {} = %s where id = %s;".format(searcht[d], searchtm[d]), (chunk+1, query_time, id))
                    # dbcur.execute("UPDATE stocks SET {} = %s where id = %s;".format(searchtm[d]), (query_time, id))
                except Exception as e:
                    badcount += 1
                    err.PrintException(stock_symbol, "Setup")
                    continue

                # Commit database changes
                dbconn.commit()

                endtime_dbasset = (b.current_milli_time()/1000)
                dbasset_endtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endtime_dbasset))
                answer = str(round((endtime_dbasset - starttime_dbasset), 2))
                print(f"{dbasset_endtime} - Elapsed Time for {stock_symbol} {searches[d]} Price Update: {answer} seconds")
                print(f"\n")

# Commit changes to DB

dbconn.commit()

endmilli = (b.current_milli_time()/1000)
prog_end = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endmilli))
print(f"Add Stocks ended at: {prog_end}")
print(
    f"Total elapsed time to add stocks: {b.proper_round((endmilli - startmilli),1)} seconds")
print(
    f"Last Year - Skipped: {yskipcount}   New: {new_stock_count}   Updated: {updated_stock_count}")
print(
    f"Full Update - Skipped: {fskipcount}   New: {new_stock_count}   Updated: {updated_stock_count}")
with open(conf.ProblemFile, 'r') as f:
    bad_symbols = f.read().split("\n")
    print(f"Blackedlisted: {badcount}   ({bad_symbols})")
