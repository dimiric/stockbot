from copy import copy
import psycopg2
import psycopg2.extras
import settings.config as conf
import alpaca_trade_api as aapi
import json
from datetime import datetime, timezone, timedelta
import datetime as d
import time

today = d.datetime.now()

def proper_round(num, dec=0):
    num = str(num)[:str(num).index('.')+dec+2]
    if num[-1]>='5':
        return float(num[:-2-(not dec)]+str(int(num[-2-(not dec)])+1))
    return float(num[:-1])

def current_milli_time():
    return round(time.time() * 1000)

startmilli = (current_milli_time()/1000)
prog_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(startmilli))
print(f"Add Stocks started at: {prog_start}")

# Declare class to store JSON data into a python dictionary
class read_data(object):
  def __init__(self, jdata):
    self.__dict__ = json.loads(jdata)

#Establish Postgresql connection based on settings in config.py
conn = psycopg2.connect(
    dbname=conf.dbname,
    user=conf.dbuser,
    password=conf.dbpass,
    host=conf.dbhost,
    port=conf.dbport,
    sslmode=conf.dbsslmode
    )

# Get token from TDA
from tda import auth, client
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

with conn:
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    #Grab current symbols in Stock Table
    cur.execute("SELECT symbol, name, id, table_daily, table_daily_milli FROM stocks")
    rows = cur.fetchall()
    stored_symbols = [row['symbol'] for row in rows]
    
    stock_count = 0
    followups = []

    for row in rows:
        try:
            start_asset = (current_milli_time()/1000)
            today_ago = datetime.now(timezone.utc) - timedelta(days=90)
            asset_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(startmilli))

            stock_count = stock_count + 1
            print(f"{stock_count} out of {len(stored_symbols)} - Adding Historical Price Data: {row['symbol']} {row['name']} ({row['id']}) ({row['table_daily_milli']}- {asset_start}")

            ###################################################
            # Last 20 years of daily price data for stock  #
            ###################################################
            #if (row['table_daily_milli] == 1):
                #today_ago = 
            today_ago = datetime.now(timezone.utc) - timedelta(days=90)
            hist_start = today_ago

            p5 = c.get_price_history_every_day(row['symbol'],
                need_extended_hours_data=True,
                start_datetime=hist_start,
                #end_datetime=hist_end
                )

            p5min = p5.json()
            p1 = 'candles'
            p5 = p5min[p1]
            fromdate = time.localtime((p5min['candles'][0]['datetime'])/1000)
            todate = time.localtime((p5min['candles'][(len(p5min['candles'])-1)]['datetime'])/1000)
            print(f"Date Range from: {time.strftime('%Y-%m-%d %H:%M:%S', fromdate)} to {time.strftime('%Y-%m-%d %H:%M:%S', todate)}")
            print(f"- Number of daily price records found for {row['symbol']}: {len(p5)}")

            for p5bar in range(0, (len(p5min['candles'])-1)):
                bardt = (p5min['candles'][p5bar]['datetime'])//1000
                bardate = time.strftime('%Y-%m-%d', time.localtime(bardt))
                bartime = time.strftime('%H:%M:%S', time.localtime(bardt))
                cur.execute("INSERT INTO prices_daily (stock_id, datetime, tradingday, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (row['id'], bardt, bardate, bartime, p5[p5bar]['open'], p5[p5bar]['high'], p5[p5bar]['low'], p5[p5bar]['close'], p5[p5bar]['volume']))

            cur.execute("UPDATE stocks SET tables_daily = '1' where id = %s;", (id))
            cur.execute("UPDATE stocks SET tables_daily_milli = %s where id = %s;", (start_asset, id))

            # Commit database changes
            conn.commit()


        except Exception as e:
            print(f"{stock_count}) Failed to add {row['symbol']}  ---->   {e}")
            print(f" ")
            print(f" **************************************************")
            print(f" Stocks that need attention related to missing data")
            print(f" **************************************************")
            for symbol in followups:
                print(f"{symbol}")
            quit()

    #Commit changes to DB
    conn.commit()

    endmilli = (current_milli_time()/1000)
    prog_end = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endmilli))
    print(f"Add Historical Daily Stock Prices ended at: {prog_end}")
    print(f"Total elapsed time to add data: {proper_round((endmilli - startmilli),1)} seconds")
    print(f" ")
    print(f" **************************************************")
    print(f" Stocks that need attention related to missing data")
    print(f" **************************************************")
    for symbol in followups:
        print(f"{symbol}")
