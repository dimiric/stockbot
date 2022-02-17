from copy import copy
import psycopg2
import psycopg2.extras
import config as conf
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

with conn:
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    #Grab current symbols in Stock Table
    cur.execute("SELECT symbol, name, id, tables_filled FROM stocks")
    rows = cur.fetchall()
    xsymbs = [row['symbol'] for row in rows]
    
    #Alpaca grab stock list
    #print(f"Grabbing fresh stock list - {today}")
    #api = aapi.REST(conf.alp_apikey, conf.alp_secret, base_url=conf.alp_base_url) # or use ENV Vars shown below
    #assets = api.list_assets()
    #curr_symbols = [asset.symbol for asset in assets]

    #chunk_size = 10
    #for i in range(0, len(curr_symbols), chunk_size):
    #    symbol_chunk = curr_symbols[i:i+chunk_size]
    #    funda = c.search_instruments({symbol_chunk}, c.Instrument.Projection.FUNDAMENTAL)
    #    var1 = funda.json[{curr_symbols[i]}]['fundamental']['symbol']
    #    var2 = funda.json[{curr_symbols[i]}]['fundamental']['high52']
    #    print(f"{var1}  ->  52 WEEK HIGH: {var2}")
    #    print(f"--------")

    i = 0
    followups = []

    for row in rows:
        try:
            print(row)
            start_asset = (current_milli_time()/1000)
            asset_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(startmilli))

            k = 0
            #try:
            i = i + 1
            print(f"{i} out of {len(xsymbs)} - Adding Historical Price Data: {row['symbol']} {row['name']} ({row['id']}) ({row['tables_filled']}- {asset_start}")
            if row['tables_filled'] == 0 or row['tables_filled'] == 1 or row['tables_filled'] == 2:
                print(f"----------->  ATTENTION: {row['symbol']} is set to {row['tables_filled']} and may be missing data! ")
                followups.append(row['symbol'])

            ###################################################
            # Last 3 months of 1 minute price data for stock  #
            ###################################################
            today_ago = datetime.now(timezone.utc) - timedelta(days=90)
            hist_start = today_ago
            k = 4

            p5 = c.get_price_history_every_minute(row['symbol'],
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
            print(f"- Number of 1 min price records found for {row['symbol']}: {len(p5)}")

            for p5bar in range(0, (len(p5min['candles'])-1)):
                bardt = (p5min['candles'][p5bar]['datetime'])//1000
                bardate = time.strftime('%Y-%m-%d', time.localtime(bardt))
                bartime = time.strftime('%H:%M:%S', time.localtime(bardt))
                cur.execute("INSERT INTO prices_1min (stock_id, datetime, tradingday, tradingtime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (row['id'], bardt, bardate, bartime, p5[p5bar]['open'], p5[p5bar]['high'], p5[p5bar]['low'], p5[p5bar]['close'], p5[p5bar]['volume']))
            cur.execute("UPDATE stocks SET tables_filled = %s where id = %s;", (k, row['id']))

            # Commit database changes
            conn.commit()


            ###################################################
            # Last 9 months of 15 minute price data for stock #
            ###################################################
            today_ago = datetime.now(timezone.utc) - timedelta(days=270)
            hist_start = today_ago
            k = 5

            p5 = c.get_price_history_every_fifteen_minutes(row['symbol'],
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
            print(f"- Number of 15 min price records found for {row['symbol']}: {len(p5)}")

            for p5bar in range(0, (len(p5min['candles'])-1)):
                bardt = (p5min['candles'][p5bar]['datetime'])//1000
                bardate = time.strftime('%Y-%m-%d', time.localtime(bardt))
                bartime = time.strftime('%H:%M:%S', time.localtime(bardt))
                cur.execute("INSERT INTO prices_15min (stock_id, datetime, tradingday, tradingtime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (row['id'], bardt, bardate, bartime, p5[p5bar]['open'], p5[p5bar]['high'], p5[p5bar]['low'], p5[p5bar]['close'], p5[p5bar]['volume']))
            cur.execute("UPDATE stocks SET tables_filled = %s where id = %s;", (k, row['id']))

            # Commit database changes
            conn.commit()


            ###################################################
            # Last 9 months of 30 minute price data for stock #
            ###################################################
            today_ago = datetime.now(timezone.utc) - timedelta(days=270)
            hist_start = today_ago
            k = 6

            p5 = c.get_price_history_every_thirty_minutes(row['symbol'],
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
            print(f"- Number of 30 min price records found for {row['symbol']}: {len(p5)}")

            count60 = 0
            count120 = 0
            count240 = 0

            for p5bar in range(0, (len(p5min['candles'])-1)):
                ###################################################
                # 30 minute bar entry                             #
                ###################################################
                bardt = (p5min['candles'][p5bar]['datetime'])//1000
                bardate = time.strftime('%Y-%m-%d', time.localtime(bardt))
                bartime = time.strftime('%H:%M:%S', time.localtime(bardt))
                cur.execute("INSERT INTO prices_30min (stock_id, datetime, tradingday, tradingtime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (row['id'], bardt, bardate, bartime, p5[p5bar]['open'], p5[p5bar]['high'], p5[p5bar]['low'], p5[p5bar]['close'], p5[p5bar]['volume']))

                ###################################################
                # 1 hour creation bar creation and entry          #
                ###################################################
                if p5bar % 2 == 0:
                    count60 = count60 + 1
                    open60 = p5min['candles'][p5bar]['open']
                    high60 = p5min['candles'][p5bar]['high']
                    low60 = p5min['candles'][p5bar]['low']
                    close60 = p5min['candles'][p5bar]['close']
                    volume60 = p5min['candles'][p5bar]['volume']
                    datetime60 = p5min['candles'][p5bar]['datetime']
                if p5bar % 2 == 1:
                    if high60 < p5min['candles'][p5bar]['high']:
                        #print(f"New 1 hour high {high60} < {p5min['candles'][p5bar]['high']}")
                        high60 = p5min['candles'][p5bar]['high']
                    if low60 > p5min['candles'][p5bar]['low']:
                        #print(f"New 1 hour low {low60} > {p5min['candles'][p5bar]['low']}")
                        low60 = p5min['candles'][p5bar]['low']
                    close60 = p5min['candles'][p5bar]['close']
                    volume60 = volume60 + p5min['candles'][p5bar]['volume']
                    bardt = datetime60//1000
                    bardate = time.strftime('%Y-%m-%d', time.localtime(bardt))
                    bartime = time.strftime('%H:%M:%S', time.localtime(bardt))
                    #print(f" 1HR: Datetime: {datetime60}  Open:{open60}  High: {high60}  Low: {low60}  Close: {close60}  Volume: {volume60}")
                    cur.execute("INSERT INTO prices_1hr (stock_id, datetime, tradingday, tradingtime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (row['id'], bardt, bardate, bartime, open60, high60, low60, close60, volume60))

                ###################################################
                # 2 hour creation bar creation and entry          #
                ###################################################
                if p5bar % 4 == 0:
                    count120 = count120 + 1
                    open120 = p5min['candles'][p5bar]['open']
                    high120 = p5min['candles'][p5bar]['high']
                    low120 = p5min['candles'][p5bar]['low']
                    close120 = p5min['candles'][p5bar]['close']
                    volume120 = p5min['candles'][p5bar]['volume']
                    datetime120 = p5min['candles'][p5bar]['datetime']
                if p5bar % 4 in range (1,4):
                    if high120 < p5min['candles'][p5bar]['high']:
                        #print(f"New 2 hour high {high120} < {p5min['candles'][p5bar]['high']}")
                        high120 = p5min['candles'][p5bar]['high']
                    if low120 > p5min['candles'][p5bar]['low']:
                        #print(f"New 2 hour low {low120} > {p5min['candles'][p5bar]['low']}")
                        low120 = p5min['candles'][p5bar]['low']
                    volume120 = volume120 + p5min['candles'][p5bar]['volume']
                    if p5bar % 4 == 3:
                        close120 = p5min['candles'][p5bar]['close']
                        bardt = datetime120//1000
                        bardate = time.strftime('%Y-%m-%d', time.localtime(bardt))
                        bartime = time.strftime('%H:%M:%S', time.localtime(bardt))
                        #print(f" 2HR: Datetime: {datetime120}  Open:{open120}  High: {high120}  Low: {low120}  Close: {close120}  Volume: {volume120}")
                        cur.execute("INSERT INTO prices_2hr (stock_id, datetime, tradingday, tradingtime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (row['id'], bardt, bardate, bartime, open120, high120, low120, close120, volume120))

                ###################################################
                # 4 hour creation bar creation and entry          #
                ###################################################
                if p5bar % 8 == 0:
                    count240 = count240 + 1
                    open240 = p5min['candles'][p5bar]['open']
                    high240 = p5min['candles'][p5bar]['high']
                    low240 = p5min['candles'][p5bar]['low']
                    close240 = p5min['candles'][p5bar]['close']
                    volume240 = p5min['candles'][p5bar]['volume']
                    datetime240 = p5min['candles'][p5bar]['datetime']
                if p5bar % 8 in range (1,8):
                    if high240 < p5min['candles'][p5bar]['high']:
                        #print(f"New 4 hour high {high240} < {p5min['candles'][p5bar]['high']}")
                        high240 = p5min['candles'][p5bar]['high']
                    if low240 > p5min['candles'][p5bar]['low']:
                        #print(f"New 4 hour low {low240} > {p5min['candles'][p5bar]['low']}")
                        low240 = p5min['candles'][p5bar]['low']
                    volume240 = volume240 + p5min['candles'][p5bar]['volume']
                    if p5bar % 8 == 7:
                        close240 = p5min['candles'][p5bar]['close']
                        bardt = datetime240//1000
                        bardate = time.strftime('%Y-%m-%d', time.localtime(bardt))
                        bartime = time.strftime('%H:%M:%S', time.localtime(bardt))
                        #print(f" 4HR: Datetime: {datetime240}  Open:{open240}  High: {high240}  Low: {low240}  Close: {close240}  Volume: {volume240}")
                        cur.execute("INSERT INTO prices_4hr (stock_id, datetime, tradingday, tradingtime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (row['id'], bardt, bardate, bartime, open240, high240, low240, close240, volume240))
                    cur.execute("UPDATE stocks SET tables_filled = %s where id = %s;", (k, row['id']))

                    # Commit database changes
                    conn.commit()

            print(f"- Number of 1 hour price records added for {row['symbol']}: {count60}")
            print(f"- Number of 2 hour price records added for {row['symbol']}: {count120}")
            print(f"- Number of 4 hour price records added for {row['symbol']}: {count240}")

            end_asset = time.time()
            print(f"Elapsed time to add all data for {row['symbol']}: {proper_round((end_asset - start_asset),1)} seconds")

        except Exception as e:
            print(f"{i}) Failed to add {row['symbol']}  ---->   {e}")
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
    print(f"Add Historical Stock Prices ended at: {prog_end}")
    print(f"Total elapsed time to add data: {proper_round((endmilli - startmilli),1)} seconds")
    print(f" ")
    print(f" **************************************************")
    print(f" Stocks that need attention related to missing data")
    print(f" **************************************************")
    for symbol in followups:
        print(f"{symbol}")
