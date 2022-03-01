# from asyncio.windows_events import NULL
from copy import copy
from numpy import float16
import psycopg2
import psycopg2.extras
import settings.config as conf
import alpaca_trade_api as aapi
import json
from os.path import exists
from datetime import datetime, timezone, timedelta
import datetime as d
import time
import linecache
import sys

#Initial Variables
new_stock_count = 0
updated_stock_count = 0
alpaca_asset_num = 0
query_time = 0
i = 0
on = 1
skipcount = 0
badcount = 0
inact_count = 0
act_count = 0

# More Descriptive Error Handling
def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    # Errors to logfile
    with open('logs/errors.txt', 'a+') as f:
        msg = 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)
        f.write(msg)
        f.write('\n')
    AddToProblemFile()
    print ('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))

# Symbols that are trouble
def AddToProblemFile():
    with open('logs/problemsymbols.txt', 'a+') as problemfile:
        problemfile.write(alpaca_asset.symbol)
        problemfile.write('\n')

# Rounding that might be error prone and need removed*********
def proper_round(num, dec=0):
    num = str(num)[:str(num).index('.')+dec+2]
    if num[-1]>='5':
        return float(num[:-2-(not dec)]+str(int(num[-2-(not dec)])+1))
    return float(num[:-1])

# Self Explanatory
def current_milli_time():
    return round(time.time() * 1000)

# Declare class to store JSON data into a python dictionary
class read_data(object):
  def __init__(self, jdata):
    self.__dict__ = json.loads(jdata)

#***********************************

#Variables
today = d.datetime.now()
startmilli = (current_milli_time()/1000)
prog_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(startmilli))
today_date = time.strftime('%Y-%m-%d', time.localtime(startmilli))

print(f"Add Stocks started at: {prog_start}")

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

# With Connection to Database active, run main set of commands
with conn:
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Grab current symbols in Stock Table from database
    cur.execute("SELECT symbol, name, table_funda_milli FROM stocks")
    rows = cur.fetchall()
    stored_symbols = [row['symbol'] for row in rows]

    # Read all Symbols that previously had a problem into a variable to avoid
    with open(conf.ProblemFile, 'r') as f:
        bad_symbols=f.read().split("\n")

    # Alpaca grab stock list
    print(f"Grabbing fresh stock list - {today}")
    api = aapi.REST(conf.alp_apikey, conf.alp_secret, base_url=conf.alp_base_url) # or use ENV Vars shown below
    alpaca_assets = api.list_assets()
    curr_symbols = [alpaca_asset.symbol for alpaca_asset in alpaca_assets]
    
    # initializing list to hold query times
    qtime_list = [] 
    for value in range(0,120):
        qtime_list.append(0)
    
    # Cycle through all symbols grabbed from Alpaca
    for alpaca_asset in alpaca_assets:
        if alpaca_asset.symbol in bad_symbols:
            badcount += 1
            print(f"Avoiding {alpaca_asset.symbol}")
            continue
        if alpaca_asset.symbol in stored_symbols:
            try:
                cur.execute("SELECT * FROM stocks where symbol = %s;", [alpaca_asset.symbol])
            except:
                print(f"{alpaca_asset.symbol} in stored_symbols but error when select statement run on stocks")
                with open('logs/issues.txt', 'a') as problemfile:
                    problemfile.write(alpaca_asset.symbol)
                    problemfile.write(' was in stored symbols, but errored on Select from stocks table\n')
            cur_id = cur.fetchall()
            id = cur_id[0][0]
            funda_state = cur_id[0][7]
            funda_update = cur_id[0][16]
            asset_status = cur_id[0][17]
            funda_date = time.strftime('%Y-%m-%d', time.localtime(funda_update))
            if ((funda_date == today_date) and (funda_state == 1)):
                skipcount += 1
#                print(f"Skipping {alpaca_asset.symbol}")
                continue
            if asset_status == "active" and alpaca_asset.status == 'inactive':
                print(f"Marking {alpaca_asset.symbol} to Inactive State")
                inact_count += 1
                cur.execute("UPDATE stocks SET status = 0, updated_date = %s where id = %s;", (today, id))
            if asset_status == "inactive" and alpaca_asset.status == 'active':
                print(f"Marking {alpaca_asset.symbol} to Active State")
                act_count += 1
                cur.execute("UPDATE stocks SET status = 1, updated_date = %s where id = %s;", (today, id))

        start_alpaca_asset = (current_milli_time()/1000)
        alpaca_asset_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(startmilli))
        alpaca_asset_num += 1

        # Process active Tradable symbols from Alpaca 
        if alpaca_asset.status == 'active' and alpaca_asset.tradable:
            #Fundamentals
            i = i + 1
            query_time = (current_milli_time()/1000)
            chunk120time = query_time - qtime_list[0]
            chunk1time = query_time - qtime_list[119]
            qtime_list.pop(0)
            qtime_list.append(query_time)
            print(f"  +++++ Time to perform last 120 TDA queries: {chunk120time} seconds")
            if ((chunk120time < 65) and (i > 120)):
                sleeptime = (66 - chunk120time)
                print(f"{i} - {chunk120time}:                  120 Marker --------->  Sleeping for {sleeptime} seconds.")
                time.sleep(sleeptime)
            if ((chunk1time < .55) and (i > 2)):
                sleeptime = (.6 - chunk1time)
                print(f"{i} - {chunk1time}:                    1 Marker --------->  Sleeping for {sleeptime} seconds.")
                time.sleep(sleeptime)

            try:
                funda = c.search_instruments({alpaca_asset.symbol}, c.Instrument.Projection.FUNDAMENTAL)
                assert funda.status_code == 200, funda.raise_for_status()

            except Exception as e:
                badcount += 1
                PrintException()
                continue

            f = funda.json()
            if json.dumps(funda.json(), indent=4) == '{}':
                print(f"Couldn't find any fundamental for symbol: {alpaca_asset.symbol}    Adding to Problem File for later review.")
                AddToProblemFile()
                continue
            f1 = alpaca_asset.symbol
            f2 = 'fundamental'
            f3 = f[f1][f2]

            # New Symbol to add to Stocks Table
            if alpaca_asset.symbol not in stored_symbols:
                new_stock_count += 1
                print(f"{alpaca_asset_num} out of {len(curr_symbols)}- Adding New Stock: ({new_stock_count}) {alpaca_asset.symbol} {alpaca_asset.name} - {alpaca_asset_start}")
                try:
                    cur.execute("INSERT INTO stocks (cusip, symbol, name, status, exchange, marginable, table_fundamentals, added_date, updated_date, type) VALUES (%s, %s, %s, 1, %s, %s, 0, %s, %s, %s);", (alpaca_asset.id, alpaca_asset.symbol, alpaca_asset.name, alpaca_asset.exchange, alpaca_asset.marginable, today, today, f[f1]['assetType']))
                    cur.execute("SELECT * FROM stocks where symbol = %s;", [alpaca_asset.symbol,])
                    cur_id = cur.fetchall()
                    id = cur_id[0][0]
                except Exception as e:
                    badcount += 1
                    PrintException()
                    continue
                try:
                    cur.execute("INSERT INTO fundamentals (stock_id, symbol, high52, low52, dividendAmount, dividendYield) VALUES (%s, %s, %s, %s, %s, %s);", (id, f3['symbol'], f3['high52'], f3['low52'], f3['dividendAmount'], f3['dividendYield']))
                except Exception as e:
                    badcount += 1
                    PrintException()
                    continue
            else:  # Update Existing Symbol in Stocks Table
                print(f"{alpaca_asset_num} out of {len(curr_symbols)}- Updating Stock: {alpaca_asset.symbol} {alpaca_asset.name} - {alpaca_asset_start}")
                updated_stock_count += 1
                cur.execute("UPDATE stocks SET updated_date = %s where id = %s;", (today, id))
                cur.execute("UPDATE fundamentals SET high52 = %s, low52 = %s, dividendAmount = %s, dividendYield = %s where stock_id = %s;", (f3['high52'], f3['low52'], f3['dividendAmount'], f3['dividendYield'], id))

            try:
                # Skipping dividendDate "format: 2022-02-17 00:00:00.000" and fund.dividendPayDate fields due to errors.  Come back to this.
                cur.execute("UPDATE fundamentals SET peRatio = %s, pegRatio = %s, pbRatio = %s, prRatio = %s where stock_id = %s;", (f3['peRatio'], f3['pegRatio'], f3['pbRatio'], f3['prRatio'], id))
                cur.execute("UPDATE fundamentals SET pcfRatio = %s, grossMarginTTM = %s, grossMarginMRQ = %s, netProfitMarginTTM = %s, netProfitMarginMRQ = %s where stock_id = %s;", (f3['pcfRatio'], f3['grossMarginTTM'], f3['grossMarginMRQ'], f3['netProfitMarginTTM'], f3['netProfitMarginMRQ'], id))
                cur.execute("UPDATE fundamentals SET operatingMarginTTM = %s, operatingMarginMRQ = %s, returnOnEquity = %s, returnOnAssets = %s, returnOnInvestment = %s where stock_id = %s;", (f3['operatingMarginTTM'], f3['operatingMarginMRQ'], f3['returnOnEquity'], f3['returnOnAssets'], f3['returnOnInvestment'], id))
                cur.execute("UPDATE fundamentals SET quickRatio = %s, currentRatio = %s, interestCoverage = %s, totalDebtToCapital = %s, ltDebtToEquity = %s where stock_id = %s;", (f3['quickRatio'], f3['currentRatio'], f3['interestCoverage'], f3['totalDebtToCapital'], f3['ltDebtToEquity'], id))
                cur.execute("UPDATE fundamentals SET totalDebtToEquity = %s, epsTTM = %s, epsChangePercentTTM = %s, epsChangeYear = %s, epsChange = %s where stock_id = %s;", (f3['totalDebtToEquity'], f3['epsTTM'], f3['epsChangePercentTTM'], f3['epsChangeYear'], f3['epsChange'], id))
                cur.execute("UPDATE fundamentals SET revChangeYear = %s, revChangeTTM = %s, revChangeIn = %s, sharesOutstanding = %s, marketCapFloat = %s where stock_id = %s;", (f3['revChangeYear'], f3['revChangeTTM'], f3['revChangeIn'], f3['sharesOutstanding'], f3['marketCapFloat'], id))
                cur.execute("UPDATE fundamentals SET marketCap = %s, bookValuePerShare = %s, shortIntToFloat = %s, shortIntDayToCover = %s, divGrowthRate3Year = %s where stock_id = %s;", (f3['marketCap'], f3['bookValuePerShare'], f3['shortIntToFloat'], f3['shortIntDayToCover'], f3['divGrowthRate3Year'], id))
                cur.execute("UPDATE fundamentals SET dividendPayAmount = %s, beta = %s, vol1DayAvg = %s, vol10DayAvg = %s, vol3MonthAvg = %s where stock_id = %s;", (f3['dividendPayAmount'], f3['beta'], f3['vol1DayAvg'], f3['vol10DayAvg'], f3['vol3MonthAvg'], id))
            except Exception as e:
                badcount += 1
                PrintException()
                continue

            end_alpaca_asset = (current_milli_time()/1000)
            answer = str(round((end_alpaca_asset - start_alpaca_asset), 2))
            print(f"Asset time: {answer} seconds")

            cur.execute("UPDATE stocks SET table_funda_milli = %s, table_fundamentals = %s where id = %s;", (start_alpaca_asset, on, id))

            # Commit database changes
            conn.commit()

    #Commit changes to DB
    conn.commit()

    endmilli = (current_milli_time()/1000)
    prog_end = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(endmilli))
    print(f"Add Stocks ended at: {prog_end}")
    print(f"Total elapsed time to add stocks: {proper_round((endmilli - startmilli),1)} seconds")
    print(f"Skipped: {skipcount}   New: {new_stock_count}   Updated: {updated_stock_count}")
    print(f"{inact_count} from Stock Table were marked inactive and {act_count} reactivated.")
    with open('logs/problemsymbols.txt', 'r') as f:
        bad_symbols=f.read().split("\n")
        print(f"Blackedlisted: {badcount}   ({bad_symbols})")