import psycopg2
import psycopg2.extras
# from datetime import datetime, timezone, timedelta
# import datetime as d
import utilities.error as err
import time
import settings.config as conf

def DBAddNewBar(dbcur, search, table, id, stock_symbol, bar_update, bar_add, badcount, **barX):
    # Need to see if the bar already exists in database
    # Need to expand this for situations where it finds multiple for same date....
    bardt = (barX['datetime'])//1000
    bardate = time.strftime('%Y-%m-%d', time.localtime(bardt))
    bartime = time.strftime('%H:%M:%S', time.localtime(bardt))

    # Establish Postgresql connection based on settings in config.py
#    sql = "SELECT count(*) FROM {table} where stock_id = %s and datetime = %s"
    try:
        dbcur.execute(
            "SELECT count(*) FROM {} where stock_id = %s and datetime = %s;".format(table), (id, bardt))
        result = dbcur.fetchall()
        price_count = result[0][0]
    except Exception as e:
        badcount += 1
        err.PrintException(stock_symbol, error_type = 'db_fail')
        exit()

    if price_count > 0:
        try:
            dbcur.execute("DELETE FROM {} where stock_id = %s and datetime = %s;".format(table), (id, bardt))
            bar_update += 1
        except Exception as e:
#            badcount += 1
            err.PrintException(stock_symbol, error_type = 'db_fail')
#            continue

    try:
        bar_add += 1
        if search == 'daily':
            dbcur.execute("INSERT INTO {} (stock_id, datetime, tradingday, open, high, low, close, volume, hist_source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1);".format(table), (id, bardt, bardate, barX['open'], barX['high'], barX['low'], barX['close'], barX['volume']))
        else:
            dbcur.execute("INSERT INTO {} (stock_id, datetime, tradingday, tradingtime, open, high, low, close, volume, hist_source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1);".format(table), (id, bardt, bardate, bartime, barX['open'], barX['high'], barX['low'], barX['close'], barX['volume']))
    except Exception as e:
#        badcount += 1
        err.PrintException(stock_symbol, error_type = 'db_fail')
#        continue

