# TD Ameritrade Token check/refresh/alert
from httpx import Client
import settings.config as conf
import utilities.error as err
from tda import auth, client
from os.path import exists
import json

def QueryTDA(c, tdatype, stock_symbol, hist_start, hist_end, badcount):
    ###################################################
    # Last X years of daily price data for stock  #
    ###################################################
    freq = 'c.PriceHistory.Frequency.DAILY'
    freqtype = 'c.PriceHistory.FrequencyType.DAILY'
    # searches = ['daily', '1min', '5min', '15min', '30min']
    try:
        if tdatype == 'daily':
            price_data = c.get_price_history_every_day(stock_symbol, start_datetime=hist_start, end_datetime=hist_end)
        elif tdatype == '1min':
            price_data = c.get_price_history_every_minute(stock_symbol, start_datetime=hist_start, end_datetime=hist_end)
        elif tdatype == '5min':
            price_data = c.get_price_history_every_five_minutes(stock_symbol, start_datetime=hist_start, end_datetime=hist_end)
        elif tdatype == '15min':
            price_data = c.get_price_history_every_fifteen_minutes(stock_symbol, start_datetime=hist_start, end_datetime=hist_end)
        else:
            price_data = c.get_price_history_every_thirty_minutes(stock_symbol, start_datetime=hist_start, end_datetime=hist_end)
        assert price_data.status_code == 200, price_data.raise_for_status()
        prices = price_data.json()
        if json.dumps(price_data.json(), indent=4) == '{}':
            print(
                f"Couldn't find any new price data for symbol: {stock_symbol}    Adding to Problem File for later review.")
            err.AddToProblemFile(stock_symbol)
            prices = {'empty' : 'True'}
        # print(json.dumps(price_data.json(), indent=4))
    except Exception as e:
        badcount += 1
        err.PrintException(stock_symbol, "blacklist")
        prices = {'empty' : 'True'}
    return prices
