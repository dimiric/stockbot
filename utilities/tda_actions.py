# TD Ameritrade Token check/refresh/alert
import settings.config as conf
import utilities.error as err
from tda import auth, client
from os.path import exists
import json

def QueryTDA(c, qtype, stock_symbol, hist_start, hist_end, badcount):
    ###################################################
    # Last X years of daily price data for stock  #
    ###################################################
    try:
        price_data = c.get_price_history_every_day(stock_symbol, start_datetime=hist_start, end_datetime=hist_end)
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
