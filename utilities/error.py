import settings.config as conf
import linecache
import sys
from copy import copy

'''
from tda import auth, client
from numpy import float16
from tqdm import tqdm
#import sqlite3
#import pandas
#import csv
import psycopg2
import psycopg2.extras
import alpaca_trade_api as aapi
import json
from os.path import exists
from datetime import datetime, timezone, timedelta
import datetime as d
import time

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
'''

def PrintException(stock_symbol, error_type):
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
    if error_type == 'blacklist':
        AddToProblemFile(stock_symbol)
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(
        filename, lineno, line.strip(), exc_obj))

# Symbols that are trouble


def AddToProblemFile(symbol):
    with open(conf.ProblemFile, 'a+') as problemfile:
        problemfile.write(symbol)
        problemfile.write('\n')
