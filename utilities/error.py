import settings.config as conf
import linecache
import sys
from copy import copy
from pushbullet import Pushbullet

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

