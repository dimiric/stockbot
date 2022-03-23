from os.path import exists
import time
import json

def CheckFileMake(file):
    # Check for log files
    file_exists = exists(file)
    if not file_exists:
        f = open(file, 'w')
        f.close()

# Rounding that might be error prone and need removed*********
def proper_round(num, dec=0):
    num = str(num)[:str(num).index('.')+dec+2]
    if num[-1] >= '5':
        return float(num[:-2-(not dec)]+str(int(num[-2-(not dec)])+1))
    return float(num[:-1])

# Self Explanatory
def current_milli_time():
    return round(time.time() * 1000)

# # Declare class to store JSON data into a python dictionary
# class read_data(object):
#     def __init__(self, jdata):
#         self.__dict__ = json.loads(jdata)
