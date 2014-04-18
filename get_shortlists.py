import sys
import traceback
import json
import bson.json_util
import time
import datetime
import pyodbc
import ConfigParser
from pprint import pprint as pp
from pprint import pformat as pf



config = ConfigParser.ConfigParser()
config.read('/etc/analytics/config.ini')

dsn = config.get("vertica","dsn")
user = config.get("vertica","user")
pwd = config.get("vertica","password")

conn = pyodbc.connect(DSN=dsn, uid=user, pwd=pwd)
cursor = conn.cursor()

def main():
    """main entry point"""
    global config,logger
    
    bootstrap()

    try:
        logger.info('STARTING')

        # connect to vertica


        # get all style names
        get_shortlist()

    except Exception as e:
        logger.exception("error getting shorlist data")
        raise
    finally:
        logger.info('FINISHED')