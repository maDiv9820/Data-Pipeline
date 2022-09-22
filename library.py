import pandas as pd     # For opening and creating dataframes
import numpy as np      # For creating mathematical and array operations
import boto3            # For using AWS services in python
import json             # Converting from Python to JSON format or vice-versa
import sqlite3          # For creating a database and dumping data into the table
import configparser     # To read config files for hard-coded values
import logging          # To save logs

# Taking out all the values from config file
config = configparser.ConfigParser()
config.read('config.ini')

samsung_headers = eval(config.get('Script1', 'samsung_headers'))
fitbit_headers = eval(config.get('Script1', 'fitbit_headers'))
file_paths = eval(config.get('Script1', 'file_paths'))
queue_url = config.get('AWS', 'queue_url')
endpoint_url = config.get('AWS', 'endpoint_url')
data_headers = eval(config.get('Script1', 'data_headers'))
database_name = config.get('Script2', 'database_name')
table_name = config.get('Script2', 'table_name')
data_fitbit_dict = eval(config.get('Script1', 'data_fitbit_dict'))
data_samsung_dict = eval(config.get('Script1', 'data_samsung_dict'))

# Creating a log file
logging.basicConfig(filename="log.txt", level=logging.DEBUG, format="%(asctime)s %(message)s")
