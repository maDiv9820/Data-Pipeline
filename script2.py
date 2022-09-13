import sqlite3
import pandas as pd

connector = sqlite3.connect('database.db')
cursor = connector.cursor()
df = pd.read_csv('Final_Data.csv')
df.to_sql('device_data', connector, if_exists = 'append', index = False)