"""
Load traffic.csv into "traffic" table in sqlite3 database.

Drop and report invalid rows.
- ip should be valid IP (see ipaddress)
- time must not be in the future
- path can't be empty
- status code must be a valid HTTP status code (see http.HTTPStatus)
- size can't be negative or empty

Report the percentage of bad rows. Fail the ETL if there are more than 5% bad rows
"""

import ipaddress
import pandas as pd
import sqlite3

# create table schema for sql

traffic_schema = '''
CREATE TABLE traffic (
  ip CHAR(12) NOT NULL,
  time DATETIME NOT NULL,
  path TEXT,
  status NUMERIC(3,0) NOT NULL,
  size NUMERIC(4,0) NOT NULL
)
'''

# add table schema to db

db_file = 'traffic.db'
conn = sqlite3.connect(db_file)
conn.executescript(traffic_schema)

# validation function

def is_valid_row(row):

  # validating ip address
  try:
    ip = ipaddress.ip_address(row['ip'])
  except ValueError:
      return False
  
  # validating time is not future
  now = pd.Timestamp.now()
  if row['time'] > now:
     return False
  
  # validating path is not empty
  if pd.isnull(row['path']) or row['path'].strip() == '':
     return False
  
  # validating https status code
  if row['status'] < 100 or row['status'] >= 600:
     return False
  
  # validating size is not empty or negative
  if pd.isnull(row['size']) or row['size'] < 0:
     return False
  
  return True

# import, clean and inser the clean data in the database

df = pd.read_csv('traffic.csv', parse_dates=['time'])
df_clean = df[df.apply(is_valid_row, axis=1)]
df.to_sql('traffic', conn, index=False, if_exists='append')

# reporting bad data

num_bad = len(df) - len(ok_df)
percent_bad = num_bad/len(df) * 100
print(f'{percent_bad:.2f}% bad rows')
if num_bad > 0:
    bad_rows = df[~df.index.isin(ok_df.index)]
    print('bad rows:')
    print(bad_rows)

