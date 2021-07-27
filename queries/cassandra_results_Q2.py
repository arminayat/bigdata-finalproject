# -*- coding: utf-8 -*-
"""
Created on Mon Jul 05 04:19:37 2021

@author: nilam
"""

from datetime import datetime, timezone
from cassandra.cluster import Cluster
from json import loads, dumps
import pandas as pd
import json
import pprint


###-----------------------------------------------------------
###---------------- cassandra connection  --------------------
###-----------------------------------------------------------

def cassandra_connection():    
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    session.set_keyspace('big_data_twits')
    return session, cluster

session, cluster = cassandra_connection()

###________________ fetch current time (timezone : UTC)_______
session.set_keyspace('big_data_twits')
current_year  = int(datetime.now(timezone.utc).strftime("%Y"))
current_month = int(datetime.now(timezone.utc).strftime("%m"))
current_day   = int(datetime.now(timezone.utc).strftime("%d"))
current_hour  = int(datetime.now(timezone.utc).strftime("%H"))

###________________________ fetch 1 last day (last 24 h) tweets __________________
rows = session.execute(f"""SELECT * FROM posts where  
                           year={current_year} AND month={current_month} AND
                           day IN ({current_day-1},{current_day}) LIMIT 100""")

print('\n\n fetch 1 last day (last 24 h) tweets : \n\n')
for row in rows:
    print(row)

###______________ count of fetch 1 last day (last 24 h) tweets ______________
cnt = session.execute(f"""SELECT count(*) FROM posts where  year={current_year} AND month={current_month} AND
                           day IN ({current_day-1},{current_day})""")

print(f'\n\n fetch {cnt[0][0]} rows from last hour tweets \n\n')
