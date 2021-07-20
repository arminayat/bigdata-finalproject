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

###________________________ fetch last hour tweets __________________
rows = session.execute(f"""SELECT * FROM posts where  
                           year={current_year} AND month={current_month} AND day={current_day} AND 
                           hour IN ({current_hour},{current_hour})""")
for row in rows:
    print(row)

###____________________ count of fetch last hour tweets __________________
cnt = session.execute(f"""SELECT count(*) FROM posts where  year={current_year} AND month={current_month} AND day={current_day} AND 
                           hour IN ({current_hour},{current_hour})""")
print(f'\n\n fetch {cnt[0][0]} rows from last hour tweets \n\n')