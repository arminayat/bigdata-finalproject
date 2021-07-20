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


###-----------------------------------------------------------
###------ queries to fetch twits from cassandra --------------
###-----------------------------------------------------------

###________________ fetch current time (timezone : UTC)_______
session.set_keyspace('big_data_twits')
current_year  = int(datetime.now(timezone.utc).strftime("%Y"))
current_month = int(datetime.now(timezone.utc).strftime("%m"))
current_day   = int(datetime.now(timezone.utc).strftime("%d"))
current_hour  = int(datetime.now(timezone.utc).strftime("%H"))


#================================== Accumulative queries ====================================
###________________________ count of tweets for each day in last week __________________
rows = session.execute(f"""SELECT year,month,day,count(*) FROM posts where
                          year={current_year} AND month={current_month} AND 
                          day IN ({current_day-6},{current_day-5},{current_day-4},
                                  {current_day-3},{current_day-2},{current_day-1},{current_day})
                          GROUP BY year,month,day""")
df = pd.DataFrame(rows)
print('\n\n count of tweets for each day in last week : \n\n')
print(df.to_string(index=False))

###________________________ count of tweets for each day in last month __________________
rows = session.execute(f"""SELECT year,month,day,count(*) FROM posts where
                          year={current_year} AND month>={current_month-1} AND month<={current_month}
                          GROUP BY year,month,day  
                          ALLOW FILTERING""")
df = pd.DataFrame(rows)
print('\n\n count of tweets for each day in last month : \n\n')
print(df.to_string(index=False))