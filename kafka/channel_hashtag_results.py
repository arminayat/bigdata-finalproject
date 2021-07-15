# -*- coding: utf-8 -*-
"""
Created on Mon Jul 05 04:19:37 2021

@author: nilam
"""

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from json import loads, dumps
import pandas as pd
import json
import pprint

########################## Define Kafka clients #########################

# Produce persistent data
producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'),
                         api_version=(0,10))


consumer = KafkaConsumer(
    'static',
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset= 'earliest', # 'earliest', # Start from last consumed, #'latest' start from last produce
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000,
     group_id='twitter',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     api_version=(0,10))


def cassandra_connection():    
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    session.set_keyspace('big_data_twits')

    return session, cluster

session, cluster = cassandra_connection()


###-----------------------------------------------------------
###------ queries to fetch twits from cassandra --------------
###-----------------------------------------------------------

###________________ fetch current time (timezone : USA)_______
session.set_keyspace('big_data_twits')
current_year  = int(datetime.now(timezone.utc).strftime("%Y"))
current_month = int(datetime.now(timezone.utc).strftime("%m"))
current_day   = int(datetime.now(timezone.utc).strftime("%d"))
current_hour  = int(datetime.now(timezone.utc).strftime("%H"))

###________________________ fetch last hour tweets __________________
rows = session.execute(f"""SELECT * FROM posts where  
                           year={current_year} AND month={current_month} AND day={current_day} AND 
                           hour IN ({current_hour-1},{current_hour})""")
df = pd.DataFrame(rows)
print('\n\n fetch last hour tweets : \n\n')
print(df.to_string(index=False))

###________________________ fetch 1 last day (last 24 h) tweets __________________
rows = session.execute(f"""SELECT * FROM posts where  
                           year={current_year} AND month={current_month} AND
                           day IN ({current_day-1},{current_day})""")
df = pd.DataFrame(rows)
print('\n\n fetch 1 last day (last 24 h) tweets : \n\n')
print(df.to_string(index=False))

###________________________ fetch specific hashtag in time range tweets __________________
rows = session.execute("""SELECT * FROM hashtags where hashtag='طالبان' AND
                          year=2021 AND month=7 AND day in (10,11,12,13,14,15) AND
                          hour IN (3,4)""")
df = pd.DataFrame(rows)
print('\n\n fetch specific hashtag in time range tweets : \n\n')
print(df.to_string(index=False))

#=========================== Accumulative queries ========================================
###________________________ count of tweets for each day in last week __________________
rows = session.execute(f"""SELECT count(*) FROM posts 
                          year={current_year} AND month={current_month} AND 
                          day IN ({current_day-6},{current_day-5},{current_day-4},{current_day-3},{current_day-2},{current_day-1},{current_day})
                          GROUP BY year,month,day""")
df = pd.DataFrame(rows)
print('\n\n count of tweets for each day in last week : \n\n')
print(df.to_string(index=False))

###________________________ count of tweets for each day in last month __________________
rows = session.execute(f"""SELECT count(*) FROM posts where
                          year={current_year} AND month>={current_month-1} AND month<={current_month}
                          GROUP BY year,month,day  
                          ALLOW FILTERING""")
df = pd.DataFrame(rows)
print('\n\n count of tweets for each day in last month : \n\n')
print(df.to_string(index=False))