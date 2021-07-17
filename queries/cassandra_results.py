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

###________________________ fetch last hour tweets __________________
rows = session.execute(f"""SELECT * FROM posts where  
                           year={current_year} AND month={current_month} AND day={current_day} AND 
                           hour IN ({current_hour-1},{current_hour})""")

print('\n\n fetch last hour tweets : \n\n')
for row in rows:
    print(row)

###________________________ fetch 1 last day (last 24 h) tweets __________________
rows = session.execute(f"""SELECT * FROM posts where  
                           year={current_year} AND month={current_month} AND
                           day IN ({current_day-1},{current_day})""")

print('\n\n fetch 1 last day (last 24 h) tweets : \n\n')
for row in rows:
    print(row)

###_______________________ fetch most common hashtag (between 1000 tweet) _________________
###        [ cause of Ram limitation to fetch all in pandas , use limit 1000]
#        -----------------------------------------------------------------------------
rows = session.execute(f"""SELECT hashtag,count(*) as cnt FROM hashtags GROUP BY hashtag LIMIT 1000""")
top_tag = pd.DataFrame(rows).sort_values('cnt',ascending=False).iloc[0]['hashtag']

###_______________________ fetch specific hashtag in time range tweets _________________
def time_range_hashtag(yy,mm,dd_s,dd_e,hh_s,hh_e,tag):
    days_interval = str(tuple(list(range(dd_s,dd_e))))
    hour_interval = str(tuple(list(range(hh_s,hh_e))))

    rows = session.execute(f"""SELECT * FROM hashtags where hashtag='{tag}' AND
                          year=2021 AND month=7 AND day in {days_interval} AND
                          hour IN {hour_interval} LIMIT 100""")
    df = pd.DataFrame(rows)
    print(f'\n\n fetch tweets have #{tag} hashtag by day in :({dd_s},{dd_e}) & hour in :({hh_s},{hh_e}) : \n\n')
    print(df.to_string(index=False))

##  call func:------ year ,       month    ,   day range   ,  hour range , hashtag -----
time_range_hashtag(yy=2021,mm=current_month,dd_s=17,dd_e=25,hh_s=0,hh_e=9,tag=top_tag)

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