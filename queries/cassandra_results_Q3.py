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
                          year={yy} AND month={mm} AND day in {days_interval} AND
                          hour IN {hour_interval}""")
    for row in rows:
        print(row)

    ##___ count ____
    cnt = session.execute(f"""SELECT count(*) FROM hashtags where hashtag='{tag}' AND
                          year={yy} AND month={mm} AND day in {days_interval} AND hour IN {hour_interval}""")

        
    print(f'\n\n fetch {cnt[0][0]} tweets that have #{tag} hashtag by day in :[{dd_s},{dd_e}] & hour in :[{hh_s},{hh_e}] : \n\n')

#______________________________________________________________________________________
#  Call Func:------ year ,       month    ,   day range   ,  hour range , hashtag -----
time_range_hashtag(yy=2021,mm=current_month,dd_s=17,dd_e=25,hh_s=0,hh_e=9,tag=top_tag)
