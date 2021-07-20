# -*- coding: utf-8 -*-
"""
Created on Sat Jul 17 08:49:23 2021

@authors: nilam & zaha
"""

from datetime import datetime, timezone
from clickhouse_driver import Client
from json import loads, dumps
from datetime import date
import pandas as pd

client = Client(host='localhost',port=9000,user='admin',password='admin') 


##___________________________ show tweets table _____________________________

twitts = client.execute('SELECT * FROM Big_analytics.tweets LIMIT 10')
cnt    = client.execute('SELECT count(*) FROM Big_analytics.tweets')
df     = pd.DataFrame(twitts)
print(f'\n\n tweets table :(# of db cols : {cnt[0][0]}) \n\n')
print(df)

##------------------------------------------------------------------!
##     for large data size we don't use pandas for reporting
##------------------------------------------------------------------!
# for twit in twitts:
    # print(twit)

##___________________________ show hashtags table _____________________________

hashtags = client.execute('SELECT * FROM Big_analytics.hashtags LIMIT 10')
cnt      = client.execute('SELECT count(*) FROM Big_analytics.hashtags')
df       = pd.DataFrame(hashtags)
print(f'\n\n hashtags table :(# of db cols : {cnt[0][0]}) \n\n')
print(df)

##------------------------------------------------------------------!
##     for large data size we don't use pandas for reporting
##------------------------------------------------------------------!
# for twit in hashtags:
    # print(twit)

##___________________________ show keywords table _____________________________

keywords = client.execute('SELECT * FROM Big_analytics.keywords LIMIT 10')
cnt      = client.execute('SELECT count(*) FROM Big_analytics.keywords')
df       = pd.DataFrame(keywords)
print(f'\n\n keywords table :(# of db cols : {cnt[0][0]}) \n\n')
print(df)

##------------------------------------------------------------------!
##     for large data size we don't use pandas for reporting
##------------------------------------------------------------------!
# for twit in keywords:
    # print(twit)

##___________________________ show users table _____________________________

#users    = client.execute('SELECT * FROM Big_analytics.users LIMIT 10')
#cnt      = client.execute('SELECT count(*) FROM Big_analytics.users')
#df       = pd.DataFrame(users)
#print(f'\n\n users table :(# of db cols : {cnt[0][0]}) \n\n')
#print(df)

##------------------------------------------------------------------!
##     for large data size we don't use pandas for reporting
##------------------------------------------------------------------!
# for twit in users:
    # print(twit)



#########################################################################
#########################      SAND BOX         #########################
#########################################################################

### tmp = client.execute('DROP DATABASE Big_analytics') 
### tmp = client.execute('CREATE DATABASE IF NOT EXISTS Big_analytics') 
### tmp = client.execute('SHOW DATABASES')
### tmp = client.execute('SHOW TABLES FROM Big_analytics')
### tmp = client.execute('SELECT * FROM Big_analytics.hashtags')


# client.execute('DROP DATABASE sandbox') 
# client.execute('CREATE DATABASE IF NOT EXISTS sandbox')  
# client.execute("""CREATE TABLE IF NOT EXISTS sandbox.test( year Int32,id_str String )
                #   ENGINE = MergeTree partition by (year) order by (id_str);""")
 
# year = 1400  
# id_str = '565689' 
# text = 'Iran' 
 
# client.execute(f"""INSERT INTO sandbox.test VALUES ({year},'{text}')""")   
 
# twitts = client.execute('SELECT * FROM sandbox.test LIMIT 10') 
# for twit in twitts: 
#     print(twit) 
