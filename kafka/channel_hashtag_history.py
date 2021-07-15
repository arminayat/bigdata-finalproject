# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 12:05:01 2021

@author: nilam
"""

from kafka import KafkaConsumer, KafkaProducer
from cassandra.cluster import Cluster
from datetime import datetime, timezone
from json import loads, dumps
import json
import pprint


########################## Define Kafka clients #########################

# Produce persistent data
producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'),
                         api_version=(0,10))


consumer = KafkaConsumer(
    'clean_tweets',
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset= 'earliest', # 'earliest', # Start from last consumed, #'latest' start from last produce
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000,
     group_id='twitter',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     api_version=(0,10))

########################## cassandra connection #########################

def cassandra_connection():
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()  
    
    session.execute("""CREATE KEYSPACE IF NOT EXISTS big_data_twits 
                       WITH REPLICATION ={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")      

    #___ posts table « partition keys :(year,month,day) , cluster keys: (hour,minute,second,id) »
    session.execute("""CREATE TABLE IF NOT EXISTS big_data_twits.posts             
                       (year int,month int,day int,hour int,minute int,second int, id text,
                       PRIMARY KEY((year,month,day),hour,minute,second,id))""")

    #___ hashtags table « partition keys :(hashtag) , cluster keys: (year,month,day,hour,minute,second,id) »
    session.execute("""CREATE TABLE IF NOT EXISTS big_data_twits.hashtags
                       (hashtag text,year int,month int,day int,hour int,minute int,second int, id text,
                       PRIMARY KEY((hashtag),year,month,day,hour,minute,second,id))""")

    #___ key_words table « partition keys :(keyword) , cluster keys: (year,month,day,hour,minute,second,id) »
    session.execute("""CREATE TABLE IF NOT EXISTS big_data_twits.key_words
                       (keyword text,year int,month int,day int,hour int,minute int,second int, id text,
                       PRIMARY KEY((keyword),year,month,day,hour,minute,second,id))""")

    return session, cluster

session, cluster = cassandra_connection()

session.set_keyspace('big_data_twits')
for message in consumer:
    tweet               = message.value
    dtime               = tweet['created_at']
    new_datetime        = datetime.strftime(datetime.strptime(dtime,'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S')
    date ,time          = new_datetime.split(' ')
    year,month,day      = date.split('-')
    hour,minute,second  = time.split(':')
    id_str              = tweet['id_str']
    hashtags_k          = tweet['hashtags_k']
    keywords_k          = tweet['keywords_k']

    #____________________ insert to posts table _____________
    session.execute("""INSERT INTO  posts (year,month,day,hour,minute,second,id) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s)""", [int(year),int(month),int(day),int(hour),int(minute),int(second),id_str])    
     #____________________ insert to hashtags table _________
    if(hashtags_k!=[]):
        for tag in hashtags_k:
            session.execute("""INSERT INTO  hashtags  (hashtag,year,month,day,hour,minute,second, id) 
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", [tag,int(year),int(month),int(day),int(hour),int(minute),int(second),id_str])
    #____________________ insert to keywords table __________
        for word in keywords_k:
            session.execute("""INSERT INTO  key_words (keyword,year,month,day,hour,minute,second, id) 
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", [word,int(year),int(month),int(day),int(hour),int(minute),int(second),id_str])

    print("tweet id: "+id_str+" added to cassandra")