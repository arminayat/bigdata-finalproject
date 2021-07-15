# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 12:05:01 2021

@author: nilam
"""

from kafka import KafkaConsumer, KafkaProducer
from cassandra.cluster import Cluster
import json



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