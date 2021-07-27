# -*- coding: utf-8 -*-
"""
Created on Sat Jul 17 08:49:23 2021

@authors: nilam & zaha
"""
 
from kafka import KafkaConsumer, KafkaProducer 
from datetime import datetime, timezone 
from clickhouse_driver import Client 
from json import loads, dumps 
from datetime import date 
 
########################## Define Kafka clients ######################### 
 
# Produce persistent data 
producer = KafkaProducer(bootstrap_servers=['localhost:29092'], 
                         value_serializer=lambda x:  
                         dumps(x).encode('utf-8'), 
                         api_version=(0,10)) 
 
 
consumer = KafkaConsumer( 
    'analytics', 
     bootstrap_servers=['localhost:29092'], 
     auto_offset_reset= 'earliest', # 'earliest', # Start from last consumed, #'latest' start from last produce 
     enable_auto_commit=True, 
     auto_commit_interval_ms = 1000, 
     group_id='twitter', 
     value_deserializer=lambda x: loads(x.decode('utf-8')), 
     api_version=(0,10)) 

 
########################## clickhouse connection ######################### 
 
client = Client(host='localhost',port=9000,user='admin',password='admin') 
  
####################### create database & tables ######################### 

client.execute('CREATE DATABASE IF NOT EXISTS Big_analytics') 
 
##___ tweets table « partition keys :(year,month,day) , sorted keys: (hour,minute,second,id) »
client.execute("""CREATE TABLE IF NOT EXISTS Big_analytics.tweets( 
    year Int32 , month Int32  , day Int32, 
    hour Int32 , minute Int32 , second Int32, 
    Tid_str String, 
    hashtags Array(String), 
    key_words Array(String)
) ENGINE = MergeTree partition by (year,month,day) order by (hour,minute,second,Tid_str); 
""") 

## ___ hashtags table « partition keys :(year,month,day) , sorted keys: (hour,minute,second,id) »
client.execute("""CREATE TABLE IF NOT EXISTS Big_analytics.hashtags( 
    year Int32 , month Int32  , day Int32, 
    hour Int32 , minute Int32 , second Int32, 
    Tid_str String, 
    hashtags String 
) ENGINE = MergeTree partition by (year,month,day) order by (hour,minute,second,Tid_str); 
""") 
 
##___ keywords table « partition keys :(year,month,day) , sorted keys: (hour,minute,second,tweet_id) »
client.execute("""CREATE TABLE IF NOT EXISTS Big_analytics.keywords( 
    year Int32 , month Int32  , day Int32, 
    hour Int32 , minute Int32 , second Int32, 
    Tid_str String, 
    key_words String
) ENGINE = MergeTree partition by (year,month,day) order by (hour,minute,second,Tid_str); 
""")

##___ users table « partition keys :(year,month,day) , sorted keys: (hour,minute,second,user_id,tweet_id) »
client.execute("""CREATE TABLE IF NOT EXISTS Big_analytics.users( 
    year Int32 , month Int32  , day Int32, 
    hour Int32 , minute Int32 , second Int32, 
    Uname   String,
    location String,
    Uid_str  String,
    Tid_str  String
) ENGINE = MergeTree partition by (year,month,day) order by (hour,minute,second,Uid_str,Tid_str); 
""")

######################## stream twits to superset channel ####################### 
 
import pprint

for message in consumer: 
 
    tweet               = message.value 
    dtime               = tweet['created_at'] 
    new_datetime        = datetime.strftime(datetime.strptime(dtime,'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S') 
    date ,time          = new_datetime.split(' ') 
    year,month,day      = [ int(x) for x in date.split('-')] 
    hour,minute,second  = [ int(x) for x in time.split(':')] 
    Tid_str             = tweet['id_str'] 
    Uid_str             = tweet['user']['id_str'] 
    Uname               = str(tweet['user']['name'])
    location            = str(tweet['user']['location'])
    hashtags_k          = tweet['hashtags_k']
    keywords_k          = tweet['keywords_k']
    punclist            = ['/','\\','\'','\"']
    
    ##__ remove punctuations from strings______!
    for punc in punclist: 
        Uname    = Uname.replace(punc,'')
        location = location.replace(punc,'')
    ##_________________________________________!

    #_____________________________________________________________________!
    ##____________________ insert to tweets table __________
    client.execute(f"""INSERT INTO Big_analytics.tweets VALUES 
                       ({year},{month},{day},{hour},{minute},{second},'{Tid_str}',{hashtags_k},{keywords_k}
                        )""") 
 
    ##___________________ insert to hashtags table _________
    for tag in hashtags_k:
        client.execute(f"""INSERT INTO Big_analytics.hashtags VALUES 
                        ({year},{month},{day},{hour},{minute},{second},'{Tid_str}','{tag}')""")  

    ##___________________ insert to keywords table _________
    for word in keywords_k:
        client.execute(f"""INSERT INTO Big_analytics.keywords VALUES 
                            ({year},{month},{day},{hour},{minute},{second},{Tid_str},'{word}')""")
    
    print("tweet id: "+Tid_str+" added to clickhouse")
    #_____________________________________________________________________!

    ##___________________ insert to users table ____________
    client.execute(f"""INSERT INTO Big_analytics.users VALUES 
                       ({year},{month},{day},{hour},{minute},{second},'{Uname}','{location}','{Uid_str}','{Tid_str}')""")
 
    print("user : "+Uname+" added to clickhouse") 
    #_____________________________________________________________________!

    ##___________________ for future streaming ____________
    # producer.send('persistance3', value=tweet)