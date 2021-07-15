# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 19:10:58 2021

@author: yaram
"""

from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps


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



############### Insert data to elastic search here #####################


# This loop will consume data forever
for message in consumer:
    
    twit = message.value
    print(twit)
    
    # Load new data into your database
    
    
    # If you want to produce something 
    #producer.send('elastic', value=twit)
    
    
    
    
    
    
    
    
    
    
    
    