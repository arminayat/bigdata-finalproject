# -*- coding: utf-8 -*-
"""
Created on Sat Jul 10 16:05:19 2021

@author: z_hab
"""

from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps


########################## Define Kafka clients #########################


# Produce persistent data
producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'),api_version=(0,10))


consumer = KafkaConsumer(
    'persistance',
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset= 'earliest', # 'earliest', # Start from last consumed, #'latest' start from last produce
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000,
     group_id='twitter',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     api_version=(0,10))



############### Insert data to elastic search here #####################





for message in consumer:
    
    twit = message.value

    print(twit)
    
    print("*************************")
    