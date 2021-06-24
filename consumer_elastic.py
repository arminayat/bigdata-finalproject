# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 19:10:58 2021

@author: yaram
"""

from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'basic',
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset= 'latest', # 'earliest', # Start from last consumed, #'latest' start from last produce
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000, #ms # Ok. cuz our messages come every 5 seconds
     group_id='elastic_search',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    print(message.value)
    print(message.key)
    
    # Load new data into your database