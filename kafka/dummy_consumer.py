# -*- coding: utf-8 -*-
"""
Created on Fri Jun 25 17:24:12 2021

@author: yaram
"""

from kafka import KafkaConsumer, KafkaProducer
from json import loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset= 'earliest', # 'earliest', # Start from last consumed, #'latest' start from last produce
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000,
     group_id='test',
     value_deserializer=lambda x: loads(x.decode('utf-8')))



for message in consumer:
    
    print(message.value)
    
    # Load new data into your database
    