# -*- coding: utf-8 -*-
"""
Created on Mon Jul 05 04:19:37 2021

@author: nilam
"""


########################## Define Kafka clients #########################

# Produce persistent data
producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'),
                         api_version=(0,10))


consumer = KafkaConsumer(
    'static',
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset= 'earliest', # 'earliest', # Start from last consumed, #'latest' start from last produce
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000,
     group_id='twitter',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     api_version=(0,10))

