# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 19:10:58 2021

@author: zaha
"""

from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
#from elasticsearch import Elasticsearch

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

# Elastic search configuation

#es = Elasticsearch(HOST=["http://localhost"], PORT=9200)
#es = Elasticsearch()

# This loop will consume data forever

for message in consumer:
    
    twit = message.value

    twittt = {k: twit[k] for k in ("created_at",'keywords_k','hashtags_k','text_k', 'id_str')}
    
    hour = twittt["created_at"][11:13]
    day = twittt["created_at"][8:10]
    twittt['hour'] = hour 
    twittt['day_k'] = day
    print(twittt)
#    index=es.index(index="twitttt", body=twittt)
    
    
    print("*************************")
    
    producer.send('history', value=twit)
   
    
