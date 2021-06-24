# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 19:09:46 2021

@author: yaram
"""
from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
for e in range(1000):
    data = {'number' : e}
    producer.send('numtest', value=data)
    print(e)
    sleep(5)