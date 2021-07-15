# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 19:10:58 2021

@author: yaram
"""


from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream

from kafka import KafkaProducer

import json
from json import dumps
import time



# Stopwords (Used for tweet filtering) './'
f = open('./data/hazm_stopwords.txt', 'r', encoding="utf8")
stop_words = f.read()
stop_words = stop_words.splitlines()
f.close()

produce_idx = 0



######################## Set Up Tweeter app ############################


access_token = '723604286333173760-XhbUSFTRBcBS4tUJxwmLijdyb503Awm'          
access_token_secret =  '2aYieQez40V8rGBIdqpeN8ONZrCoyk0VnmdbkPC4secrR' 
api_key =   '1EnLErbcBHsv4ajxMrlaVJI46'
api_secret =  'VPvOSew0XlHXrIHHkO2q0uSkuWfE1wBBVwBhEcWd48ZhVBowqg'

auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)



###################### Streamer and Producer ##########################


class stream_listener(StreamListener):
    

    
    def on_data(self, data):
        global produce_idx
        
        json_ = json.loads(data) # String to Dict
        for attribute, value in json_.items():
           print(attribute)
           print(value)
           input("********")
        input("*******************")
        
        print(produce_idx)
        print(json_['text'])
        
        producer.send("dirty_tweets", value=dumps(json_).encode('utf-8'))
        produce_idx += 1
        time.sleep(5)
       
        return True
    
    def on_error(self, status):
        
        print (status)
        
      

producer = KafkaProducer(bootstrap_servers='localhost:29092',api_version=(0,10))
l = stream_listener()

stream = Stream(auth, l)
# Can't filter only on language
# Can't retrieve more than 1% of tweets anyway, so ok to only crawl tweets containing our stopwords
stream.filter(track = stop_words, languages=["fa"])
