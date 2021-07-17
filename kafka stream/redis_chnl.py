from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from datetime import datetime
import redis


consumer = KafkaConsumer(
    'clean_tweets',
    bootstrap_servers=['localhost:29092'],
    # 'earliest', # Start from last consumed, #'latest' start from last produce
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    group_id='twitter',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

r = redis.Redis(host='localhost', port=6379)