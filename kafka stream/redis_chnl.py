from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
from datetime import datetime
import redis


consumer = KafkaConsumer(
    'statistics',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    group_id='twitter',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'),
                         api_version=(0, 10))

r = redis.Redis(host='localhost', port=6379)


def store_tweet_by_keyword(tweet):
    keywords = tweet["keywords_k"]
    t_id = tweet["id"]
    for keyword in keywords:
        r.set(f"keyword:{keyword}:{t_id}", dumps(tweet), ex=60 * 60 * 6)


def store_tweet_by_time(tweet):
    time_key = datetime.strftime(datetime.strptime(
        tweet["created_at"], '%a %b %d %H:%M:%S +0000 %Y'), '%Y:%m:%d:%H')
    r.lpush(f"tweets:{time_key}", dumps(tweet))


def store_last_hour_hashtags(tweet):
    hashtags = tweet["hashtags_k"]
    for hashtag in hashtags:
        r.set(f"last_hour_hashtags:{hashtag}", hashtag, nx=True)
        r.expire(f"last_hour_hashtags:{hashtag}", 60 * 60)


def store_last_hashtags(tweet):
    hashtags = tweet["hashtags_k"]
    for hashtag in hashtags:
        r.lpush("last_hashtags", hashtag)
        r.ltrim("last_hashtags", 0, 999)


def store_last_tweets(tweet):
    r.lpush("last_tweets", dumps(tweet))
    r.ltrim("last_tweets", 0, 99)


for msg in consumer:
    tweet = msg.value
    store_tweet_by_keyword(tweet)
    store_tweet_by_time(tweet)
    store_last_hour_hashtags(tweet)
    store_last_hashtags(tweet)
    store_last_tweets(tweet)
    print("stored tweet with id " + str(tweet["id"]))

    producer.send('analytics', value=tweet)
