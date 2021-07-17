import time
from flask.helpers import make_response
import redis
from flask import Flask, jsonify, request
from json import loads, dumps
import logging


app = Flask(__name__)
r = redis.Redis(host='127.0.0.1', port=6379)


@app.route('/keywords')
def get_tweets_by_keyword():
    keywords = [x.decode().split(":")[1] for x in r.keys("keyword:*")]
    keywords_counts = {}
    for k in keywords:
        keywords_counts[k] = keywords.count(k)
    resp = make_response(jsonify(keywords_counts), 200)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


@app.route('/time-filter')
def get_tweets_by_time():
    start = [int(x) for x in request.args.get("start").split("-")]

    end = [int(x) for x in request.args.get("end").split("-")]
    date = end.copy()
    query_dates = []
    while start <= date:
        query_dates.append(":".join(["{:02d}".format(x) for x in date]))
        app.logger.info(str(date))

        date[0] = date[0] if (date[1] != 1 or date[2] !=
                              1 or date[3] != 0) else date[0] - 1
        date[1] = date[1] if (date[2] != 1 or date[3] !=
                              0) else date[1] - 1 if date[1] > 1 else 12
        date[2] = date[2] if date[3] != 0 else date[2] - \
            1 if date[2] > 1 else 30
        date[3] = date[3] - 1 if date[3] > 0 else 24

    data = []
    for d in query_dates:
        data.extend(r.lrange(f"tweets:{d}", 1, -1))
        
    resp = make_response(jsonify([loads(x.decode()) for x in data]), 200)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp