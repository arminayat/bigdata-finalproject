import time
from flask.helpers import make_response
import redis
from flask import Flask, jsonify, request
from json import loads, dumps
import logging


app = Flask(__name__)
r = redis.Redis(host='127.0.0.1', port=6379)


@app.route('/bd-api/keywords')
def get_tweets_by_keyword():
    keywords = [x.decode().split(":")[1] for x in r.keys("keyword:*")]
    keywords_counts = {}
    for k in keywords:
        keywords_counts[k] = keywords.count(k)
    resp = make_response(jsonify(keywords_counts), 200)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp