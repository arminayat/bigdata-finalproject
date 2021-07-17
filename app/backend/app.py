import time
from flask.helpers import make_response
import redis
from flask import Flask, jsonify, request
from json import loads, dumps
import logging


app = Flask(__name__)
r = redis.Redis(host='127.0.0.1', port=6379)