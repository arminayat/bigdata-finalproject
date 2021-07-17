# -*- coding: utf-8 -*-
"""
Created on Sat Jul 17 06:58:16 2021

@authors: nilam & zaha
"""

from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timezone
from clickhouse_driver import Client
from json import loads, dumps
from datetime import date