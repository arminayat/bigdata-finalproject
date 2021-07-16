from datetime import date
from clickhouse_driver import Client

client = Client(host='localhost',port=9000)

client.execute('SHOW DATABASES')