WE FOLLOW THE STEPS IN BELOW LINK :

[SuperSet link](https://medium.com/geekculture/run-apache-superset-locally-in-10-minutes-30bc70ed808c)

- docker run -d -p 8080:8088 --name data_diving apache/superset

- docker exec -it data_diving superset fab create-admin --username  data_diving --firstname Admin --lastname Admin --email admin@superset.com --password data_diving

- docker exec -it data_diving superset db upgrade

- docker exec -it data_diving superset load_examples

- docker exec -it data_diving superset init

- pip install clickhouse-sqlalchemy


