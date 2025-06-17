import redis
import json
import time
from pyspark.sql import SparkSession
from tratadores import limpeza, remover_colunas_vazias

REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_CHANNEL = "oms"
BATCH_SIZE = 20000

spark = SparkSession.builder.appName("ETL_Limpeza").getOrCreate()
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
pubsub = r.pubsub()
pubsub.subscribe(REDIS_CHANNEL)

records = []
print(f"Consumindo mensagens do canal '{REDIS_CHANNEL}'...")

while len(records) < BATCH_SIZE:
    message = pubsub.get_message()
    if message and message['type'] == 'message':
        data = json.loads(message['data'])
        records.append(data)
    else:
        time.sleep(0.01)

df = spark.createDataFrame(records)

# Pipeline de tratadores
df = limpeza(df)
df = remover_colunas_vazias(df)

df.show(5)
print(f"Total após limpeza: {df.count()} registros, {len(df.columns)} colunas")
