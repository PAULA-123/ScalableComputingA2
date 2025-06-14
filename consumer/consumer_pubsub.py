# consumer/consumer_pubsub.py
import redis
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestConsumer").getOrCreate()
r = redis.Redis(host="redis", port=6379)
pubsub = r.pubsub()
pubsub.subscribe("dados-saude")

print("Consumidor Spark escutando...")

for message in pubsub.listen():
    if message["type"] == "message":
        data = json.loads(message["data"])
        df = spark.createDataFrame([data])
        df.show()
