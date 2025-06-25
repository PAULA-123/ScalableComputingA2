import os
import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Configurações
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_agrupador_cep")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "filtered_secretary")
DEST_TOPIC = os.getenv("DEST_TOPIC", "grouped_secretary")

schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def processar(df, producer):
    df_grouped = df.groupBy("CEP").agg(
        avg("Diagnostico").alias("media_diagnostico"),
        avg("Vacinado").alias("media_vacinado"),
        avg("Escolaridade").alias("media_escolaridade"),
        avg("Populacao").alias("media_populacao")
    )

    resultados = df_grouped.rdd.map(lambda row: json.dumps({
        "CEP": row["CEP"],
        "media_diagnostico": round(row["media_diagnostico"], 4),
        "media_vacinado": round(row["media_vacinado"], 4),
        "media_escolaridade": round(row["media_escolaridade"], 4),
        "media_populacao": round(row["media_populacao"], 2)
    })).collect()

    for r in resultados:
        producer.produce(DEST_TOPIC, r.encode("utf-8"))
    producer.flush()

    print(f"✅ {len(resultados)} agrupamentos enviados para '{DEST_TOPIC}'")

def main():
    print(f"📦 [AGRUPADOR] Iniciando: {SOURCE_TOPIC} → {DEST_TOPIC}")

    spark = SparkSession.builder.appName("tratador_agrupador_cep").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([SOURCE_TOPIC])

    registros = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if registros:
                    df = spark.createDataFrame(registros, schema=schema)
                    processar(df, producer)
                    registros = []
                    consumer.commit()
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Kafka Error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                registros.extend(payload.get("batch", []))
            except Exception as e:
                print(f"❌ JSON inválido: {e}")

    except KeyboardInterrupt:
        print("🛑 Encerrando...")
        if registros:
            df = spark.createDataFrame(registros, schema=schema)
            processar(df, producer)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
