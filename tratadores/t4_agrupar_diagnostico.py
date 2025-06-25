import os
import json
import time
import requests
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, avg
from typing import List, Dict

# Configuração Kafka e API
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_agrupamento_group")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "filtered_secretary")
API_URL = os.getenv("API_URL", "http://api:8000/agrupamento")

# Schema esperado após limpeza
schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def calcular_media_por_cep(df, enviar_api=True):
    agrupado = df.groupBy("CEP").agg(
        avg("Diagnostico").alias("media_diagnostico")
    )

    resultados: List[Dict] = agrupado.rdd.map(lambda r: {
        "CEP": int(r["CEP"]),
        "media_diagnostico": round(r["media_diagnostico"], 4)
    }).collect()

    print("\n📊 Média de Diagnósticos por CEP:")
    for r in resultados:
        print(f"🏙️ CEP {r['CEP']}: média {r['media_diagnostico']}")

    if enviar_api and resultados:
        try:
            response = requests.post(API_URL, json=resultados)
            if response.status_code == 200:
                print("✅ Resultados enviados com sucesso para a API")
            else:
                print(f"❌ Erro ao enviar: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"❌ Falha ao conectar com API: {e}")

def main():
    print("🚀 Iniciando tratador T4 - Média por CEP")

    spark = SparkSession.builder.appName("tratador_media_diagnostico").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe([SOURCE_TOPIC])
    print(f"🛰️ Subscrito ao tópico: {SOURCE_TOPIC}")

    registros = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if registros:
                    df = spark.createDataFrame(registros, schema=schema)
                    calcular_media_por_cep(df)
                    registros = []
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Erro Kafka: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                registros.extend(payload.get("batch", []))
            except Exception as e:
                print(f"⚠️ Erro ao processar mensagem: {e}")

    except KeyboardInterrupt:
        print("🛑 Interrompido manualmente.")
        if registros:
            df = spark.createDataFrame(registros, schema=schema)
            calcular_media_por_cep(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
