import os
import json
import time
import requests
from typing import List, Dict
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_evolucao_group")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "filtered_secretary")
API_URL = os.getenv("API_URL", "http://api:8000/evolucao-vacinacao")

schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def calcular_evolucao(df, enviar_api=True):
    df_agrupado = df.groupBy("Data").agg(
        spark_sum("Vacinado").alias("total_vacinados"),
        spark_sum("Populacao").alias("total_populacao")
    ).withColumn("taxa_vacinacao", col("total_vacinados") / col("total_populacao"))

    resultados: List[Dict] = df_agrupado.rdd.map(lambda r: {
        "Data": r["Data"],
        "total_vacinados": int(r["total_vacinados"]),
        "total_populacao": int(r["total_populacao"]),
        "taxa_vacinacao": round(r["taxa_vacinacao"], 4)
    }).collect()

    print("\n📈 Evolução histórica da vacinação:")
    for r in resultados:
        print(f"📆 {r['Data']}: {r['taxa_vacinacao']*100:.2f}% ({r['total_vacinados']}/{r['total_populacao']})")

    if enviar_api:
        try:
            response = requests.post(API_URL, json=resultados)
            if response.status_code == 200:
                print("✅ Resultados enviados com sucesso para a API")
            else:
                print(f"❌ Erro ao enviar: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"❌ Erro ao conectar com API: {e}")

def main():
    print(f"\n📦 [EVOLUCAO] Iniciando tratador de evolução de vacinação")

    spark = SparkSession.builder.appName("tratador_evolucao_vacinacao").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    print(f"[EVOLUCAO] Subscrito ao tópico: {SOURCE_TOPIC}")

    registros = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if registros:
                    df = spark.createDataFrame(registros, schema=schema)
                    calcular_evolucao(df)
                    registros.clear()
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[EVOLUCAO][ERRO] Kafka: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                batch = payload.get("batch", [])
                registros.extend(batch)
            except Exception as e:
                print(f"❌ JSON inválido: {e}")

    except KeyboardInterrupt:
        print("🛑 Interrompido pelo usuário")
        if registros:
            df = spark.createDataFrame(registros, schema=schema)
            calcular_evolucao(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
