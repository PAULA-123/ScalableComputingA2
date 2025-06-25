import os
import json
import time
import requests
from typing import List, Dict
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_evolucao_diagnostico_group")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "filtered_secretary")
API_URL = os.getenv("API_URL", "http://api:8000/evolucao-diagnostico")

schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def calcular_diagnosticos(df, enviar_api=True):
    df = df.withColumn("Data", to_date(col("Data"), "yyyy-MM-dd"))

    df_agrupado = df.groupBy("Data").agg(
        spark_sum("Diagnostico").alias("total_diagnosticos")
    )

    resultados: List[Dict] = df_agrupado.rdd.map(lambda r: {
        "Data": r["Data"].strftime("%Y-%m-%d") if r["Data"] else None,
        "total_diagnosticos": int(r["total_diagnosticos"])
    }).collect()

    print("\n📊 Evolução histórica de diagnosticados:")
    for r in resultados:
        print(f"📆 {r['Data']}: {r['total_diagnosticos']} diagnosticados")

    if enviar_api:
        try:
            response = requests.post(API_URL, json=resultados)
            if response.status_code == 200:
                print("✅ Resultados enviados para a API com sucesso")
            else:
                print(f"❌ Erro ao enviar para API: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"❌ Erro de conexão com API: {e}")

def main():
    print(f"\n📦 [DIAG] Iniciando tratador de evolução de diagnosticados")

    spark = SparkSession.builder.appName("tratador_evolucao_diagnostico").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    print(f"[DIAG] Subscrito ao tópico: {SOURCE_TOPIC}")

    registros = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[DIAG][ERRO] Kafka: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                batch = payload.get("batch", [])
                registros.extend(batch)

                if len(registros) >= 500:  # processa a cada 500 registros
                    df = spark.createDataFrame(registros, schema=schema)
                    calcular_diagnosticos(df)
                    registros.clear()
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"❌ JSON inválido: {e}")

    except KeyboardInterrupt:
        print("🛑 Interrompido pelo usuário")
        if registros:
            df = spark.createDataFrame(registros, schema=schema)
            calcular_diagnosticos(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
