import json
import os
import time
import requests
from typing import List, Dict
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, mean as spark_mean
from pyspark.sql.types import StructType, StructField, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_metricas_group"
SOURCE_TOPIC = "filtered_secretary"
OUTPUT_FILE = "databases_mock/resultados_metrica.json"
API_URL = "http://api:8000/metricas"

schema = StructType([
    StructField("Vacinado", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Diagnostico", IntegerType(), True)
])

def enviar_resultado_api(resultados: List[Dict]) -> None:
    try:
        response = requests.post(API_URL, json=resultados)
        if response.status_code == 200:
            print("[METRICA] Resultados enviados para API com sucesso")
        else:
            print(f"[METRICA] Falha ao enviar resultados: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[METRICA] Erro ao enviar resultados para API: {e}")

def salvar_resultado(resultados: List[Dict]) -> None:
    try:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(resultados, f, indent=4)
    except Exception as e:
        print(f"[ERRO] Falha ao salvar arquivo JSON: {e}")

def main() -> None:
    print("[METRICA] Iniciando tratador de métricas com Spark")
    spark = SparkSession.builder.appName("tratador_metricas").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    resultados: List[Dict] = []
    acumulado: List[Dict] = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[METRICA] Erro no Kafka: {msg.error()}")
                continue

            try:
                dado = json.loads(msg.value().decode("utf-8"))
                print(f"[METRICA] Dado recebido do Kafka: {dado}", flush=True)

                processed_data = {
                    "Vacinado": int(dado.get("Vacinado", 0)),
                    "Escolaridade": int(dado.get("Escolaridade", 0)),
                    "Populacao": int(dado.get("Populacao", 0)),
                    "Diagnostico": int(dado.get("Diagnostico", 0))
                }
                acumulado.append(processed_data)

                if len(acumulado) % 10 == 0:
                    df = spark.createDataFrame(acumulado, schema=schema)
                    metrics_row = df.agg(
                        spark_mean("Vacinado").alias("taxa_vacinacao"),
                        spark_mean("Escolaridade").alias("media_escolaridade"),
                        spark_mean("Diagnostico").alias("taxa_diagnostico"),
                        avg("Populacao").alias("media_populacao")
                    ).first()

                    resultado = {
                        "quantidade": len(acumulado),
                        "taxa_vacinacao": round(metrics_row["taxa_vacinacao"], 4),
                        "media_escolaridade": round(metrics_row["media_escolaridade"], 2),
                        "taxa_diagnostico": round(metrics_row["taxa_diagnostico"], 4),
                        "media_populacao": round(metrics_row["media_populacao"], 2)
                    }

                    resultados.append(resultado)
                    print(f"[METRICA] Resultado #{len(resultados)}: {resultado}")

                    salvar_resultado(resultados)
                    enviar_resultado_api(resultados)

                consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"[METRICA] Erro ao processar mensagem: {e}")

    except KeyboardInterrupt:
        print("\n[METRICA] Interrompido pelo usuário")
    finally:
        consumer.close()
        spark.stop()
        print("[METRICA] Tratador finalizado")

if __name__ == "__main__":
    main()
