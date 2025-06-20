import json
import os
import time
from statistics import mean
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, mean as spark_mean
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
import requests

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_metricas_group"
SOURCE_TOPIC = "filtered_secretary"
OUTPUT_FILE = "databases_mock/resultados_metrica.json"

API_URL = "http://api:8000/metricas"

# Schema para os dados de entrada - agora com mais campos relevantes
schema = StructType([
    StructField("Vacinado", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Diagnostico", IntegerType(), True)  # Mantido para taxa de diagnóstico
])

def enviar_resultado_api(resultados):
    try:
        response = requests.post(API_URL, json=resultados)
        if response.status_code == 200:
            print("[METRICA] Resultados enviados para API com sucesso")
        else:
            print(f"[METRICA] Falha ao enviar resultados: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[METRICA] Erro ao enviar resultados para API: {e}")

def salvar_resultado(resultados):
    try:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(resultados, f, indent=4)
    except Exception as e:
        print(f"[ERRO] Falha ao salvar arquivo JSON: {e}")

def main():
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
    resultados = []
    acumulado = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[METRICA] Erro no Kafka: {msg.error()}")
                    continue

            try:
                dado = json.loads(msg.value().decode("utf-8"))
                
                print(f"[METRICA] Dado recebido do Kafka: {dado}", flush=True)
                
                # Filtra campos relevantes e converte tipos
                processed_data = {
                    "Vacinado": int(dado.get("Vacinado", 0)),
                    "Escolaridade": int(dado.get("Escolaridade", 0)),
                    "Populacao": int(dado.get("Populacao", 0)),
                    "Diagnostico": int(dado.get("Diagnostico", 0))
                }
                
                acumulado.append(processed_data)

                # Processa a cada 10 registros (ajustável conforme necessidade)
                if len(acumulado) % 10 == 0:
                    df = spark.createDataFrame(acumulado, schema=schema)
                    
                    # Calcula várias métricas relevantes
                    metrics = df.agg(
                        spark_mean(col("Vacinado")).alias("taxa_vacinacao"),
                        spark_mean(col("Escolaridade")).alias("media_escolaridade"),
                        spark_mean(col("Diagnostico")).alias("taxa_diagnostico"),
                        avg(col("Populacao")).alias("media_populacao")
                    ).collect()[0]
                    
                    resultado = {
                        "quantidade": len(acumulado),
                        "taxa_vacinacao": round(metrics["taxa_vacinacao"], 4),
                        "media_escolaridade": round(metrics["media_escolaridade"], 2),
                        "taxa_diagnostico": round(metrics["taxa_diagnostico"], 4),
                        "media_populacao": round(metrics["media_populacao"], 2)
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