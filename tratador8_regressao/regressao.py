import json
import os
import requests
from typing import List, Dict
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

# Configurações
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_metricas_group"
SOURCE_TOPIC = "grouped_merge"
OUTPUT_FILE = "databases_mock/resultados_metrica.json"
API_URL = "http://api:8000/metricas"

# Schema compatível com dados do merge
schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("Total_Vacinados", IntegerType(), True),
    StructField("Media_escolaridade", DoubleType(), True)
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
    spark = SparkSession.builder.appName("tratador_metricas_merge").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    resultados: List[Dict] = []

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
                payload = json.loads(msg.value().decode("utf-8"))
                batch = payload.get("batch", [])
                print(f"[METRICA] Batch recebido com {len(batch)} registros")

                if not batch:
                    continue

                df = spark.read.schema(schema).json(
                    spark.sparkContext.parallelize([json.dumps(row) for row in batch])
                )

                # Renomeia para facilitar
                df = df.withColumnRenamed("Media_escolaridade", "x").withColumnRenamed("Total_Vacinados", "y")

                # Estatísticas necessárias
                stats = df.withColumn("x2", col("x") * col("x")) \
                          .withColumn("xy", col("x") * col("y")) \
                          .agg(
                              avg("x").alias("x_bar"),
                              avg("y").alias("y_bar"),
                              avg("x2").alias("x2_bar"),
                              avg("xy").alias("xy_bar")
                          ).first()

                x_bar = stats["x_bar"]
                y_bar = stats["y_bar"]
                x2_bar = stats["x2_bar"]
                xy_bar = stats["xy_bar"]

                cov_xy = xy_bar - x_bar * y_bar
                var_x = x2_bar - x_bar ** 2

                beta1 = cov_xy / var_x if var_x != 0 else 0
                beta0 = y_bar - beta1 * x_bar

                resultado = {
                    "quantidade": df.count(),
                    "beta0": round(beta0, 4),
                    "beta1": round(beta1, 4)
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
