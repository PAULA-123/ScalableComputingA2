import json
import os
import requests
from typing import List, Dict
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

# ============================
# CONFIGURAÇÕES COM VARIÁVEIS DE AMBIENTE
# ============================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_desvio_group")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "grouped_secretary")
API_URL = os.getenv("API_URL", "http://api:8000/desvios")

# ============================
# SCHEMA
# ============================
schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("media_diagnostico", FloatType(), True),
    StructField("media_vacinado", FloatType(), True),
    StructField("media_escolaridade", FloatType(), True),
    StructField("media_populacao", FloatType(), True)
])

def calcular_desvios(df) -> List[Dict[str, float]]:
    print("\n📈 Calculando desvios padrão das métricas")
    resultado = df.agg(
        stddev("media_diagnostico").alias("media_diagnostico"),
        stddev("media_vacinado").alias("media_vacinado"),
        stddev("media_escolaridade").alias("media_escolaridade"),
        stddev("media_populacao").alias("media_populacao")
    ).collect()[0]

    return [
        {"variavel": nome, "desvio": round(valor or 0.0, 4)}
        for nome, valor in resultado.asDict().items()
    ]

def enviar_para_api(dados):
    try:
        response = requests.post(API_URL, json=dados, timeout=10)
        response.raise_for_status()
        print(f"✅ Desvios enviados para a API ({len(dados)} métricas)")
    except Exception as e:
        print(f"❌ Erro ao enviar para API: {e}")

def main():
    print("\n=== INICIANDO TRATADOR DE DESVIO ===")
    spark = SparkSession.builder.appName("tratador_desvio").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    print(f"📦 Subscrito no tópico: {SOURCE_TOPIC}")

    registros = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if registros:
                    df = spark.createDataFrame(registros, schema=schema)
                    desvios = calcular_desvios(df)
                    enviar_para_api(desvios)
                    registros = []
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Erro Kafka: {msg.error()}")
                continue

            try:
                raw = msg.value().decode("utf-8")
                payload = json.loads(raw)

                if isinstance(payload, list):
                    registros.extend(payload)
                    print(f"📥 Recebidos {len(payload)} registros (total: {len(registros)})")
                elif isinstance(payload, dict):
                    if "batch" in payload and isinstance(payload["batch"], list):
                        registros.extend(payload["batch"])
                        print(f"📥 Recebidos {len(payload['batch'])} registros via 'batch' (total: {len(registros)})")
                    else:
                        registros.append(payload)
                        print(f"📥 Recebido 1 registro (total: {len(registros)})")
                else:
                    print("⚠️ Payload inesperado:", payload)

            except Exception as e:
                print(f"❌ JSON inválido: {e}")

    except KeyboardInterrupt:
        print("⏹️ Encerrado pelo usuário")
        if registros:
            df = spark.createDataFrame(registros, schema=schema)
            desvios = calcular_desvios(df)
            enviar_para_api(desvios)

    finally:
        consumer.close()
        spark.stop()
        print("[FINALIZADO] Recursos liberados")

if __name__ == "__main__":
    main()
