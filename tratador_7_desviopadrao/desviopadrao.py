import json
import time
import requests
from typing import List, Dict
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_desvio_group"
SOURCE_TOPIC = "grouped_secretary"
API_URL = "http://api:8000/desvios"

schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("media_diagnostico", FloatType(), True),
    StructField("media_vacinado", FloatType(), True),
    StructField("media_escolaridade", FloatType(), True),
    StructField("media_populacao", FloatType(), True)
])

def calcular_desvios(df, enviar_api: bool = True) -> None:
    print("\n📈 Desvios padrão das métricas:")
    try:
        # Agregação única e paralela
        resultado = df.agg(
            stddev("media_diagnostico").alias("media_diagnostico"),
            stddev("media_vacinado").alias("media_vacinado"),
            stddev("media_escolaridade").alias("media_escolaridade"),
            stddev("media_populacao").alias("media_populacao")
        ).collect()[0]

        desvios: List[Dict[str, float]] = [
            {"variavel": nome, "desvio": round(valor, 4)}
            for nome, valor in resultado.asDict().items()
        ]

        for d in desvios:
            print(f"📌 {d['variavel']}: {d['desvio']:.4f}")

        if enviar_api:
            response = requests.post(API_URL, json=desvios)
            if response.status_code == 200:
                print("✅ Desvios enviados com sucesso para a API")
            else:
                print(f"❌ Falha ao enviar para API: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"❌ Erro ao calcular ou enviar desvio padrão: {e}")

def main() -> None:
    spark = SparkSession.builder.appName("tratador_desvio").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    print(f"📦 Inscrito no tópico {SOURCE_TOPIC}")

    registros = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if registros:
                    df = spark.createDataFrame(registros, schema=schema)
                    calcular_desvios(df)
                    registros = []
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Erro Kafka: {msg.error()}")
                continue

            try:
                dado = json.loads(msg.value().decode("utf-8"))
                registros.append(dado)
            except Exception as e:
                print(f"❌ JSON inválido: {e}")

    except KeyboardInterrupt:
        print("⏹️ Interrompido pelo usuário")
        if registros:
            df = spark.createDataFrame(registros, schema=schema)
            calcular_desvios(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
