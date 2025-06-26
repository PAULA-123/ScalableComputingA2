import os
import json
import requests
from typing import List, Dict
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import StructType, StructField, IntegerType

# ============================
# CONFIGURAÇÕES COM VARIÁVEIS DE AMBIENTE
# ============================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_regressao_group")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "filtered_secretary")
API_URL = os.getenv("API_URL", "http://api:8000/regressao")

# ============================
# SCHEMA DOS DADOS
# ============================
schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Data", IntegerType(), True)
])

def enviar_resultado_api(resultados: List[Dict]) -> None:
    try:
        response = requests.post(API_URL, json=resultados)
        if response.status_code == 200:
            print("[REGRESSAO] Resultados enviados para API com sucesso")
        else:
            print(f"[REGRESSAO] Falha ao enviar resultados: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[REGRESSAO] Erro ao enviar resultados para API: {e}")

def calcular_regressao(df) -> List[Dict]:
    assembler = VectorAssembler(
        inputCols=["Vacinado", "Escolaridade", "Populacao"],
        outputCol="features"
    )
    df_com_features = assembler.transform(df).select("features", "Diagnostico")

    lr = LinearRegression(featuresCol="features", labelCol="Diagnostico")
    model = lr.fit(df_com_features)
    coeficientes = model.coefficients.toArray()

    return [
        {"variavel": "Vacinado", "beta": round(coeficientes[0], 4)},
        {"variavel": "Escolaridade", "beta": round(coeficientes[1], 4)},
        {"variavel": "Populacao", "beta": round(coeficientes[2], 4)}
    ]

def main():
    print("[REGRESSAO] Iniciando tratador de regressão com Spark")
    spark = SparkSession.builder.appName("tratador_regressao").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    registros = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if registros:
                    df = spark.createDataFrame(registros, schema=schema)
                    resultado = calcular_regressao(df)
                    enviar_resultado_api(resultado)
                    registros = []
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[REGRESSAO] Erro Kafka: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                batch = payload.get("batch")
                if batch:
                    registros.extend(batch)
                else:
                    registros.append(payload)

            except Exception as e:
                print(f"[REGRESSAO] JSON inválido: {e}")

    except KeyboardInterrupt:
        print("[REGRESSAO] Interrompido pelo usuário")
    finally:
        consumer.close()
        spark.stop()
        print("[REGRESSAO] Tratador finalizado")

if __name__ == "__main__":
    main()
