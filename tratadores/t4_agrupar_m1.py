import os
import json
import time
import requests
from typing import List, Dict
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_evolucao_group")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "filtered_secretary")
API_URL = os.getenv("API_URL", "http://api:8000/evolucao-vacinacao")

BATCHES_PARA_PROCESSAR = 5  # atualiza a cada 5 batches

schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

acumulado_por_data: Dict[str, Dict[str, int]] = {}

def atualizar_acumulado(df):
    df = df.withColumn("Data", to_date(col("Data"), "yyyy-MM-dd"))
    registros = df.select("Data", "Vacinado", "Populacao").collect()

    for row in registros:
        data = row["Data"]
        if data is None:
            continue
        data_str = data.strftime("%Y-%m-%d")
        if data_str not in acumulado_por_data:
            acumulado_por_data[data_str] = {"vacinados": 0, "populacao": 0}

        acumulado_por_data[data_str]["vacinados"] += int(row["Vacinado"] or 0)
        acumulado_por_data[data_str]["populacao"] += int(row["Populacao"] or 0)

def calcular_evolucao(enviar_api=True):
    resultados: List[Dict] = []

    for data_str, valores in sorted(acumulado_por_data.items()):
        total_vacinados = valores["vacinados"]
        total_populacao = valores["populacao"]
        taxa = total_vacinados / total_populacao if total_populacao > 0 else 0.0

        resultados.append({
            "Data": data_str,
            "total_vacinados": total_vacinados,
            "total_populacao": total_populacao,
            "taxa_vacinacao": round(taxa, 4)
        })

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

    contador_bloqueios = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[EVOLUCAO][ERRO] Kafka: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                batch = payload.get("batch", [])
                if not batch:
                    continue

                df = spark.createDataFrame(batch, schema=schema)
                atualizar_acumulado(df)
                contador_bloqueios += 1

                if contador_bloqueios >= BATCHES_PARA_PROCESSAR:
                    calcular_evolucao()
                    contador_bloqueios = 0
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"❌ JSON inválido: {e}")

    except KeyboardInterrupt:
        print("🛑 Interrompido pelo usuário")
        calcular_evolucao()
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
