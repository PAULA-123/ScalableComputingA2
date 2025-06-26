import json
import time
import os
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests

# ============================
# CONFIGURAÇÕES COM VARIÁVEIS DE AMBIENTE
# ============================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_merge_group")
TOPIC_HOSPITAL = os.getenv("TOPIC_HOSPITAL", "filtered_hospital")
TOPIC_SECRETARY = os.getenv("TOPIC_SECRETARY", "filtered_secretary")
API_URL = os.getenv("API_URL", "http://api:8000/merge-cep")

# ============================
# SCHEMAS
# ============================
hospital_schema = StructType([
    StructField("ID_Hospital", IntegerType(), True),
    StructField("Data", StringType(), True),
    StructField("Internado", IntegerType(), True),
    StructField("Idade", IntegerType(), True),
    StructField("Sexo", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Sintoma1", IntegerType(), True),
    StructField("Sintoma2", IntegerType(), True),
    StructField("Sintoma3", IntegerType(), True),
    StructField("Sintoma4", IntegerType(), True)
])

secretary_schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def enviar_para_api(dados):
    try:
        response = requests.post(API_URL, json=dados, timeout=10)
        response.raise_for_status()
        print(f"[API] Dados enviados com sucesso. Status: {response.status_code}")
    except Exception as e:
        print(f"[ERRO API] Falha ao enviar dados: {str(e)}")

def processar_batch(dados_hosp, dados_secr, spark):
    df_hosp = spark.createDataFrame(dados_hosp, schema=hospital_schema)
    df_secr = spark.createDataFrame(dados_secr, schema=secretary_schema)

    agg_hosp = df_hosp.groupBy("CEP").agg(
        spark_sum("Internado").alias("Total_Internados"),
        spark_sum("Idade").alias("Soma_Idade"),
        spark_sum("Sintoma1").alias("Total_Sintoma1"),
        spark_sum("Sintoma2").alias("Total_Sintoma2"),
        spark_sum("Sintoma3").alias("Total_Sintoma3"),
        spark_sum("Sintoma4").alias("Total_Sintoma4")
    )

    agg_secr = df_secr.groupBy("CEP").agg(
        spark_sum("Diagnostico").alias("Total_Diagnosticos"),
        spark_sum("Vacinado").alias("Total_Vacinados"),
        spark_sum("Escolaridade").alias("Soma_Escolaridade"),
        spark_sum("Populacao").alias("Soma_Populacao")
    )

    merged = agg_hosp.join(agg_secr, on="CEP", how="outer").fillna(0)
    return [row.asDict() for row in merged.collect()]

def main():
    print("\n=== INICIANDO TRATADOR DE MERGE ===")
    spark = SparkSession.builder \
        .appName("TratadorMerge") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe([TOPIC_HOSPITAL, TOPIC_SECRETARY])
    print(f"[KAFKA] Subscrito em: {TOPIC_HOSPITAL}, {TOPIC_SECRETARY}")

    try:
        dados_hosp, dados_secr = [], []
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERRO] Kafka: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                batch = payload.get("batch", [])

                if msg.topic() == TOPIC_HOSPITAL:
                    dados_hosp.extend(batch)
                    print(f"[HOSPITAL] +{len(batch)} registros (total: {len(dados_hosp)})")
                elif msg.topic() == TOPIC_SECRETARY:
                    dados_secr.extend(batch)
                    print(f"[SECRETARIA] +{len(batch)} registros (total: {len(dados_secr)})")

                if dados_hosp and dados_secr:
                    resultado = processar_batch(dados_hosp, dados_secr, spark)
                    enviar_para_api(resultado)
                    print(f"[MERGE] Batch enviado - {len(resultado)} CEPs consolidados")
                    dados_hosp, dados_secr = [], []
                    consumer.commit()

            except Exception as e:
                print(f"[ERRO] Processamento da mensagem: {str(e)}")

    except KeyboardInterrupt:
        print("\n=== ENCERRANDO CONSUMER ===")
    finally:
        consumer.close()
        spark.stop()
        print("[FINALIZADO] Recursos liberados")

if __name__ == "__main__":
    main()
