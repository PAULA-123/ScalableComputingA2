import json
import time
import os
import traceback
import requests
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when
from pyspark.sql.window import Window

# ========================
# Configurações
# ========================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_alerta_group")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "filtered_oms")
API_URL = os.getenv("API_URL", "http://api:8000/alerta-obitos")

# ========================
# Setup Spark
# ========================
def setup_spark():
    return SparkSession.builder \
        .appName("TratadorAlerta") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

# ========================
# Processamento do batch
# ========================
def process_batch(batch, spark):
    df = spark.createDataFrame(batch)
    if df.isEmpty():
        print("[⚠️] DataFrame vazio recebido")
        return None

    window = Window.orderBy("Data").rowsBetween(-6, 0)
    media_obitos = df.select(mean(col("N_obitos"))).collect()[0][0] or 0

    df_final = df.withColumn("Alerta",
                    when(col("N_obitos") > media_obitos, "Vermelho").otherwise("Verde")) \
                 .withColumn("Media_Movel",
                    mean(col("N_obitos")).over(window))
    return df_final

# ========================
# Processamento da mensagem
# ========================
def process_message(msg, spark):
    payload = json.loads(msg.value().decode('utf-8'))
    print(f"[🧾] Mensagem recebida com campos: {payload.keys()}")

    if "batch" not in payload:
        print("[⚠️] Campo 'batch' ausente na mensagem.")
        return

    batch = payload["batch"]
    print(f"[🧪] Processando batch com {len(batch)} registros")

    df = process_batch(batch, spark)
    if df is None:
        return

    resultado = [row.asDict() for row in df.collect()]

    try:
        response = requests.post(API_URL, json=resultado, timeout=5)
        response.raise_for_status()
        print(f"[✅] Dados enviados com sucesso para API ({response.status_code})")
    except Exception as e:
        print(f"[❌] Erro ao enviar dados para API: {e}")

# ========================
# Main
# ========================
def main():
    print("🚨 Iniciando Tratador de Alerta")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Tópico: {SOURCE_TOPIC}")
    print(f"Enviando para: {API_URL}")

    spark = setup_spark()
    spark.sparkContext.setLogLevel("WARN")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000
    })

    consumer.subscribe([SOURCE_TOPIC])
    print("🧭 Aguardando mensagens...")

    try:
        while True:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                print("[⌛] Nenhuma mensagem nova.")
                continue

            if msg.error():
                handle_kafka_error(msg.error())
                continue

            try:
                process_message(msg, spark)
            except Exception as e:
                print(f"[🔥] Falha ao processar mensagem: {str(e)}")
                traceback.print_exc()

    except KeyboardInterrupt:
        print("\n🛑 Encerrando tratador.")
    finally:
        consumer.close()
        spark.stop()
        print("✅ Recursos liberados.")

# ========================
# Erros do Kafka
# ========================
def handle_kafka_error(error):
    if error.code() == KafkaError._PARTITION_EOF:
        return
    print(f"[⚠️ Kafka] {error.str()}")

if __name__ == "__main__":
    main()
