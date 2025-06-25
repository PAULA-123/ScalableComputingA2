import json
import time
import os
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests

# Configurações
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_alerta_group"
SOURCE_TOPIC = "filtered_oms"  # Consome dados filtrados
OUTPUT_DIR = "/app/databases_mock"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "alerta_batch.json")
API_URL = "http://api:8000/alerta-obitos"

# Schema para dados OMS filtrados
schema = StructType([
    StructField("N_obitos", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("N_recuperados", IntegerType(), True),
    StructField("N_vacinados", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def enviar_para_api(dados):
    try:
        response = requests.post(API_URL, json=dados)
        response.raise_for_status()
        print(f"[ALERTA] Dados enviados para API. Status: {response.status_code}")
    except Exception as e:
        print(f"[ALERTA] Erro na API: {str(e)}")

def salvar_resultado(dados):
    try:
        # Cria diretório se não existir
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR, mode=0o777, exist_ok=True)
        
        # Escreve arquivo temporário primeiro
        temp_file = OUTPUT_FILE + ".tmp"
        with open(temp_file, "w") as f:
            json.dump(dados, f, indent=4)
            f.flush()
            os.fsync(f.fileno())
        
        # Renomeia para substituir o arquivo original atomicamente
        os.replace(temp_file, OUTPUT_FILE)
        
        print(f"[ALERTA] Dados salvos em {OUTPUT_FILE}")
    except Exception as e:
        print(f"[ALERTA] Erro ao salvar: {str(e)}")

def calcular_alertas(df):
    media_obitos = df.select(mean(col("N_obitos"))).collect()[0][0] or 0
    return df.withColumn(
        "Alerta",
        when(col("N_obitos") > media_obitos, "Vermelho").otherwise("Verde")
    ).withColumn(
        "Media_Movel",
        mean(col("N_obitos")).over(Window.orderBy("Data").rowsBetween(-6, 0))
    )

def main():
    print("\n=== INICIANDO TRATADOR DE ALERTA (BATCH) ===")
    spark = SparkSession.builder \
        .appName("TratadorAlerta") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    print(f"🔍 Inscrito no tópico: {SOURCE_TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Erro no consumidor: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                batch = payload.get("batch", [])
                
                if not batch:
                    continue

                df = spark.createDataFrame(batch, schema=schema)
                df_alertas = calcular_alertas(df)
                
                resultado = [row.asDict() for row in df_alertas.collect()]
                salvar_resultado(resultado)
                enviar_para_api(resultado)
                print(f"[ALERTA] Batch processado - {len(resultado)} registros")
                consumer.commit()

            except Exception as e:
                print(f"Erro no processamento: {str(e)}")

    except KeyboardInterrupt:
        print("\n=== FINALIZANDO CONSUMER ===")
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()