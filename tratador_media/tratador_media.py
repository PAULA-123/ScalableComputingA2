import json
import time
import os
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when
from pyspark.sql.window import Window
import requests

# Configurações com fallback
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_alerta_group")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "filtered_oms")
OUTPUT_DIR = "/app/databases_mock"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "alerta_batch.json")
API_URL = "http://api:8000/alerta-obitos"

# Schema com tipos explícitos
schema = {
    "N_obitos": "integer",
    "Populacao": "integer", 
    "CEP": "integer",
    "N_recuperados": "integer",
    "N_vacinados": "integer",
    "Data": "string"
}

def setup_spark():
    return SparkSession.builder \
        .appName("TratadorAlerta") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def process_batch(batch, spark):
    df = spark.createDataFrame(batch)
    
    # Verificação de dados
    if df.isEmpty():
        print("[AVISO] DataFrame vazio recebido")
        return None
        
    # Cálculos
    window = Window.partitionBy("CEP").orderBy("Data").rowsBetween(-6, 0)
    media_obitos = df.select(mean(col("N_obitos"))).collect()[0][0] or 0
    
    return df.withColumn("Alerta", 
               when(col("N_obitos") > media_obitos, "Vermelho").otherwise("Verde")) \
           .withColumn("Media_Movel", 
               mean(col("N_obitos")).over(window))

def main():
    print("\n=== INICIANDO TRATADOR DE ALERTA ===")
    print(f"Conectando ao Kafka em: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Tópico: {SOURCE_TOPIC}")
    print(f"Grupo: {GROUP_ID}")
    
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
    print(f"Inscrito no tópico {SOURCE_TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            
            if msg is None:
                print("[DEBUG] Nenhuma mensagem recebida. Verificando tópico...")
                continue

            if msg.error():
                handle_kafka_error(msg.error())
                continue

            try:
                process_message(msg, spark)
                
            except Exception as e:
                print(f"[ERRO] Processamento falhou: {str(e)}")
                import traceback
                traceback.print_exc()

    except KeyboardInterrupt:
        print("\n=== ENCERRANDO ===")
    finally:
        consumer.close()
        spark.stop()
        print("Recursos liberados")

def process_message(msg, spark):
    payload = json.loads(msg.value().decode('utf-8'))
    print(f"[DEBUG] Mensagem recebida: {payload.keys()}")
    
    if "batch" not in payload:
        print("[AVISO] Mensagem sem campo 'batch'")
        return

    batch = payload["batch"]
    print(f"[PROCESSAMENTO] Batch com {len(batch)} registros")
    
    df_processed = process_batch(batch, spark)
    if df_processed is None:
        return

    resultado = [row.asDict() for row in df_processed.collect()]
    
    # Salvar arquivo
    with open(OUTPUT_FILE, "w") as f:
        json.dump(resultado, f)
    print(f"[ARQUIVO] Dados salvos em {OUTPUT_FILE}")
    
    # Enviar para API
    try:
        response = requests.post(API_URL, json=resultado, timeout=5)
        response.raise_for_status()
        print(f"[API] Dados enviados. Status: {response.status_code}")
    except Exception as e:
        print(f"[ERRO API] {str(e)}")

def handle_kafka_error(error):
    if error.code() == KafkaError._PARTITION_EOF:
        return
    print(f"[ERRO KAFKA] {error.str()}")

if __name__ == "__main__":
    main()