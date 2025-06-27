import json
import os
import time
from confluent_kafka import Consumer, KafkaError, Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, first
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests

# ============================ CONFIGURAÇÕES ============================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_merge_group"
TOPIC_HOSPITAL = "filtered_hospital"
TOPIC_SECRETARY = "filtered_secretary"
TOPIC_SAIDA = "merge_hospital_secretary"

OUTPUT_DIR = "/app/databases_mock"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "merge_batch.json")
API_URL = "http://api:8000/merge-cep"

# ============================ SPARK SESSION ============================

spark = SparkSession.builder.appName("TratadorMerge").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ============================ SCHEMAS ============================

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
    StructField("Sintoma4", IntegerType(), True),
])

secretary_schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True),
])

# ============================ PRODUTOR ============================

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

# ============================ UTILITÁRIOS ============================

def verificar_api():
    try:
        response = requests.get(f"{API_URL.replace('/merge-cep', '')}", timeout=5)
        return response.status_code == 200
    except:
        return False

def enviar_para_api(dados):
    try:
        if not verificar_api():
            print("[API] Indisponível")
            return False
        response = requests.post(API_URL, json=dados, timeout=10)
        response.raise_for_status()
        print(f"[API] Dados enviados com status {response.status_code}")
        return True
    except Exception as e:
        print(f"[ERRO API] {str(e)}")
        return False

def salvar_resultado(dados):
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        anterior = []
        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                try:
                    anterior = json.load(f)
                    if not isinstance(anterior, list):
                        anterior = []
                except json.JSONDecodeError as e:
                    print(f"[ERRO ARQUIVO] JSON inválido, sobrescrevendo: {e}")
                    anterior = []

        anterior.extend(dados)

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(anterior, f, indent=4, ensure_ascii=False)

        print(f"[ARQUIVO] Salvo {len(dados)} registros no JSON.")
        return True
    except Exception as e:
        print(f"[ERRO ARQUIVO] {str(e)}")
        return False

def processar_batch(dados_hosp, dados_secr):
    df_hosp = spark.createDataFrame(dados_hosp, schema=hospital_schema)
    df_secr = spark.createDataFrame(dados_secr, schema=secretary_schema)

    agg_hosp = df_hosp.groupBy("CEP").agg(
        spark_sum("Internado").alias("Total_Internados"),
        avg("Idade").alias("Media_Idade"),
        spark_sum("Sintoma1").alias("Total_Sintoma1"),
        spark_sum("Sintoma2").alias("Total_Sintoma2"),
        spark_sum("Sintoma3").alias("Total_Sintoma3"),
        spark_sum("Sintoma4").alias("Total_Sintoma4")
    )

    agg_secr = df_secr.groupBy("CEP").agg(
        spark_sum("Diagnostico").alias("Total_Diagnosticos"),
        spark_sum("Vacinado").alias("Total_Vacinados"),
        avg("Escolaridade").alias("Media_Escolaridade"),
        first("Populacao").alias("Populacao")
    )

    merged = agg_hosp.join(agg_secr, on="CEP", how="outer").fillna(0)
    return merged.toJSON().map(lambda x: json.loads(x)).collect()

# ============================ LOOP PRINCIPAL ============================

def main():
    print("\n=== INICIANDO TRATADOR DE MERGE ===\n")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC_HOSPITAL, TOPIC_SECRETARY])
    dados_hosp, dados_secr = [], []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[KAFKA] Erro: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                registros = payload.get("batch", [])

                if msg.topic() == TOPIC_HOSPITAL:
                    dados_hosp.extend(registros)
                    print(f"[HOSPITAL] +{len(registros)} registros")
                elif msg.topic() == TOPIC_SECRETARY:
                    dados_secr.extend(registros)
                    print(f"[SECRETARIA] +{len(registros)} registros")

                if dados_hosp and dados_secr:
                    resultado = processar_batch(dados_hosp, dados_secr)
                    salvar_resultado(resultado)
                    enviar_para_api(resultado)

                    payload_kafka = json.dumps({
                        "batch": resultado,
                        "source": "merge"
                    })

                    producer.produce(TOPIC_SAIDA, value=payload_kafka.encode("utf-8"))
                    producer.flush()
                    print(f"[KAFKA] Batch publicado em {TOPIC_SAIDA}")

                    dados_hosp, dados_secr = []
                    consumer.commit()

            except Exception as e:
                print(f"[ERRO PROCESSAMENTO] {str(e)}")

    except KeyboardInterrupt:
        print("\n[ENCERRADO PELO USUÁRIO]")

    finally:
        consumer.close()
        spark.stop()

# ============================ EXECUÇÃO ============================

if __name__ == "__main__":
    main()
