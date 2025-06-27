import json
import os
import time
from confluent_kafka import Consumer, KafkaError, Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, first, substring
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import requests

# ============================ CONFIGURAÇÕES ============================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_merge_group"
TOPIC_HOSPITAL = "filtered_hospital"
TOPIC_SECRETARY = "filtered_secretary"
TOPIC_HIST_HOSPITAL = "raw_hospital_h"
TOPIC_HIST_SECRETARY = "raw_secretary_h"
TOPIC_SAIDA = "merge_hospital_secretary"

OUTPUT_DIR = "/app/databases_mock"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "merge_batch.json")
HISTORIC_FILE = os.path.join(OUTPUT_DIR, "historico_agregado.json")
API_URL = "http://api:8000/merge-cep"
HISTORIC_API_URL = "http://api:8000/historico"

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

# Schemas para dados históricos
hist_hospital_schema = StructType([
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

hist_secretary_schema = StructType([
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

def verificar_api(url):
    try:
        response = requests.get(f"{url.replace('/merge-cep', '')}", timeout=5)
        return response.status_code == 200
    except:
        return False

def enviar_para_api(dados, url=API_URL):
    try:
        if not verificar_api(url):
            print(f"[API] Indisponível: {url}")
            return False
        
        response = requests.post(url, json=dados, timeout=10)
        response.raise_for_status()
        print(f"[API] Dados enviados para {url} com status {response.status_code}")
        return True
    except Exception as e:
        print(f"[ERRO API] {str(e)}")
        return False

def salvar_resultado(dados, output_file=OUTPUT_FILE):
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        anterior = []
        if os.path.exists(output_file):
            with open(output_file, "r", encoding="utf-8") as f:
                try:
                    anterior = json.load(f)
                    if not isinstance(anterior, list):
                        anterior = []
                except json.JSONDecodeError as e:
                    print(f"[ERRO ARQUIVO] JSON inválido, sobrescrevendo: {e}")
                    anterior = []

        anterior.extend(dados)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(anterior, f, indent=4, ensure_ascii=False)

        print(f"[ARQUIVO] Salvo {len(dados)} registros em {output_file}")
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

def processar_historico(dados_hosp, dados_secr):
    df_hosp = spark.createDataFrame(dados_hosp, schema=hist_hospital_schema)
    df_secr = spark.createDataFrame(dados_secr, schema=hist_secretary_schema)

    # Extrai AnoMes no formato YYYYMM
    df_hosp = df_hosp.withColumn("AnoMes", substring(col("Data"), 7, 4) + substring(col("Data"), 4, 2))
    df_secr = df_secr.withColumn("AnoMes", substring(col("Data"), 7, 4) + substring(col("Data"), 4, 2))

    # Agrega por período
    agg_hosp = df_hosp.groupBy("AnoMes").agg(
        spark_sum("Internado").alias("Total_Internados"),
        avg("Idade").alias("Media_Idade"),
        spark_sum("Sintoma1").alias("Total_Sintoma1"),
        spark_sum("Sintoma2").alias("Total_Sintoma2"),
        spark_sum("Sintoma3").alias("Total_Sintoma3"),
        spark_sum("Sintoma4").alias("Total_Sintoma4")
    )

    agg_secr = df_secr.groupBy("AnoMes").agg(
        spark_sum("Diagnostico").alias("Total_Diagnosticos"),
        spark_sum("Vacinado").alias("Total_Vacinados"),
        avg("Escolaridade").alias("Media_Escolaridade"),
        avg("Populacao").alias("Media_Populacao")
    )

    merged = agg_hosp.join(agg_secr, on="AnoMes", how="outer").fillna(0)
    result = merged.toJSON().map(lambda x: json.loads(x)).collect()
    
    # Envia para API e salva localmente
    enviar_para_api(result, HISTORIC_API_URL)
    salvar_resultado(result, HISTORIC_FILE)
    
    return result

# ============================ LOOP PRINCIPAL ============================

def main():
    print("\n=== INICIANDO TRATADOR DE MERGE ===\n")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC_HOSPITAL, TOPIC_SECRETARY, TOPIC_HIST_HOSPITAL, TOPIC_HIST_SECRETARY])
    dados_hosp, dados_secr, hist_hosp, hist_secr = [], [], [], []

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
                elif msg.topic() == TOPIC_HIST_HOSPITAL:
                    hist_hosp.extend(registros)
                    print(f"[HIST HOSPITAL] +{len(registros)} registros históricos")
                elif msg.topic() == TOPIC_HIST_SECRETARY:
                    hist_secr.extend(registros)
                    print(f"[HIST SECRETARIA] +{len(registros)} registros históricos")

                # Processa dados atuais
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

                    dados_hosp, dados_secr = [], []
                    consumer.commit()

                # Processa dados históricos quando houver suficiente
                if len(hist_hosp) > 10000 and len(hist_secr) > 10000:
                    print(f"[HISTORICO] Processando lote histórico ({len(hist_hosp)} registros hospital, {len(hist_secr)} secretaria)")
                    processar_historico(hist_hosp[:10000], hist_secr[:10000])
                    hist_hosp, hist_secr = hist_hosp[10000:], hist_secr[10000:]

            except Exception as e:
                print(f"[ERRO PROCESSAMENTO] {str(e)}")

    except KeyboardInterrupt:
        print("\n[ENCERRADO PELO USUÁRIO]")

    finally:
        # Processa qualquer dado histórico restante
        if hist_hosp and hist_secr:
            print(f"[HISTORICO] Processando último lote histórico ({len(hist_hosp)} registros)")
            processar_historico(hist_hosp, hist_secr)
        
        consumer.close()
        spark.stop()

# ============================ EXECUÇÃO ============================

if __name__ == "__main__":
    main()