"""
Este script é responsável por consumir batches de dados dos tópicos Kafka referentes
a registros hospitalares e da secretaria de saúde, e realizar um merge inteligente
desses dados com base no CEP, utilizando Spark para agregações eficientes.

Ele trata dois cenários distintos:
1. Dados históricos: Acumulam em memória até sinal de fim, quando então são processados de uma só vez.
2. Dados normais: São processados imediatamente por lote, sem acúmulo.

O objetivo é permitir um pipeline de ingestão flexível, balanceado e escalável para diferentes fluxos.
"""

import json
import os
import time
from confluent_kafka import Consumer, KafkaError, Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, first
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests

# Configuração
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_merge_group"

# Tópicos para dados em tempo real
TOPIC_HOSPITAL = "filtered_hospital"
TOPIC_SECRETARY = "filtered_secretary"
TOPIC_SAIDA = "merge_hospital_secretary"

# Tópicos para dados  históricos
# TOPIC_HOSPITAL = "raw_hospital_h"
# TOPIC_SECRETARY = "raw_secretary_h"

OUTPUT_DIR = "/app/databases_mock"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "merge_batch.json")
API_URL = "http://api:8000/merge-cep"


spark = SparkSession.builder.appName("TratadorMerge").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define o schema para dados hospitalares
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

# Define o schema para dados da secretaria
secretary_schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True),
])


producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


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

        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                anterior = json.load(f)
                if not isinstance(anterior, list):
                    anterior = []
        else:
            anterior = []

        anterior.extend(dados)

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(anterior, f, indent=4, ensure_ascii=False)

        print(f"[ARQUIVO] Salvo {len(dados)} registros no JSON.")
        return True
    except Exception as e:
        print(f"[ERRO ARQUIVO] {str(e)}")
        return False

# Função de processamento
def processar_batch(dados_hosp, dados_secr):
    """
    Recebe listas de registros hospitalares e da secretaria,
    e realiza agregações por CEP, unificando os dados em um único DataFrame.

    Parâmetros:
        dados_hosp (list): registros do hospital
        dados_secr (list): registros da secretaria

    Retorno:
        DataFrame Spark com dados agregados e mesclados
    """
    df_hosp = spark.createDataFrame(dados_hosp, schema=hospital_schema)
    df_secr = spark.createDataFrame(dados_secr, schema=secretary_schema)

    # Agrega informações hospitalares por CEP
    agg_hosp = df_hosp.groupBy("CEP").agg(
        spark_sum("Internado").alias("total_Internados"),
        avg("Idade").alias("media_idade"),
        spark_sum("Sintoma1").alias("total_Sintoma1"),
        spark_sum("Sintoma2").alias("Total_Sintoma2"),
        spark_sum("Sintoma3").alias("Total_Sintoma3"),
        spark_sum("Sintoma4").alias("Total_Sintoma4")
    )

    # Agrega informações da secretaria por CEP
    agg_secr = df_secr.groupBy("CEP").agg(
        spark_sum("Diagnostico").alias("total_diagnosticos"),
        spark_sum("Vacinado").alias("total_vacinados"),
        avg("Escolaridade").alias("media_escolaridade"),
        first("Populacao").alias("populacao")
    )

    merged = agg_hosp.join(agg_secr, on="CEP", how="outer").fillna(0)
    return merged.toJSON().map(lambda x: json.loads(x)).collect()

# Execução principal
def main():
    print("\n=== INICIANDO TRATADOR DE MERGE ===\n")

    # Criação do consumidor Kafka
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    # Inscreve o consumidor nos tópicos
    consumer.subscribe([TOPIC_HOSPITAL, TOPIC_SECRETARY])

    # Buffers para armazenar dados históricos
    dados_hosp_historico = []
    dados_secr_historico = []
    fim_hosp = False
    fim_secr = False

    # Buffers para dados em tempo real
    dados_hosp_normal = []
    dados_secr_normal = []

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
                # Decodifica a mensagem recebida
                raw = msg.value().decode("utf-8")
                payload = json.loads(raw)

                registros = payload.get("batch", [])
                source = payload.get("source", "")
                # coloca normal por padrão
                msg_type = payload.get("type", "normal")

                # Caso seja o fim do histórico de uma fonte
                if msg_type == "fim_historico":
                    if msg.topic() == TOPIC_HOSPITAL:
                        fim_hosp = True
                        print("[HOSPITAL] Recebido fim do histórico")
                    elif msg.topic() == TOPIC_SECRETARY:
                        fim_secr = True
                        print("[SECRETARIA] Recebido fim do histórico")

                # Caso seja um lote intermediário de dados históricos
                elif msg_type == "dados":
                    if msg.topic() == TOPIC_HOSPITAL:
                        dados_hosp_historico.extend(registros)
                        # print(f"[HOSPITAL HISTÓRICO] +{len(registros)} registros (total: {len(dados_hosp_historico)})")
                    elif msg.topic() == TOPIC_SECRETARY:
                        dados_secr_historico.extend(registros)
                        # print(f"[SECRETARIA HISTÓRICO] +{len(registros)} registros (total: {len(dados_secr_historico)})")

                # Caso seja um lote de dados normais (em tempo real)
                else:
                    if msg.topic() == TOPIC_HOSPITAL:
                        dados_hosp_normal.extend(registros)
                        # print(f"[HOSPITAL] +{len(registros)} registros (total: {len(dados_hosp_normal)})")
                    elif msg.topic() == TOPIC_SECRETARY:
                        dados_secr_normal.extend(registros)
                        # print(f"[SECRETARIA] +{len(registros)} registros (total: {len(dados_secr_normal)})")

                # Quando há dados normais de ambas fontes, processa imediatamente
                if len(dados_hosp_normal) > 0 and len(dados_secr_normal) > 0:
                    json_batch = processar_batch(dados_hosp_normal, dados_secr_normal)

                    # Converte resultado para JSON com tag de source
                    # json_batch = df_merge.toJSON().map(lambda x: json.loads(x)).collect()
                    payload = json.dumps({"batch": json_batch, "source": "merge"})

                    # Envia para tópico específico de merge (crie esse tópico no seu Kafka)
                    producer.produce("grouped_merge", payload.encode("utf-8"))
                    producer.flush()
                    print("Resultado do merge enviado para tópico grouped_merge NORMAL")

                    # print(f"Merge NORMAL com {len(json_batch)} linhas")

                    # Limpa buffers e marca commit
                    dados_hosp_normal = []
                    dados_secr_normal = []
                    consumer.commit(asynchronous=False)

                # Quando os dados históricos de ambas as fontes chegaram, processa o merge final
                if fim_hosp and fim_secr:
                    df_merge = processar_batch(dados_hosp_historico, dados_secr_historico)

                    # Converte resultado para JSON com tag de source
                    json_batch = df_merge.toJSON().map(lambda x: json.loads(x)).collect()
                    payload = json.dumps({"batch": json_batch, "source": "merge"})

                    # Envia para tópico específico de merge (crie esse tópico no seu Kafka)
                    producer.produce("grouped_merge", payload.encode("utf-8"))
                    producer.flush()
                    print("Resultado do merge histórico enviado para tópico grouped_merge")

                    # print(f"Merge HISTÓRICO FINAL com {df_merge.count()} linhas")

                    # Limpa buffers e reinicia sinalizadores
                    dados_hosp_historico = []
                    dados_secr_historico = []
                    fim_hosp = False
                    fim_secr = False

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
