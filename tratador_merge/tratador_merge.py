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
import time
import os
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Configuração
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_merge_group"

# Tópicos para dados em tempo real
TOPIC_HOSPITAL_B = "raw_hospital_b"
TOPIC_SECRETARY_B = "raw_secretary_b"

# Tópicos para dados  históricos
# TOPIC_HOSPITAL_B = "raw_hospital_h"
# TOPIC_SECRETARY_B = "raw_secretary_h"

spark = SparkSession.builder.appName("tratador_merge_batch").getOrCreate()
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
    StructField("Sintoma4", IntegerType(), True)
])

# Define o schema para dados da secretaria
secretary_schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

# Funçaõ de processamento
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
        spark_sum("Internado").alias("Total_Internados"),
        spark_sum("Idade").alias("Soma_Idade"),
        spark_sum("Sintoma1").alias("Total_Sintoma1"),
        spark_sum("Sintoma2").alias("Total_Sintoma2"),
        spark_sum("Sintoma3").alias("Total_Sintoma3"),
        spark_sum("Sintoma4").alias("Total_Sintoma4")
    )

    # Agrega informações da secretaria por CEP
    agg_secr = df_secr.groupBy("CEP").agg(
        spark_sum("Diagnostico").alias("Total_Diagnosticos"),
        spark_sum("Vacinado").alias("Total_Vacinados"),
        spark_sum("Escolaridade").alias("Soma_Escolaridade"),
        spark_sum("Populacao").alias("Soma_Populacao")
    )

    # Faz o merge das duas agregações, mantendo todos os CEPs
    merged = agg_hosp.join(agg_secr, on="CEP", how="outer").fillna(0)
    return merged

def main():
    print("\n" + "="*50)
    print(" INICIANDO TRATADOR DE MERGE POR BATCH ")
    print("="*50 + "\n")

    # Criação do consumidor Kafka
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    # Inscreve o consumidor nos tópicos
    consumer.subscribe([TOPIC_HOSPITAL_B, TOPIC_SECRETARY_B])
    print("Inscrito nos tópicos:", TOPIC_HOSPITAL_B, TOPIC_SECRETARY_B)

    # Buffers para armazenar dados históricos
    dados_hosp_historico = []
    dados_secr_historico = []
    fim_hosp = False
    fim_secr = False

    # Buffers para dados em tempo real
    dados_hosp_normal = []
    dados_secr_normal = []

    msg_count = 0
    start_time = time.time()

    try:
        while True:
            # Tenta buscar uma nova mensagem Kafka
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Erro no consumidor: {msg.error()}")
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
                    if msg.topic() == TOPIC_HOSPITAL_B:
                        fim_hosp = True
                        print("[HOSPITAL] Recebido fim do histórico")
                    elif msg.topic() == TOPIC_SECRETARY_B:
                        fim_secr = True
                        print("[SECRETARIA] Recebido fim do histórico")

                # Caso seja um lote intermediário de dados históricos
                elif msg_type == "dados":
                    if msg.topic() == TOPIC_HOSPITAL_B:
                        dados_hosp_historico.extend(registros)
                        print(f"[HOSPITAL HISTÓRICO] +{len(registros)} registros (total: {len(dados_hosp_historico)})")
                    elif msg.topic() == TOPIC_SECRETARY_B:
                        dados_secr_historico.extend(registros)
                        print(f"[SECRETARIA HISTÓRICO] +{len(registros)} registros (total: {len(dados_secr_historico)})")

                # Caso seja um lote de dados normais (em tempo real)
                else:
                    if msg.topic() == TOPIC_HOSPITAL_B:
                        dados_hosp_normal.extend(registros)
                        print(f"[HOSPITAL] +{len(registros)} registros (total: {len(dados_hosp_normal)})")
                    elif msg.topic() == TOPIC_SECRETARY_B:
                        dados_secr_normal.extend(registros)
                        print(f"[SECRETARIA] +{len(registros)} registros (total: {len(dados_secr_normal)})")

                # Quando há dados normais de ambas fontes, processa imediatamente
                if len(dados_hosp_normal) > 0 and len(dados_secr_normal) > 0:
                    msg_count += 1
                    df_merge = processar_batch(dados_hosp_normal, dados_secr_normal)

                    print(f"Merge NORMAL com {df_merge.count()} linhas")

                    # Limpa buffers e marca commit
                    dados_hosp_normal = []
                    dados_secr_normal = []
                    consumer.commit(asynchronous=False)

                # Quando os dados históricos de ambas as fontes chegaram, processa o merge final
                if fim_hosp and fim_secr:
                    df_merge = processar_batch(dados_hosp_historico, dados_secr_historico)

                    print(f"Merge HISTÓRICO FINAL com {df_merge.count()} linhas")

                    # Limpa buffers e reinicia sinalizadores
                    dados_hosp_historico = []
                    dados_secr_historico = []
                    fim_hosp = False
                    fim_secr = False
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"Erro ao processar mensagem: {e}")

    except KeyboardInterrupt:
        print("\n" + "="*50)
        print(" INTERRUPÇÃO SOLICITADA - ENCERRANDO CONSUMER ")
        print(f"Total de merges processados: {msg_count}")
        print(f"Tempo de execução: {time.time() - start_time:.2f} segundos")
        print("="*50)
    finally:
        consumer.close()

# Executa o consumidor quando chamado diretamente
if __name__ == "__main__":
    main()
