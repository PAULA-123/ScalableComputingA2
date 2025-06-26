"""
Este script é responsável por consumir batches de dados da secretaria de saúde e hospitais,
provenientes do Kafka e realizar agrupamentos por CEP separadamente, usando Apache Spark.

Ele trata dois tipos de dados:
1. Dados históricos: São acumulados em memória até a chegada do sinal de fim, e então processados.
2. Dados normais: São processados imediatamente, sem acúmulo.

O agrupamento é feito separadamente para cada fonte (secretaria e hospital), mantendo
flexibilidade e paralelismo entre as fontes de dados.
"""

import os
import json
import requests
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, first, sum as _sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Configurações Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_agrupamento_2fontes"

# Tópicos de entrada Kafka (históricos e normais)
TOPIC_SECRETARY_H = "raw_secretary_h"
TOPIC_HOSPITAL_H = "raw_hospital_h"
TOPIC_SECRETARY_B = "raw_secretary_b"
TOPIC_HOSPITAL_B = "raw_hospital_b"

# Tópicos de saída Kafka
DEST_TOPIC_SECRETARY = "grouped_secretary"
DEST_TOPIC_HOSPITAL = "grouped_hospital"

# Endpoints das APIs
API_SECRETARY = "http://api:8000/agrupamento"
API_HOSPITAL = "http://api:8000/agrupamento_hospital"

# Schemas dos dados esperados
schema_secretary = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

schema_hospital = StructType([
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

# Agrupa os dados por CEP com diferentes lógicas para cada origem
def agrupar_por_cep(df, origem):
    if origem == "secretary":
        return df.groupBy("CEP").agg(
            _sum("Diagnostico").alias("total_diagnostico"),
            avg("Escolaridade").alias("media_escolaridade"),
            first("Populacao").alias("media_populacao"),
            _sum("Vacinado").alias("total_vacinados")
        )
    elif origem == "hospital":
        return df.groupBy("CEP", "Data").agg(
            _sum("Internado").alias("total_internados"),
            _sum("Idade").alias("soma_idade"),
            _sum("Sintoma1").alias("total_sintoma1"),
            _sum("Sintoma2").alias("total_sintoma2"),
            _sum("Sintoma3").alias("total_sintoma3"),
            _sum("Sintoma4").alias("total_sintoma4")
        )

# Realiza o processamento: agrupamento por CEP, envio ao Kafka e à API
def processar(dados, origem, spark, producer):
    # Define o schema conforme a origem dos dados
    schema = schema_secretary if origem == "secretary" else schema_hospital
    df = spark.createDataFrame(dados, schema=schema)

    # Agrupa os dados por CEP
    df_grouped = agrupar_por_cep(df, origem)

    # Converte o DataFrame agrupado para JSON para envio
    json_batch = df_grouped.toJSON().map(lambda x: json.loads(x)).collect()

    # Envia resultado para o tópico Kafka correspondente
    if json_batch:
        destino = DEST_TOPIC_SECRETARY if origem == "secretary" else DEST_TOPIC_HOSPITAL
        producer.produce(destino, json.dumps({"batch": json_batch}).encode("utf-8"))
        producer.flush()
        print(f"Enviado para Kafka: {destino} ({len(json_batch)} grupos)")

    # Envia o mesmo resultado para a API REST correspondente
    api_url = API_SECRETARY if origem == "secretary" else API_HOSPITAL
    try:
        response = requests.post(api_url, json=json_batch)
        if response.status_code == 200:
            print(f"Enviado para API {origem} com sucesso ({len(json_batch)} grupos)")
        else:
            print(f"Erro API {origem}: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Falha na API {origem}: {e}")

    return len(json_batch), len(dados)

# Loop principal de consumo do Kafka
def main():
    # Inicializa Spark
    spark = SparkSession.builder.appName("tratador_agrupamento_2fontes").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Inicializa Kafka consumer e producer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([TOPIC_SECRETARY_H,TOPIC_HOSPITAL_H,TOPIC_SECRETARY_B,TOPIC_HOSPITAL_B])

    # Buffers para acumular os dados históricos de cada origem
    buffer_secretary = []
    buffer_hospital = []
    fim_secretary = False
    fim_hospital = False

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {msg.error()}")
                continue

            try:
                # Decodifica a mensagem Kafka
                conteudo = json.loads(msg.value().decode("utf-8"))
                tipo = conteudo.get("type", "normal")
                dados = conteudo.get("batch", [])
                origem = conteudo.get("source", "")

                # Decide o tratamento com base na origem e tipo de mensagem
                if origem == "secretary":
                    if tipo == "dados":
                        buffer_secretary.extend(dados)
                    elif tipo == "fim_historico":
                        fim_secretary = True
                    elif tipo == "normal":
                        print(f"[Secretaria] Batch normal com {len(dados)} registros")
                        processar(dados, "secretary", spark, producer)
                        consumer.commit(asynchronous=False)

                elif origem == "hospital":
                    if tipo == "dados":
                        buffer_hospital.extend(dados)
                    elif tipo == "fim_historico":
                        fim_hospital = True
                    elif tipo == "normal":
                        print(f"[Hospital] Batch normal com {len(dados)} registros")
                        processar(dados, "hospital", spark, producer)
                        consumer.commit(asynchronous=False)

                # Processamento de batches históricos quando fim é recebido
                if fim_secretary:
                    print("Processando histórico da secretaria")
                    processar(buffer_secretary, "secretary", spark, producer)
                    buffer_secretary.clear()
                    fim_secretary = False
                    consumer.commit(asynchronous=False)

                if fim_hospital:
                    print("Processando histórico do hospital")
                    processar(buffer_hospital, "hospital", spark, producer)
                    buffer_hospital.clear()
                    fim_hospital = False
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"Erro no processamento: {e}")

    except KeyboardInterrupt:
        print("Interrupção manual detectada")
    finally:
        consumer.close()
        spark.stop()
        producer.flush()

# Execução principal
if __name__ == "__main__":
    main()
