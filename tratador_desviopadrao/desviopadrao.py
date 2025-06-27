"""
Este script consome batches de dados agregados da secretaria de saúde ou do merge final,
e calcula os desvios padrão de variáveis de interesse usando Apache Spark.

Os desvios padrão são enviados para uma API REST para visualização ou armazenamento.

Fluxo:
1. Consome mensagens Kafka dos tópicos grouped_secretary e grouped_merge.
2. Processa os dados como batches JSON com Spark.
3. Calcula os desvios padrão de diagnósticos, vacinados, escolaridade e população.
4. Envia os desvios para a API REST.
"""

import json
import time
import os
import requests
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

# Configurações Kafka e API
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_desvio_group"
SOURCE_TOPICS = ["merge_hospital_secretary"]
API_URL = "http://api:8000/desvios"

# Schema dos dados esperados nos batches agrupados
schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("Total_Diagnosticos", FloatType(), True),
    StructField("Media_Escolaridade", FloatType(), True),
    StructField("Total_Vacinados", FloatType(), True),
    StructField("Populacao", FloatType(), True)
])

# Função para calcular os desvios padrão e enviar para a API
def calcular_desvios(df, enviar_api=True):
    try:
        # Calcula os desvios padrão em paralelo com Spark    

        df_desvios = df.agg(
        stddev("Total_Diagnosticos").alias("Total_Diagnosticos"),
        stddev("Total_Vacinados").alias("Total_Vacinados"),
        stddev("Media_Escolaridade").alias("Media_Escolaridade")
    )

        df1 = df_desvios.selectExpr("round(Total_Diagnosticos, 4) as desvio") \
            .withColumn("variavel", lit("Total_Diagnosticos"))
        df2 = df_desvios.selectExpr("round(Total_Vacinados, 4) as desvio") \
            .withColumn("variavel", lit("Total_Vacinados"))
        df3 = df_desvios.selectExpr("round(Media_Escolaridade, 4) as desvio") \
            .withColumn("variavel", lit("Media_Escolaridade"))

        df_formatado = df1.union(df2).union(df3).select("variavel", "desvio")

        # Converte para JSON
        payload = [json.loads(row_json) for row_json in df_formatado.toJSON().collect()]

        # Envia para a API
        if enviar_api:
            response = requests.post(API_URL, json=payload)
            if response.status_code == 200:
                print("Desvios enviados com sucesso para a API")
            else:
                print(f"Erro ao enviar para API: {response.status_code} - {response.text}")

        # Salva localmente
        salvar_em_json(payload)

    except Exception as e:
        print(f"Erro ao calcular ou enviar desvio padrão: {e}")
        
        
def salvar_em_json(payload, output_path="/app/databases_mock/desvio.json"):
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        if os.path.exists(output_path):
            try:
                with open(output_path, "r", encoding="utf-8") as f:
                    dados_existentes = json.load(f)
                if isinstance(dados_existentes, list):
                    dados_existentes.extend(payload)
                else:
                    dados_existentes = [dados_existentes] + payload
            except Exception:
                dados_existentes = payload
        else:
            dados_existentes = payload

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(dados_existentes, f, indent=4, ensure_ascii=False)

        print(f"[ARQUIVO] JSON salvo com sucesso em {output_path}")
        return True
    except Exception as e:
        print(f"[ERRO ARQUIVO] Falha ao salvar JSON: {e}")
        return False

# Função principal
def main():
    spark = SparkSession.builder.appName("tratador_desviopadrao").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe(SOURCE_TOPICS)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Erro Kafka: {msg.error()}")
                continue

            try:
                # Decodifica a mensagem Kafka (espera um dict com chave "batch")
                conteudo = json.loads(msg.value().decode("utf-8"))
                dados = conteudo.get("batch", [])
                origem = conteudo.get("source", "normal")

                if dados:
                    print(f"Lote recebido da origem: {origem} com {len(dados)} registros")

                    # Converte para DataFrame
                    df = spark.read.schema(schema).json(
                        spark.sparkContext.parallelize([json.dumps(d) for d in dados])
                    )

                    # Calcula e envia os desvios
                    calcular_desvios(df)
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"Erro ao processar mensagem Kafka: {e}")

    except KeyboardInterrupt:
        print("Interrupção manual detectada")
    finally:
        consumer.close()
        spark.stop()

# Execução
if __name__ == "__main__":
    main()