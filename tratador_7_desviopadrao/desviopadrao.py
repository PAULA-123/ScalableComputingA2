import json
import time
import requests
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_desvio_group"
SOURCE_TOPIC = "grouped_secretary"
API_URL = "http://api:8000/desvios"

schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("media_diagnostico", FloatType(), True),
    StructField("media_vacinado", FloatType(), True),
    StructField("media_escolaridade", FloatType(), True),
    StructField("media_populacao", FloatType(), True)
])

def calcular_desvios(df, enviar_api=True):
    print("\n Desvios padrão das métricas:")
    try:
        # Cálculo dos desvios paralelamente via Spark
        df_desvios = df.agg(
            stddev("media_diagnostico").alias("media_diagnostico"),
            stddev("media_vacinado").alias("media_vacinado"),
            stddev("media_escolaridade").alias("media_escolaridade"),
            stddev("media_populacao").alias("media_populacao")
        )

        # Transformar diretamente em JSON Spark sem coletar
        df_formatado = df_desvios.selectExpr(
            "named_struct('variavel', 'media_diagnostico', 'desvio', round(media_diagnostico, 4)) as v1",
            "named_struct('variavel', 'media_vacinado', 'desvio', round(media_vacinado, 4)) as v2",
            "named_struct('variavel', 'media_escolaridade', 'desvio', round(media_escolaridade, 4)) as v3",
            "named_struct('variavel', 'media_populacao', 'desvio', round(media_populacao, 4)) as v4"
        ).selectExpr("v1", "v2", "v3", "v4") \
         .selectExpr("stack(4, v1, v2, v3, v4) as desvios") \
         .selectExpr("desvios.variavel as variavel", "desvios.desvio as desvio")

        # Converter para JSON e enviar
        payload = [json.loads(row_json) for row_json in df_formatado.toJSON().collect()]

        for d in payload:
            print(f"- {d['variavel']}: {d['desvio']:.4f}")

        if enviar_api:
            response = requests.post(API_URL, json=payload)
            if response.status_code == 200:
                print(" Desvios enviados com sucesso para a API")
            else:
                print(f"======== Erro ao enviar para API: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"=========== Erro ao calcular ou enviar desvio padrão: {e}")

def main():
    spark = SparkSession.builder.appName("tratador_desvio").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    print(f" Inscrito no tópico {SOURCE_TOPIC}")

    registros = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if registros:
                    df = spark.createDataFrame(registros, schema=schema)
                    calcular_desvios(df)
                    registros = []
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"======== Erro Kafka: {msg.error()}")
                continue

            try:
                dado = json.loads(msg.value().decode("utf-8"))
                registros.append(dado)
            except Exception as e:
                print(f"======== JSON inválido: {e}")

    except KeyboardInterrupt:
        print(" Interrompido pelo usuário")
        if registros:
            df = spark.createDataFrame(registros, schema=schema)
            calcular_desvios(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
