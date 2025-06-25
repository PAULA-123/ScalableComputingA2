import json
import time
import requests
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_correlacao_group"
SOURCE_TOPIC = "grouped_secretary"
API_URL = "http://api:8000/correlacao"

schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("media_diagnostico", FloatType(), True),
    StructField("media_vacinado", FloatType(), True),
    StructField("media_escolaridade", FloatType(), True),
    StructField("media_populacao", FloatType(), True)
])

def process_batch(df, enviar_api=True):
    try:
        esc_vac = df.stat.corr("media_escolaridade", "media_vacinado")
        print(f" Correlação Escolaridade x Vacinado: {esc_vac:.4f}")

        if enviar_api:
            payload = [{"Escolaridade": round(esc_vac, 4), "Vacinado": round(esc_vac, 4)}]
            try:
                response = requests.post(API_URL, json=payload)
                if response.status_code == 200:
                    print(" Correlações enviadas com sucesso para a API")
                else:
                    print(f"=========== Erro ao enviar para API: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"=========== Erro na requisição para API: {e}")
    except Exception as e:
        print(f"=========== Erro ao calcular correlação: {e}")

def main():
    spark = SparkSession.builder.appName("tratador_correlacao").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    print(f" Inscrito no tópico {SOURCE_TOPIC}")

    buffer = []
    batch_size = 100

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if buffer:
                    df = spark.read.schema(schema).json(spark.sparkContext.parallelize([json.dumps(d) for d in buffer]))
                    process_batch(df)
                    buffer = []
                    consumer.commit()
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"=========== Erro Kafka: {msg.error()}")
                continue

            try:
                dado = json.loads(msg.value().decode("utf-8"))
                buffer.append(dado)
            except Exception as e:
                print(f"=========== JSON inválido: {e}")

            if len(buffer) >= batch_size:
                df = spark.read.schema(schema).json(spark.sparkContext.parallelize([json.dumps(d) for d in buffer]))
                process_batch(df)
                buffer = []
                consumer.commit()

    except KeyboardInterrupt:
        print(" Interrompido pelo usuário")
        if buffer:
            df = spark.read.schema(schema).json(spark.sparkContext.parallelize([json.dumps(d) for d in buffer]))
            process_batch(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
