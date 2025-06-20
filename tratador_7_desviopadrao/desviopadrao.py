import json
import time
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_desvio_group"
SOURCE_TOPIC = "grouped_secretary"

schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("media_diagnostico", FloatType(), True),
    StructField("media_vacinado", FloatType(), True),
    StructField("media_escolaridade", FloatType(), True),
    StructField("media_populacao", FloatType(), True)
])

def calcular_desvios(df):
    print("\n📈 Desvios padrão das métricas:")
    try:
        colunas = ["media_diagnostico", "media_vacinado", "media_escolaridade", "media_populacao"]
        for col in colunas:
            valor = df.select(stddev(col).alias("desvio")).first()["desvio"]
            print(f"📌 {col}: {valor:.4f}")
    except Exception as e:
        print(f"❌ Erro ao calcular desvio padrão: {e}")

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
    print(f"📦 Inscrito no tópico {SOURCE_TOPIC}")

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
                    print(f"❌ Erro Kafka: {msg.error()}")
                continue

            try:
                dado = json.loads(msg.value().decode("utf-8"))
                registros.append(dado)
            except Exception as e:
                print(f"❌ JSON inválido: {e}")

    except KeyboardInterrupt:
        print("⏹️ Interrompido pelo usuário")
        if registros:
            df = spark.createDataFrame(registros, schema=schema)
            calcular_desvios(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
