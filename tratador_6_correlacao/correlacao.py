import json
import time
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import corr
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_correlacao_group"
SOURCE_TOPIC = "grouped_secretary"

schema = StructType([
    StructField("CEP", IntegerType(), True),  # ou a chave usada no agrupamento
    StructField("media_diagnostico", FloatType(), True),
    StructField("media_vacinado", FloatType(), True),
    StructField("media_escolaridade", FloatType(), True),
    StructField("media_populacao", FloatType(), True)
])

def processar_correlacoes(df):
    print("\n📊 Correlações calculadas:")
    try:
        colunas = ["media_diagnostico", "media_vacinado", "media_escolaridade", "media_populacao"]
        for i in range(len(colunas)):
            for j in range(i + 1, len(colunas)):
                c1 = colunas[i]
                c2 = colunas[j]
                valor = df.select(corr(c1, c2)).first()[0]
                print(f"📌 Correlação entre {c1} e {c2}: {valor:.4f}")
    except Exception as e:
        print(f"❌ Erro ao calcular correlações: {e}")

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
    print(f"📦 Inscrito no tópico {SOURCE_TOPIC}")

    registros = []
    start_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if registros:
                    df = spark.createDataFrame(registros, schema=schema)
                    processar_correlacoes(df)
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
            processar_correlacoes(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
