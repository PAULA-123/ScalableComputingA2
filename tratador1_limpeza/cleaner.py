import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_limpeza_group"
SOURCE_TOPIC = "raw_secretary"
DEST_TOPIC = "clean_secretary"

# Schema robusto
schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def limpeza(df):
    return df.filter(
        (col("Diagnostico").isin(0, 1)) &
        (col("Vacinado").isin(0, 1)) &
        (col("CEP").between(11001, 30999)) &
        (col("Escolaridade").between(0, 5)) &
        (col("Populacao") > 0) &
        (col("Data").isNotNull())
    )

def process_batch(buffer, spark):
    """
    Processa um batch de registros usando Spark e retorna lista de JSON strings válidas.
    """
    df = spark.createDataFrame(buffer, schema=schema)
    df_limpo = limpeza(df)
    # Converter linhas para JSON
    return df_limpo.rdd.map(lambda row: json.dumps(row.asDict())).collect()

def main():
    spark = SparkSession.builder.appName("tratador_limpeza").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([SOURCE_TOPIC])
    print(f"🔍 Aguardando mensagens no tópico {SOURCE_TOPIC}...")

    buffer = []
    batch_size = 100
    total = limpos = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Erro Kafka: {msg.error()}")
                continue

            try:
                dado = json.loads(msg.value().decode('utf-8'))
                buffer.append(dado)
                total += 1
            except Exception as e:
                print(f"❌ JSON inválido: {e}")

            if len(buffer) >= batch_size:
                mensagens = process_batch(buffer, spark)
                for msg_json in mensagens:
                    producer.produce(DEST_TOPIC, msg_json.encode('utf-8'))
                limpos += len(mensagens)
                producer.flush()
                consumer.commit()
                buffer = []

    except KeyboardInterrupt:
        print(f"\n✅ Finalizado. {limpos}/{total} mensagens válidas.")
    finally:
        producer.flush()
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
