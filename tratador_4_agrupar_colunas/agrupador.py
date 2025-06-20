import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_agrupamento_group"
SOURCE_TOPIC = "filtered_secretary"
DEST_TOPIC = "grouped_secretary"

AGRUPAR_POR = "CEP"  # <- você pode alterar para 'Data', 'Escolaridade', etc.

schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def process_batch(messages, spark, producer):
    try:
        dados = [json.loads(msg.value().decode("utf-8")) for msg in messages]
        df = spark.createDataFrame(dados, schema=schema)

        # Agrupamento por coluna
        df_grouped = df.groupBy(AGRUPAR_POR).agg(
            avg("Diagnostico").alias("media_diagnostico"),
            avg("Vacinado").alias("media_vacinado"),
            avg("Escolaridade").alias("media_escolaridade"),
            avg("Populacao").alias("media_populacao")
        )

        resultados = df_grouped.collect()

        for row in resultados:
            resultado_dict = row.asDict()
            producer.produce(
                DEST_TOPIC,
                json.dumps(resultado_dict).encode("utf-8")
            )

        return len(resultados), len(dados)

    except Exception as e:
        print(f"❌ Erro ao processar lote: {e}")
        return 0, len(messages)

def main():
    spark = SparkSession.builder.appName("tratador_agrupamento").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([SOURCE_TOPIC])
    print(f"📦 Inscrito no tópico {SOURCE_TOPIC}")

    batch = []
    batch_size = 50
    msg_count = processed_count = 0
    start_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if batch:
                    processed, total = process_batch(batch, spark, producer)
                    processed_count += processed
                    msg_count += total
                    batch = []
                    consumer.commit(asynchronous=False)
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Kafka Error: {msg.error()}")
                continue

            batch.append(msg)
            if len(batch) >= batch_size:
                processed, total = process_batch(batch, spark, producer)
                processed_count += processed
                msg_count += total
                batch = []
                producer.flush()
                consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print(f"\n📊 Agrupados: {processed_count}/{msg_count} mensagens processadas")
        print(f"⏱️ Tempo: {time.time() - start_time:.2f} segundos")
    finally:
        producer.flush()
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
