import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_filtro_group"
SOURCE_TOPIC = "clean_secretary"
DEST_TOPIC = "filtered_secretary"

# Schema estendido para filtragem
schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def filtrar_spark(df):
    """Filtros mais sofisticados com regras de negócio"""
    return df.filter(
        (col("Populacao") > 1000) &
        (col("Diagnostico").isNotNull()) &
        (col("CEP").isNotNull()) &
        ((col("Vacinado") == 1) | (col("Diagnostico") == 1))
    )

def coerir_para_int(dado, chave):
    try:
        if chave in dado and dado[chave] is not None:
            dado[chave] = int(dado[chave])
    except Exception:
        dado[chave] = None

def process_batch(messages, spark, producer):
    """Processa um lote de mensagens de forma mais segura"""
    try:
        dados = [json.loads(msg.value().decode('utf-8')) for msg in messages]

        # Correção de tipos para IntegerType esperados
        for d in dados:
            for campo in ["Diagnostico", "Vacinado", "CEP", "Escolaridade", "Populacao"]:
                coerir_para_int(d, campo)

        df = spark.createDataFrame(dados, schema=schema)
        df_filtrado = filtrar_spark(df)
        resultados_json = df_filtrado.rdd.map(lambda row: json.dumps(row.asDict())).collect()

        for msg_json in resultados_json:
            producer.produce(
                DEST_TOPIC,
                msg_json.encode('utf-8')
            )

        return len(resultados_json), len(dados)

    except Exception as e:
        print(f"❌ Erro no processamento do lote: {e}")
        return 0, len(messages)

def main():
    spark = SparkSession.builder.appName("tratador_filtragem").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([SOURCE_TOPIC])
    print(f"🔍 Inscrito no tópico {SOURCE_TOPIC}. Aguardando mensagens...")

    try:
        msg_count = processed_count = 0
        start_time = time.time()
        batch_size = 50
        batch = []

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if batch:
                    processed, total = process_batch(batch, spark, producer)
                    processed_count += processed
                    msg_count += total
                    consumer.commit(asynchronous=False)
                    batch = []
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Erro no consumidor Kafka: {msg.error()}")
                continue

            batch.append(msg)
            if len(batch) >= batch_size:
                processed, total = process_batch(batch, spark, producer)
                processed_count += processed
                msg_count += total
                producer.flush()
                consumer.commit(asynchronous=False)
                batch = []

    except KeyboardInterrupt:
        print(f"\n📊 Estatísticas: {processed_count}/{msg_count} mensagens processadas")
        print(f"⏱️  Tempo total: {time.time() - start_time:.2f} segundos")
    finally:
        producer.flush()
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
