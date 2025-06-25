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

schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def filtrar_spark(df):
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
    try:
        dados = [json.loads(msg.value().decode("utf-8")) for msg in messages]

        for d in dados:
            for campo in ["Diagnostico", "Vacinado", "CEP", "Escolaridade", "Populacao"]:
                coerir_para_int(d, campo)

        df = spark.createDataFrame(dados, schema=schema)
        df_filtrado = filtrar_spark(df)
        resultados = df_filtrado.rdd.map(lambda row: json.dumps(row.asDict())).collect()

        for r in resultados:
            producer.produce(DEST_TOPIC, r.encode("utf-8"))

        print(f"[FILTRO] Batch de {len(dados)} processado → {len(resultados)} válidos para '{DEST_TOPIC}'")
        return len(resultados), len(dados)

    except Exception as e:
        print(f"[FILTRO][ERRO] Processamento do lote: {e}")
        return 0, len(messages)

def consumir_clean_secretary():
    spark = SparkSession.builder \
        .appName("tratador_filtragem") \
        .config("spark.driver.memory", "512m") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([SOURCE_TOPIC])
    print(f"[FILTRO] Subscrito ao tópico {SOURCE_TOPIC}")

    try:
        batch_size = 50
        batch = []
        total = filtrados = 0
        inicio = time.time()

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if batch:
                    filtrado, lidos = process_batch(batch, spark, producer)
                    total += lidos
                    filtrados += filtrado
                    consumer.commit()
                    batch = []
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[FILTRO][ERRO] Kafka: {msg.error()}")
                continue

            batch.append(msg)
            if len(batch) >= batch_size:
                filtrado, lidos = process_batch(batch, spark, producer)
                total += lidos
                filtrados += filtrado
                producer.flush()
                consumer.commit()
                batch = []

    except KeyboardInterrupt:
        duracao = time.time() - inicio
        print(f"📊 [FILTRO] Finalizado: {filtrados}/{total} registros válidos em {duracao:.2f}s")
    finally:
        try:
            consumer.close()
        except Exception as e:
            print(f"[FILTRO][ERRO] ao fechar consumer: {e}")
        try:
            spark.stop()
        except Exception as e:
            print(f"[FILTRO][ERRO] ao fechar Spark: {e}")

if __name__ == "__main__":
    consumir_clean_secretary()
