import json
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SOURCE_TOPIC = "raw_secretary"
DEST_TOPIC = "clean_secretary"
GROUP_ID = "tratador_limpeza_group"

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

def consumir_raw_secretary():
    spark = SparkSession.builder \
        .appName("tratador_limpeza") \
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
    print(f"[LIMPEZA] Subscrito ao tópico {SOURCE_TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[LIMPEZA][ERRO] Kafka: {msg.error()}")
                continue

            try:
                mensagem = json.loads(msg.value().decode("utf-8"))
                batch = mensagem.get("batch", [])

                if not batch:
                    continue

                print(f"[LIMPEZA] Batch recebido com {len(batch)} registros brutos")

                df = spark.createDataFrame(batch, schema=schema)
                df_limpo = limpeza(df)

                resultados = df_limpo.rdd.map(lambda row: json.dumps(row.asDict())).collect()
                for r in resultados:
                    producer.produce(DEST_TOPIC, r.encode("utf-8"))

                producer.flush()
                consumer.commit()
                print(f"[LIMPEZA] {len(resultados)} registros válidos enviados para '{DEST_TOPIC}'")

            except Exception as e:
                print(f"[LIMPEZA][ERRO] Processamento: {e}")

    except KeyboardInterrupt:
        print("[LIMPEZA] Interrompido pelo usuário")
    finally:
        try:
            consumer.close()
        except Exception as e:
            print(f"[LIMPEZA][ERRO] ao fechar consumer: {e}")
        try:
            spark.stop()
        except Exception as e:
            print(f"[LIMPEZA][ERRO] ao fechar Spark: {e}")
