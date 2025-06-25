import os
import json
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

# Configurações por ambiente
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "raw_secretary")
DEST_TOPIC = os.getenv("DEST_TOPIC", "clean_secretary")
GROUP_ID = os.getenv("GROUP_ID", "tratador_limpeza_group")

# Define o schema conforme esperado
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

def main():
    # Cabeçalho de inicialização
    print(f"\n🧹 [LIMPEZA] Iniciando tratador. Consumindo de '{SOURCE_TOPIC}', produzindo para '{DEST_TOPIC}'")

    # Cria SparkSession
    spark = SparkSession.builder \
        .appName("tratador_limpeza") \
        .config("spark.driver.memory", "512m") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Configura Kafka
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([SOURCE_TOPIC])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[LIMPEZA] Kafka error: {msg.error()}")
                continue

            try:
                # Decodifica e extrai batch
                mensagem = json.loads(msg.value().decode("utf-8"))
                batch = mensagem.get("batch", [])

                if not batch:
                    continue

                print(f"[LIMPEZA] Batch recebido: {len(batch)} registros")
                #print("[LIMPEZA] Mensagem crua:")
                #print(json.dumps(mensagem, indent=2, ensure_ascii=False))

                # Converte para DataFrame
                df = spark.createDataFrame(batch, schema=schema)
                print("[LIMPEZA] DataFrame original (primeiras 5 linhas):")
                df.show(5, truncate=False)

                # Executa limpeza
                df_limpo = limpeza(df)
                print("[LIMPEZA] DataFrame após limpeza (primeiras 5 linhas):")
                df_limpo.show(5, truncate=False)

                # Coleta resultados e envia ao Kafka em um único batch
                resultados = df_limpo.rdd.map(lambda row: json.dumps(row.asDict())).collect()
                if resultados:
                    batch_payload = json.dumps({"batch": [json.loads(r) for r in resultados]})
                    producer.produce(DEST_TOPIC, batch_payload.encode("utf-8"))
                    print(f"[LIMPEZA] Enviado batch com {len(resultados)} registros limpos para '{DEST_TOPIC}'")
                else:
                    print("[LIMPEZA] Nenhum registro limpo para enviar")

            except Exception as e:
                print(f"[LIMPEZA] Erro de processamento: {e}")

    except KeyboardInterrupt:
        print("\n[LIMPEZA] Interrompido pelo usuário")
    finally:
        try:
            consumer.close()
            spark.stop()
        except Exception as e:
            print(f"[LIMPEZA] Erro no encerramento: {e}")

if __name__ == "__main__":
    main()
