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

# Cria a sessão Spark (global)
spark = SparkSession.builder.appName("tratador_filtragem").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Defina o schema para os dados esperados (exemplo com campos CEP e Diagnostico)
schema = StructType([
    StructField("Populacao", IntegerType(), True),
    StructField("Diagnostico", StringType(), True)
])

def filtrar_spark(df):
    # Filtra CEP >= 0 e Diagnostico não nulo (exemplo simples)
    return df.filter(
        (col("Populacao") >= 0) &
        (col("Diagnostico").isNotNull())
    )

def main():
    print("\n" + "="*50)
    print(" INICIANDO TRATADOR DE FILTRAGEM DE DADOS ")
    print("="*50)
    print(f"Conectando ao Kafka em: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Tópico de origem: {SOURCE_TOPIC}")
    print(f"Tópico de destino: {DEST_TOPIC}")
    print(f"Grupo de consumidores: {GROUP_ID}")
    print("="*50 + "\n")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    def delivery_report(err, msg):
        if err is not None:
            print(f"❌ Falha ao enviar mensagem: {err}")

    consumer.subscribe([SOURCE_TOPIC])
    print(f"🔍 Inscrito no tópico {SOURCE_TOPIC}. Aguardando mensagens...")

    msg_count = 0
    start_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Erro no consumidor Kafka: {msg.error()}")
                    continue

            msg_count += 1
            try:
                dado = json.loads(msg.value().decode('utf-8'))
                print(f"[FILTRAGEM] Mensagem recebida: {dado}")

                # Cria DataFrame com uma linha
                df = spark.createDataFrame([dado], schema=schema)

                # Aplica filtro Spark
                df_filtrado = filtrar_spark(df)

                resultado = df_filtrado.collect()

                if resultado:
                    dado_filtrado = resultado[0].asDict()
                    print(f"[FILTRAGEM] Publicando filtrado: {dado_filtrado}")

                    producer.produce(
                        DEST_TOPIC,
                        json.dumps(dado_filtrado).encode('utf-8'),
                        callback=delivery_report
                    )
                    producer.flush()
                else:
                    print(f"[FILTRAGEM] Registro descartado: {dado}")

                consumer.commit(asynchronous=False)

            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON: {e}")
            except Exception as e:
                print(f"❌ Erro inesperado: {e}")

    except KeyboardInterrupt:
        print("\n" + "="*50)
        print(" INTERRUPÇÃO SOLICITADA - ENCERRANDO CONSUMER ")
        print(f"Total de mensagens processadas: {msg_count}")
        print(f"Tempo de execução: {time.time() - start_time:.2f} segundos")
        print("="*50)
    finally:
        consumer.close()
        print("✅ Conexão com Kafka encerrada corretamente")

if __name__ == "__main__":
    main()
