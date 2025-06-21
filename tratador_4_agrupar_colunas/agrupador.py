import json
import time
import requests
from collections import defaultdict
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_agrupamento_group"
SOURCE_TOPIC = "filtered_secretary"
DEST_TOPIC = "grouped_secretary"
API_URL = "http://api:8000/agrupamento"
AGRUPAR_POR = "CEP"

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

        # Agrupamento por CEP
        df_grouped = df.groupBy(AGRUPAR_POR).agg(
            avg("Diagnostico").alias("media_diagnostico"),
            avg("Vacinado").alias("media_vacinado"),
            avg("Escolaridade").alias("media_escolaridade"),
            avg("Populacao").alias("media_populacao")
        )

        resultados = df_grouped.collect()

        # Obter data e total_vacinados manualmente por CEP
        dados_por_cep = defaultdict(list)
        for dado in dados:
            dados_por_cep[dado["CEP"]].append(dado)

        payload = []
        for row in resultados:
            r = row.asDict()
            cep = r["CEP"]
            grupo = dados_por_cep.get(cep, [])

            data_mais_comum = grupo[0]["Data"] if grupo else "01-01-2025"
            total_vacinados = sum(p.get("Vacinado", 0) for p in grupo)

            api_item = {
                "CEP": cep,
                "media_diagnostico": round(r["media_diagnostico"], 4),
                "data": data_mais_comum,
                "total_vacinados": total_vacinados
            }
            payload.append(api_item)

            producer.produce(
                DEST_TOPIC,
                json.dumps(r).encode("utf-8")
            )

        # Envio para API
        if payload:
            try:
                response = requests.post(API_URL, json=payload)
                if response.status_code == 200:
                    print("✅ Resultados enviados para API /agrupamento")
                else:
                    print(f"❌ Erro API: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"❌ Falha ao enviar para API: {e}")

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
