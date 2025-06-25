import os
import json
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_merge_group")
TOPIC_HOSPITAL_B = os.getenv("TOPIC_HOSPITAL_B", "raw_hospital_b")
TOPIC_SECRETARY_B = os.getenv("TOPIC_SECRETARY_B", "raw_secretary_b")

hospital_schema = StructType([
    StructField("ID_Hospital", IntegerType(), True),
    StructField("Data", StringType(), True),
    StructField("Internado", IntegerType(), True),
    StructField("Idade", IntegerType(), True),
    StructField("Sexo", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Sintoma1", IntegerType(), True),
    StructField("Sintoma2", IntegerType(), True),
    StructField("Sintoma3", IntegerType(), True),
    StructField("Sintoma4", IntegerType(), True)
])

secretary_schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def processar_batch(spark, dados_hosp, dados_secr):
    df_hosp = spark.createDataFrame(dados_hosp, schema=hospital_schema)
    df_secr = spark.createDataFrame(dados_secr, schema=secretary_schema)

    agg_hosp = df_hosp.groupBy("CEP").agg(
        spark_sum("Internado").alias("Total_Internados"),
        spark_sum("Idade").alias("Soma_Idade"),
        spark_sum("Sintoma1").alias("Total_Sintoma1"),
        spark_sum("Sintoma2").alias("Total_Sintoma2"),
        spark_sum("Sintoma3").alias("Total_Sintoma3"),
        spark_sum("Sintoma4").alias("Total_Sintoma4")
    )

    agg_secr = df_secr.groupBy("CEP").agg(
        spark_sum("Diagnostico").alias("Total_Diagnosticos"),
        spark_sum("Vacinado").alias("Total_Vacinados"),
        spark_sum("Escolaridade").alias("Soma_Escolaridade"),
        spark_sum("Populacao").alias("Soma_Populacao")
    )

    return agg_hosp.join(agg_secr, on="CEP", how="outer").fillna(0)

def main():
    print("\n📦 [MERGE] Iniciando tratador T3")
    spark = SparkSession.builder.appName("tratador_merge_batch").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC_HOSPITAL_B, TOPIC_SECRETARY_B])
    print(f"[MERGE] Subscrito aos tópicos: {TOPIC_HOSPITAL_B}, {TOPIC_SECRETARY_B}")

    dados_hosp = []
    dados_secr = []
    msg_count = 0
    start_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[MERGE][ERRO] Kafka: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                registros = payload.get("batch", [])

                if not isinstance(registros, list):
                    print(f"[MERGE][WARN] Payload inesperado: {payload}")
                    continue

                if msg.topic() == TOPIC_HOSPITAL_B:
                    dados_hosp.extend(registros)
                    print(f"[MERGE][HOSP] +{len(registros)} registros")
                elif msg.topic() == TOPIC_SECRETARY_B:
                    dados_secr.extend(registros)
                    print(f"[MERGE][SECR] +{len(registros)} registros")

                if dados_hosp and dados_secr:
                    msg_count += 1
                    df_merge = processar_batch(spark, dados_hosp, dados_secr)

                    os.makedirs("databases_mock", exist_ok=True)
                    file_path = f"databases_mock/merge_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    linhas = df_merge.toJSON().collect()

                    with open(file_path, "w", encoding="utf-8") as f:
                        for linha in linhas:
                            f.write(linha + "\n")

                    print(f"[MERGE] ✅ Merge #{msg_count} salvo com {len(linhas)} linhas em {file_path}")

                    dados_hosp.clear()
                    dados_secr.clear()
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"[MERGE][ERRO] Processamento: {e}")

    except KeyboardInterrupt:
        print("\n🛑 [MERGE] Interrompido pelo usuário")
        print(f"🔁 Merges realizados: {msg_count}")
        print(f"⏱️ Duração: {time.time() - start_time:.2f}s")
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
