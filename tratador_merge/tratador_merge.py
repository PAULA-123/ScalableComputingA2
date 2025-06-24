import json
import time
import os
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_merge_group"
TOPIC_HOSPITAL_B = "raw_hospital_b"
TOPIC_SECRETARY_B = "raw_secretary_b"

FLUSH = True

# Inicializa Spark
spark = SparkSession.builder.appName("tratador_merge_batch").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Schemas
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

def processar_batch(dados_hosp, dados_secr):
    df_hosp = spark.createDataFrame(dados_hosp, schema=hospital_schema)
    df_secr = spark.createDataFrame(dados_secr, schema=secretary_schema)

    # Agregação por CEP
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

    merged = agg_hosp.join(agg_secr, on="CEP", how="outer")
    merged = merged.fillna(0)
    return merged

def main():
    print("\n" + "="*50)
    print(" INICIANDO TRATADOR DE MERGE POR BATCH ")
    print("="*50 + "\n")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC_HOSPITAL_B, TOPIC_SECRETARY_B])
    print("Inscrito nos tópicos:", TOPIC_HOSPITAL_B, TOPIC_SECRETARY_B, flush=FLUSH)

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
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Erro no consumidor: {msg.error()}")
                    continue

            try:
                raw = msg.value().decode("utf-8")
                payload = json.loads(raw)
                registros = payload.get("batch", [])

                if msg.topic() == TOPIC_HOSPITAL_B:
                    dados_hosp.extend(registros)
                    print(f"[HOSPITAL] +{len(registros)} registros (total: {len(dados_hosp)})")
                elif msg.topic() == TOPIC_SECRETARY_B:
                    dados_secr.extend(registros)
                    print(f"[SECRETARIA] +{len(registros)} registros (total: {len(dados_secr)})")

                # Processa quando houver dados de ambos os tópicos
                if len(dados_hosp) > 0 and len(dados_secr) > 0:
                    msg_count += 1
                    df_merge = processar_batch(dados_hosp, dados_secr)

                    output_dir = "databases_mock"
                    output_file = os.path.join(output_dir, "merge_batch.json")
                    os.makedirs(output_dir, exist_ok=True)

                    linhas_json = df_merge.toJSON().collect()
                    with open(output_file, "w", encoding="utf-8") as f:
                        for linha in linhas_json:
                            f.write(linha + "\n")

                    print(f"✅ Merge salvo em {output_file} com {df_merge.count()} linhas")

                    # Reset após salvar
                    dados_hosp = []
                    dados_secr = []
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"❌ Erro ao processar mensagem: {e}")

    except KeyboardInterrupt:
        print("\n" + "="*50)
        print(" INTERRUPÇÃO SOLICITADA - ENCERRANDO CONSUMER ")
        print(f"Total de merges processados: {msg_count}")
        print(f"Tempo de execução: {time.time() - start_time:.2f} segundos")
        print("="*50)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
