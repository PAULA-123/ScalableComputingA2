import json
import os
import time
from statistics import mean
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import StructType, StructField, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_metricas_group"
SOURCE_TOPIC = "filtered_secretary"
OUTPUT_FILE = "databases_mock/resultados_metrica.json"

# Schema para os dados de entrada
schema = StructType([
    StructField("Diagnostico", IntegerType(), True)
])

def salvar_resultado(resultados):
    try:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(resultados, f, indent=4)
    except Exception as e:
        print(f"[ERRO] Falha ao salvar arquivo JSON: {e}")

def main():
    print("[METRICA] Iniciando tratador de métricas com Spark")
    spark = SparkSession.builder.appName("tratador_metricas").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    resultados = []
    acumulado = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[METRICA] Erro no Kafka: {msg.error()}")
                    continue

            try:
                dado = json.loads(msg.value().decode("utf-8"))
                if "Diagnostico" in dado and dado["Diagnostico"] is not None:
                    try:
                        dado["Diagnostico"] = int(dado["Diagnostico"])
                    except Exception as e:
                        print(f"[METRICA] Ignorando dado com Diagnostico inválido: {dado['Diagnostico']} ({e})")
                        continue

                    acumulado.append(dado)

                    if len(acumulado) % 5 == 0:
                        df = spark.createDataFrame(acumulado, schema=schema)
                        media_row = df.select(avg(col("Diagnostico")).alias("media")).collect()[0]
                        media = media_row["media"]

                        resultado = {
                            "quantidade": len(acumulado),
                            "media_diagnostico": round(media, 4)
                        }
                        resultados.append(resultado)

                        print(f"[METRICA] Resultado #{len(resultados)}: {resultado}")

                        salvar_resultado(resultados)

                consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"[METRICA] Erro ao processar mensagem: {e}")

    except KeyboardInterrupt:
        print("\n[METRICA] Interrompido pelo usuário")
    finally:
        consumer.close()
        spark.stop()
        print("[METRICA] Tratador finalizado")


if __name__ == "__main__":
    main()
