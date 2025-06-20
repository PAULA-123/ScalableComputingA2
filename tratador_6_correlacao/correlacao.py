import json
import time
import requests
from typing import List, Dict
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_correlacao_group"
SOURCE_TOPIC = "grouped_secretary"
API_URL = "http://api:8000/correlacao"

schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("media_diagnostico", FloatType(), True),
    StructField("media_vacinado", FloatType(), True),
    StructField("media_escolaridade", FloatType(), True),
    StructField("media_populacao", FloatType(), True)
])

def processar_correlacoes(df, enviar_api: bool = True) -> None:
    print("\n📊 Correlações calculadas:")
    try:
        # Pares específicos que o dashboard espera
        resultados: List[Dict[str, float]] = []

        # Correlação entre Escolaridade e Vacinado
        esc_vac = df.stat.corr("media_escolaridade", "media_vacinado")
        resultados.append({"Escolaridade": round(esc_vac, 4), "Vacinado": round(esc_vac, 4)})
        print(f"📌 Correlação Escolaridade x Vacinado: {esc_vac:.4f}")

        if enviar_api:
            response = requests.post(API_URL, json=resultados)
            if response.status_code == 200:
                print("✅ Correlações enviadas com sucesso para a API")
            else:
                print(f"❌ Erro ao enviar para API: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"❌ Erro ao calcular ou enviar correlações: {e}")

def main():
    spark = SparkSession.builder.appName("tratador_correlacao").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    print(f"📦 Inscrito no tópico {SOURCE_TOPIC}")

    registros = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if registros:
                    df = spark.createDataFrame(registros, schema=schema)
                    processar_correlacoes(df)
                    registros = []
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Erro Kafka: {msg.error()}")
                continue

            try:
                dado = json.loads(msg.value().decode("utf-8"))
                registros.append(dado)
            except Exception as e:
                print(f"❌ JSON inválido: {e}")

    except KeyboardInterrupt:
        print("⏹️ Interrompido pelo usuário")
        if registros:
            df = spark.createDataFrame(registros, schema=schema)
            processar_correlacoes(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
