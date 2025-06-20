import json
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_media_movel_group"
SOURCE_TOPIC = "filtered_secretary"

schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)  # formato 'YYYY-MM-DD'
])

def calcular_media_movel(df):
    # Converte string para tipo data
    df = df.withColumn("Data", to_date(col("Data"), "yyyy-MM-dd"))
    
    # Agrupa por data e calcula média de Diagnóstico
    df_agrupado = df.groupBy("Data").agg(avg("Diagnostico").alias("media_diagnostico_diaria"))

    # Janela de ordenação por data (últimos 7 dias, por exemplo)
    window_spec = Window.orderBy(col("Data")).rowsBetween(-6, 0)
    df_com_movel = df_agrupado.withColumn("media_movel", avg("media_diagnostico_diaria").over(window_spec))

    # Identifica o dia mais recente e extrai apenas essa linha
    ultima_data = df_com_movel.agg({"Data": "max"}).first()[0]
    resultado = df_com_movel.filter(col("Data") == ultima_data)

    print("📈 Média móvel para o dia mais recente:")
    resultado.show(truncate=False)

def main():
    spark = SparkSession.builder.appName("tratador_media_movel").getOrCreate()
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
                    calcular_media_movel(df)
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
            calcular_media_movel(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
