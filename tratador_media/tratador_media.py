import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when, lit
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_alerta_group"
SOURCE_TOPIC = "raw_oms_batch"
DEST_TOPIC = "alert_oms"

FLUSH = True

# Inicializa Spark
spark = SparkSession.builder.appName("tratador_alerta_media").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Schema esperado
schema = StructType([
    StructField("N_obitos", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("N_recuperados", IntegerType(), True),
    StructField("N_vacinados", IntegerType(), True),
    StructField("Data",     # poderia também ser StringType se não quiser converter
        # DateType() se você quiser converter usando `.to_date()`
        # StringType() se quiser deixar como está
        StringType(), True
    )
])

def marcar_alertas(df):
    media_valor = df.select(mean(col("Populacao"))).collect()[0][0]
    
    if media_valor is None:
        return df.withColumn("Alerta", lit("Verde"))

    df_com_alerta = df.withColumn(
        "Alerta",
        when(col("Populacao") > media_valor, "Vermelho").otherwise("Verde")
    )
    return df_com_alerta

def main():
    print("\n" + "="*50)
    print(" INICIANDO TRATADOR DE MÉDIA E ALERTA POR BATCH ")
    # print("="*50)
    # print(f"Conectando ao Kafka em: {KAFKA_BOOTSTRAP_SERVERS}")
    # print(f"Tópico de origem: {SOURCE_TOPIC}")
    # print(f"Tópico de destino: {DEST_TOPIC}")
    # print(f"Grupo de consumidores: {GROUP_ID}")
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
    print(f"Inscrito no tópico {SOURCE_TOPIC}. Aguardando mensagens...", flush=FLUSH)

    msg_count = 0
    start_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=0.1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Erro no consumidor: {msg.error()}")
                    continue

            try:
                msg_count += 1

                # Lê um batch como lista de registros
                raw_msg = msg.value().decode("utf-8")
                dados = json.loads(raw_msg)
                if "batch" in dados:
                    dados_batch = dados["batch"]  # Extrai a lista de registros
                else:
                    dados_batch = [dados]

                print("Dados recebidos: ", len(dados_batch), flush = True)

                # Cria DataFrame com todo o batch
                df = spark.createDataFrame(dados_batch, schema=schema)

                # Marca alertas no batch
                df_resultado = marcar_alertas(df)

                output_dir = "databases_mock"
                output_file = os.path.join(output_dir, "alerta_batch.json")

                os.makedirs(output_dir, exist_ok=True)

                # Converte cada linha para JSON (como string)
                linhas_json = df_resultado.toJSON().collect()

                # Salva como arquivo fixo
                with open(output_file, "w", encoding="utf-8") as f:
                    for linha in linhas_json:
                        f.write(linha + "\n")

                print(f"✅ Batch salvo como arquivo único em {output_file}")

                consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"❌ Erro no processamento da mensagem: {e}")

    except KeyboardInterrupt:
        print("\n" + "="*50)
        print(" INTERRUPÇÃO SOLICITADA - ENCERRANDO CONSUMER ")
        print(f"Total de batches processados: {msg_count}")
        print(f"Tempo de execução: {time.time() - start_time:.2f} segundos")
        print("="*50)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()