import json
import requests
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_correlacao_group"
SOURCE_TOPIC = "grouped_secretary"
API_URL = "http://api:8000/correlacao"

schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("total_diagnostico", FloatType(), True),       # nome correto
    StructField("media_escolaridade", FloatType(), True),
    StructField("media_populacao", FloatType(), True),
    StructField("data", StringType(), True),                   # campo extra que existe
    StructField("total_vacinados", FloatType(), True)          # nome correto
])

def calcular_e_enviar(df):
    try:
        # Calcula correlação entre escolaridade e vacinado
        esc_vac = df.stat.corr("media_escolaridade", "total_vacinados")
        print(f"📊 Correlação Escolaridade x Vacinado: {esc_vac:.4f}")

        # Envia para API
        payload = [{"Escolaridade": round(esc_vac, 4), "Vacinado": round(esc_vac, 4)}]
        try:
            response = requests.post(API_URL, json=payload)
            if response.status_code == 200:
                print("✅ Correlação enviada para API com sucesso")
            else:
                print(f"❌ Erro ao enviar para API: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"❌ Erro ao fazer requisição para API: {e}")
    except Exception as e:
        print(f"❌ Erro ao calcular correlação: {e}")

def main():
    spark = SparkSession.builder.appName("tratador_correlacao_batch").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    print(f"📥 Inscrito no tópico: {SOURCE_TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Kafka error: {msg.error()}")
                continue

            try:
                conteudo = json.loads(msg.value().decode("utf-8"))
                dados = conteudo.get("batch", [])

                print(dados)

                if dados:
                    df = spark.read.schema(schema).json(spark.sparkContext.parallelize([json.dumps(d) for d in dados]))
                    calcular_e_enviar(df)
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"❌ Erro ao processar mensagem Kafka: {e}")

    except KeyboardInterrupt:
        print("🛑 Interrompido pelo usuário")
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
