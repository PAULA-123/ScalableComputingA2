import json
import time
import requests
import os
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# ==================== CONFIGURAÇÕES ====================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_media_movel_group"
SOURCE_TOPIC = "filtered_secretary"
API_URL = "http://api:8000/media-movel"
OUTPUT_PATH = "/app/databases_mock/media_movel.json"

# ==================== ESQUEMA SPARK ====================
schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

# =============== FUNÇÃO PARA SALVAR EM JSON ===============
def salvar_media_movel_em_json(payload, output_path=OUTPUT_PATH):
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        if os.path.exists(output_path):
            try:
                with open(output_path, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                if isinstance(existing_data, list):
                    existing_data.extend(payload)
                    payload = existing_data
                else:
                    payload = [existing_data] + payload
            except json.JSONDecodeError:
                pass  # arquivo corrompido, ignora

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4, ensure_ascii=False)

        print(f"✅ Média móvel salva localmente em {output_path}")
        return True
    except Exception as e:
        print(f"❌ Erro ao salvar média móvel localmente: {e}")
        return False

# ========== VERIFICADOR DE EXISTÊNCIA DO TÓPICO ==========
def esperar_topico(kafka_bootstrap_servers, topic, timeout=30):
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({'bootstrap.servers': kafka_bootstrap_servers})
    print(f"⏳ Aguardando tópico '{topic}' estar disponível no Kafka...")

    for _ in range(timeout):
        metadata = admin.list_topics(timeout=5)
        if topic in metadata.topics and not metadata.topics[topic].error:
            print(f"✅ Tópico '{topic}' encontrado no Kafka.")
            return True
        time.sleep(1)

    print(f"❌ Tópico '{topic}' não encontrado após {timeout} segundos.")
    return False

# =============== CÁLCULO E ENVIO DA MÉDIA MÓVEL ===============
def calcular_media_movel(df, enviar_api: bool = True) -> None:
    try:
        df = df.withColumn("Data", to_date(col("Data"), "dd-MM-yyyy"))

        # Agrupa por Data e calcula a média do Diagnóstico diário
        df_agrupado = df.groupBy("Data").agg(
            avg("Diagnostico").alias("media_diagnostico_diaria")
        )

        window_spec = Window.orderBy(col("Data")).rowsBetween(-6, 0)
        df_com_movel = df_agrupado.withColumn(
            "media_movel", avg("media_diagnostico_diaria").over(window_spec)
        )

        ultima_data = df_com_movel.agg({"Data": "max"}).first()[0]
        resultado = df_com_movel.filter(col("Data") == ultima_data)

        print("\n📊 Média móvel para o dia mais recente:")
        resultado.show(truncate=False)

        resultados_json = resultado.rdd.map(lambda r: {
            "Data": r["Data"].strftime("%Y-%m-%d") if r["Data"] else None,
            "media_movel": round(r["media_movel"], 4) if r["media_movel"] else None
        }).collect()

        # Salva localmente
        if not salvar_media_movel_em_json(resultados_json):
            print("⚠️ Falha ao salvar média móvel localmente")

        # Envia para API
        if enviar_api:
            try:
                response = requests.post(API_URL, json=resultados_json)
                if response.status_code == 200:
                    print("✅ Média móvel enviada com sucesso para a API")
                else:
                    print(f"❌ Erro ao enviar para API: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"❌ Erro ao fazer requisição para API: {e}")

    except Exception as e:
        print(f"❌ Erro ao calcular ou enviar média móvel: {e}")

# ========================= LOOP PRINCIPAL =========================
def main():
    spark = SparkSession.builder.appName("tratador_media_movel").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if not esperar_topico(KAFKA_BOOTSTRAP_SERVERS, SOURCE_TOPIC):
        print("⛔ Encerrando execução: tópico indisponível.")
        return

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    print(f"✅ Inscrito no tópico {SOURCE_TOPIC}")

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
                payload = json.loads(msg.value().decode("utf-8"))
                batch = payload.get("batch", [])
                registros.extend(batch)

            except Exception as e:
                print(f"❌ JSON inválido: {e}")

    except KeyboardInterrupt:
        print("⛔ Interrompido pelo usuário")
        if registros:
            df = spark.createDataFrame(registros, schema=schema)
            calcular_media_movel(df)
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
