import os
import json
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, first, sum as _sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Configurações Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_agrupamento_2fontes"

# Tópicos de entrada
TOPIC_SECRETARY_H = "hist_secretary"
TOPIC_HOSPITAL_H = "hist_hospital"
TOPIC_SECRETARY = "filtered_secretary"
TOPIC_HOSPITAL = "filtered_hospital"

# Tópicos de saída (Kafka)
DEST_TOPIC_SECRETARY = "grouped_secretary"
DEST_TOPIC_HOSPITAL = "grouped_hospital"

# Schemas esperados
schema_secretary = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

schema_hospital = StructType([
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

def agrupar_por_cep(df, origem):
    if origem == "secretary":
        return df.groupBy("CEP").agg(
            _sum("Diagnostico").alias("total_diagnostico"),
            avg("Escolaridade").alias("media_escolaridade"),
            first("Populacao").alias("populacao"),
            _sum("Vacinado").alias("total_vacinados")
        )
    elif origem == "hospital":
        return df.groupBy("CEP", "Data").agg(
            _sum("Internado").alias("total_internados"),
            _sum("Idade").alias("soma_idade"),
            _sum("Sintoma1").alias("total_sintoma1"),
            _sum("Sintoma2").alias("total_sintoma2"),
            _sum("Sintoma3").alias("total_sintoma3"),
            _sum("Sintoma4").alias("total_sintoma4")
        )

def processar(dados, origem, spark, producer):
    print(f"🔄 Iniciando processamento para: {origem} ({len(dados)} registros)")
    schema = schema_secretary if origem == "secretary" else schema_hospital
    df = spark.createDataFrame(dados, schema=schema)
    df_grouped = agrupar_por_cep(df, origem)

    json_batch = df_grouped.toJSON().map(lambda x: json.loads(x)).collect()

    if not json_batch:
        print(f"[{origem.upper()}] ❌ Nenhum grupo gerado, nada enviado.")
        return

    destino = DEST_TOPIC_SECRETARY if origem == "secretary" else DEST_TOPIC_HOSPITAL
    payload = json.dumps({
        "batch": json_batch,
        "source": origem,
        "type": "normal"
    })

    print(f"📤 Enviando batch com {len(json_batch)} grupos para Kafka ({destino})")
    producer.produce(destino, payload.encode("utf-8"))
    producer.flush()
    print(f"[{origem.upper()}] ✅ Envio concluído.")

def main():
    print("🚀 Iniciando tratador de agrupamento com Spark + Kafka")
    spark = SparkSession.builder.appName("tratador_agrupamento_2fontes").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([TOPIC_SECRETARY_H, TOPIC_HOSPITAL_H, TOPIC_SECRETARY, TOPIC_HOSPITAL])
    print(f"📡 Subscrito aos tópicos: {TOPIC_SECRETARY_H}, {TOPIC_HOSPITAL_H}, {TOPIC_SECRETARY}, {TOPIC_HOSPITAL}")

    buffer_secretary = []
    buffer_hospital = []
    fim_secretary = False
    fim_hospital = False

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERRO] Kafka: {msg.error()}")
                continue

            try:
                conteudo_raw = msg.value().decode("utf-8")
                print(f"\n📥 Mensagem Kafka recebida: {conteudo_raw[:120]}...")  # Preview
                conteudo = json.loads(conteudo_raw)

                tipo = conteudo.get("type", "normal")
                dados = conteudo.get("batch", [])
                origem = conteudo.get("source", "")

                if origem == "secretary":
                    if tipo == "dados":
                        buffer_secretary.extend(dados)
                        print(f"[SECRETARY HIST] +{len(dados)} registros acumulados (total={len(buffer_secretary)})")
                    elif tipo == "fim_historico":
                        fim_secretary = True
                        print("[SECRETARY HIST] 🔚 Sinal de fim de histórico recebido")
                    elif tipo == "normal":
                        print(f"[SECRETARY NORMAL] {len(dados)} registros recebidos")
                        processar(dados, "secretary", spark, producer)
                        consumer.commit(asynchronous=False)

                elif origem == "hospital":
                    if tipo == "dados":
                        buffer_hospital.extend(dados)
                        print(f"[HOSPITAL HIST] +{len(dados)} registros acumulados (total={len(buffer_hospital)})")
                    elif tipo == "fim_historico":
                        fim_hospital = True
                        print("[HOSPITAL HIST] 🔚 Sinal de fim de histórico recebido")
                    elif tipo == "normal":
                        print(f"[HOSPITAL NORMAL] {len(dados)} registros recebidos")
                        processar(dados, "hospital", spark, producer)
                        consumer.commit(asynchronous=False)

                if fim_secretary:
                    print("🧮 [PROCESSAMENTO] Histórico completo da secretaria")
                    processar(buffer_secretary, "secretary", spark, producer)
                    buffer_secretary.clear()
                    fim_secretary = False
                    consumer.commit(asynchronous=False)

                if fim_hospital:
                    print("🧮 [PROCESSAMENTO] Histórico completo do hospital")
                    processar(buffer_hospital, "hospital", spark, producer)
                    buffer_hospital.clear()
                    fim_hospital = False
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"[ERRO] ❌ Falha no processamento da mensagem: {e}")

    except KeyboardInterrupt:
        print("⛔ Interrupção manual detectada. Encerrando...")
    finally:
        consumer.close()
        producer.flush()
        spark.stop()
        print("🧹 Finalização completa do tratador.")

if __name__ == "__main__":
    main()
