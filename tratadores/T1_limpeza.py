import os
import json
import logging
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

# ======================= CONFIGURAÇÃO DE LOG =======================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format='[%(levelname)s] %(message)s')
logger = logging.getLogger("tratador_limpeza")

# ======================= CONFIGURAÇÕES =======================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "tratador_limpeza_group")

TOPICS_IN = {
    'secretary': os.getenv("TOPIC_SECRETARY", "raw_secretary"),
    'hospital': os.getenv("TOPIC_HOSPITAL", "raw_hospital"),
    'oms': os.getenv("TOPIC_OMS", "raw_oms_batch")
}
TOPICS_OUT = {
    'secretary': os.getenv("TOPIC_CLEAN_SECRETARY", "clean_secretary"),
    'hospital': os.getenv("TOPIC_CLEAN_HOSPITAL", "clean_hospital"),
    'oms': os.getenv("TOPIC_CLEAN_OMS", "clean_oms")
}

# ======================= SCHEMAS =======================
SCHEMAS = {
    'secretary': StructType([
        StructField("Diagnostico", IntegerType(), True),
        StructField("Vacinado", IntegerType(), True),
        StructField("CEP", IntegerType(), True),
        StructField("Escolaridade", IntegerType(), True),
        StructField("Populacao", IntegerType(), True),
        StructField("Data", StringType(), True)
    ]),
    'hospital': StructType([
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
    ]),
    'oms': StructType([
        StructField("N_obitos", IntegerType(), True),
        StructField("Populacao", IntegerType(), True),
        StructField("CEP", IntegerType(), True),
        StructField("N_recuperados", IntegerType(), True),
        StructField("N_vacinados", IntegerType(), True),
        StructField("Data", StringType(), True)
    ])
}

# ======================= FUNÇÕES DE LIMPEZA =======================
def clean_secretary(df):
    return df.filter(
        (col("Diagnostico").isin(0, 1)) &
        (col("Vacinado").isin(0, 1)) &
        (col("CEP").between(11001, 30999)) &
        (col("Escolaridade").between(0, 5)) &
        (col("Populacao") > 0) &
        (col("Data").isNotNull())
    )

def clean_hospital(df):
    return df.filter(
        (col("Internado").isin(0, 1)) &
        (col("Idade").between(0, 120)) &
        (col("Sexo").isin(0, 1)) &
        (col("CEP").between(11001, 30999)) &
        (col("Sintoma1").isin(0, 1)) &
        (col("Sintoma2").isin(0, 1)) &
        (col("Sintoma3").isin(0, 1)) &
        (col("Sintoma4").isin(0, 1)) &
        (col("Data").isNotNull())
    )

def clean_oms(df):
    return df.filter(
        (col("N_obitos") >= 0) &
        (col("Populacao") > 0) &
        (col("N_recuperados") >= 0) &
        (col("N_vacinados") >= 0) &
        (col("CEP").between(11, 30)) &
        (col("Data").isNotNull())
    )

# ======================= MAIN =======================
def main():
    spark = SparkSession.builder \
        .appName("tratador_limpeza_unificado") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe(list(TOPICS_IN.values()))
    logger.info(f"Inscrito nos tópicos: {list(TOPICS_IN.values())}")

    topic_para_funcao = {
        TOPICS_IN['secretary']: clean_secretary,
        TOPICS_IN['hospital']: clean_hospital,
        TOPICS_IN['oms']: clean_oms
    }

    topic_para_schema = {
        TOPICS_IN['secretary']: SCHEMAS['secretary'],
        TOPICS_IN['hospital']: SCHEMAS['hospital'],
        TOPICS_IN['oms']: SCHEMAS['oms']
    }

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Erro no Kafka: {msg.error()}")
                continue

            try:
                mensagem = json.loads(msg.value().decode("utf-8"))
                batch = mensagem.get("batch", [])
                if not batch:
                    continue

                topic = msg.topic()
                func = topic_para_funcao.get(topic)
                schema = topic_para_schema.get(topic)

                if not func or not schema:
                    logger.warning(f"Tópico desconhecido: {topic}")
                    continue

                df = spark.createDataFrame(batch, schema=schema)
                logger.debug(f"Schema detectado para tópico {topic}")
                df.show(5, truncate=False)

                df_limpo = func(df)
                df_limpo.show(5, truncate=False)
                resultados = df_limpo.rdd.map(lambda row: json.loads(json.dumps(row.asDict()))).collect()

                if resultados:
                    payload = json.dumps({"batch": resultados})
                    producer.produce(TOPICS_OUT[[k for k, v in TOPICS_IN.items() if v == topic][0]], payload.encode("utf-8"))
                    logger.info(f"Enviado {len(resultados)} registros limpos para {topic}")
                else:
                    logger.info("Nenhum registro válido encontrado")

                producer.flush()
                consumer.commit()

            except Exception as e:
                logger.error(f"Erro ao processar mensagem: {e}")

    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário")
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
