import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "cleaner_group"

# Tópicos de entrada e saída para cada tipo
TOPICS_IN = {
    'secretary': 'raw_secretary_b',
    'hospital': 'raw_hospital_b',
    'oms': 'raw_oms_b'
}

TOPICS_OUT = {
    'secretary': 'clean_secretary',
    'hospital': 'clean_hospital',
    'oms': 'clean_oms'
}

# Schemas para cada tipo de dado
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

# Funções de limpeza específicas para cada tipo
def clean_secretary(df):
    return df.filter(
        (col("Diagnostico").isin(0, 1)) &
        (col("Vacinado").isin(0, 1)) &
        (col("CEP").isNotNull()) &
        (col("CEP") >= 11001) &
        (col("CEP") <= 30999) &
        (col("Escolaridade").between(0, 5)) &
        (col("Populacao") > 0) &
        (col("Data").isNotNull())
    )

def clean_hospital(df):
    return df.filter(
        (col("Internado").isin(0, 1)) &
        (col("Idade").between(0, 120)) &
        (col("Sexo").isin(0, 1)) &
        (col("CEP").isNotNull()) &
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
        (col("CEP").isNotNull()) &
        (col("Data").isNotNull())
    )

def process_batch(msg, spark, producer):
    try:
        payload = json.loads(msg.value().decode('utf-8'))
        if 'batch' not in payload:
            print("Formato de mensagem inválido - sem campo 'batch'")
            return None

        # Determinar o tipo de dados pelo tópico
        data_type = None
        for dtype, topic in TOPICS_IN.items():
            if msg.topic() == topic:
                data_type = dtype
                break
        
        if not data_type:
            print(f"Tópico desconhecido: {msg.topic()}")
            return None

        # Processar o batch inteiro
        dados = payload['batch']
        df = spark.createDataFrame(dados, schema=SCHEMAS[data_type])
        
        # Aplicar a função de limpeza correta
        if data_type == 'secretary':
            df_limpo = clean_secretary(df)
        elif data_type == 'hospital':
            df_limpo = clean_hospital(df)
        elif data_type == 'oms':
            df_limpo = clean_oms(df)

        # Enviar como BATCH para o próximo estágio
        batch_limpo = [row.asDict() for row in df_limpo.collect()]
        if batch_limpo:
            producer.produce(
                TOPICS_OUT[data_type],
                json.dumps({"batch": batch_limpo}).encode('utf-8')
            )
        
        return len(batch_limpo)
        
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")
        return None

def main():
    spark = SparkSession.builder.appName("cleaner").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe(list(TOPICS_IN.values()))
    print(f"Inscrito nos tópicos: {list(TOPICS_IN.values())}. Aguardando mensagens...")

    try:
        msg_count = processed_count = 0
        start_time = time.time()
        
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Erro no consumidor Kafka: {msg.error()}")
                continue

            msg_count += 1
            result = process_batch(msg, spark, producer)
            
            if result is not None:
                processed_count += result
                producer.flush()
                consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print(f"\nEstatísticas: {processed_count}/{msg_count} mensagens processadas")
        print(f"Tempo total: {time.time() - start_time:.2f} segundos")
    finally:
        producer.flush()
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()