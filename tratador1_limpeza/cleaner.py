import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_limpeza_group"
SOURCE_TOPIC = "raw_secretary"
DEST_TOPIC = "clean_secretary"

# Schema mais completo para validação
schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

def limpeza(df):
    """Aplica regras de limpeza mais robustas"""
    df_limpo = df.filter(
        (col("Diagnostico").isin(0, 1)) &  # Apenas 0 ou 1
        (col("Vacinado").isin(0, 1)) &
        (col("CEP").isNotNull()) &
        (col("CEP") >= 11001) &  # CEP mínimo válido
        (col("CEP") <= 30999) &  # CEP máximo válido
        (col("Escolaridade").between(0, 5)) &  # Valores válidos
        (col("Populacao") > 0) &
        (col("Data").isNotNull()))
    return df_limpo

def process_message(msg, spark, producer):
    """Processa uma mensagem individual com tratamento de erros"""
    try:
        dado = json.loads(msg.value().decode('utf-8'))
        
        # Validação básica antes de criar DataFrame
        if not all(key in dado for key in ["Diagnostico", "Populacao"]):
            print("🗑️ Registro incompleto descartado")
            return None
            
        df = spark.createDataFrame([dado], schema=schema)
        df_limpo = limpeza(df)
        
        if df_limpo.count() > 0:
            dado_limpo = df_limpo.first().asDict()
            producer.produce(
                DEST_TOPIC,
                json.dumps(dado_limpo).encode('utf-8')
            )
            return True
        return False
        
    except json.JSONDecodeError as e:
        print(f"❌ Erro ao decodificar JSON: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
    return None

def main():
    spark = SparkSession.builder.appName("tratador_limpeza").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([SOURCE_TOPIC])
    print(f"🔍 Inscrito no tópico {SOURCE_TOPIC}. Aguardando mensagens...")

    try:
        msg_count = processed_count = 0
        start_time = time.time()
        batch_size = 100
        batch_count = 0
        
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Erro no consumidor Kafka: {msg.error()}")
                continue

            msg_count += 1
            result = process_message(msg, spark, producer)
            
            if result is not None:
                if result:
                    processed_count += 1
                batch_count += 1
                
                if batch_count >= batch_size:
                    producer.flush()
                    consumer.commit(asynchronous=False)
                    batch_count = 0

    except KeyboardInterrupt:
        print(f"\n📊 Estatísticas: {processed_count}/{msg_count} mensagens processadas")
        print(f"⏱️  Tempo total: {time.time() - start_time:.2f} segundos")
    finally:
        producer.flush()
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()