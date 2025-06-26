import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Configurações Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "filtrador_group"

# Tópicos de entrada (clean) e saída (filtered)
TOPICS_CONFIG = {
    'secretary': {
        'in': 'clean_secretary',
        'out': 'filtered_secretary',
        'schema': StructType([
            StructField("Diagnostico", IntegerType(), True),
            StructField("Vacinado", IntegerType(), True),
            StructField("CEP", IntegerType(), True),
            StructField("Escolaridade", IntegerType(), True),
            StructField("Populacao", IntegerType(), True),
            StructField("Data", StringType(), True)
        ])
    },
    'hospital': {
        'in': 'clean_hospital',
        'out': 'filtered_hospital',
        'schema': StructType([
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
    },
    'oms': {
        'in': 'clean_oms',
        'out': 'filtered_oms',
        'schema': StructType([
            StructField("N_obitos", IntegerType(), True),
            StructField("Populacao", IntegerType(), True),
            StructField("CEP", IntegerType(), True),
            StructField("N_recuperados", IntegerType(), True),
            StructField("N_vacinados", IntegerType(), True),
            StructField("Data", StringType(), True)
        ])
    }
}

# Funções de filtro específicas para cada tipo
def filter_secretary(df):
    """Filtra dados da secretaria com regras específicas"""
    return df.filter(
        (col("Populacao") > 500) &
        (col("Diagnostico").isNotNull()) &
        (col("CEP").isNotNull()) 
        # ((col("Vacinado") == 1) | (col("Diagnostico") == 1))
    )

def filter_hospital(df):
    """Filtra dados hospitalares com regras específicas"""
    return df.filter(
        (col("Internado") == 1) &      # Apenas pacientes internados
        (col("Idade") >= 18) &         # Apenas adultos
        (
            (col("Sintoma1") == 1) |   # Com pelo menos um sintoma
            (col("Sintoma2") == 1) |
            (col("Sintoma3") == 1) |
            (col("Sintoma4") == 1)
        )
    )
    
def filter_oms(df):
    filtered = df
    # filtered = df.filter(
    #     (col("N_obitos") > 0) | # Apenas com óbitos ou
    #     (col("N_recuperados") > 0) # recuperados
    # )
    print(f"[OMS] Enviando {filtered.count()} registros para filtered_oms")
    return filtered

def process_batch(msg, spark, producer):
    """Processa um batch completo de mensagens"""
    try:
        payload = json.loads(msg.value().decode('utf-8'))
        if 'batch' not in payload:
            print("Formato inválido: mensagem sem campo 'batch'")
            return None

        # Identifica o tipo de dados pelo tópico
        data_type = next((k for k, v in TOPICS_CONFIG.items() if v['in'] == msg.topic()), None)
        
        if not data_type:
            print(f"Tópico desconhecido: {msg.topic()}")
            return None

        config = TOPICS_CONFIG[data_type]
        dados = payload['batch']
        
        # Cria DataFrame e aplica filtro
        df = spark.createDataFrame(dados, schema=config['schema'])
        
        if data_type == 'secretary':
            df_filtrado = filter_secretary(df)
        elif data_type == 'hospital':
            df_filtrado = filter_hospital(df)
        elif data_type == 'oms':
            df_filtrado = filter_oms(df)

        # Prepara e envia o batch filtrado
        batch_filtrado = [row.asDict() for row in df_filtrado.collect()]
        
        if batch_filtrado:
            producer.produce(
                config['out'],
                json.dumps({"batch": batch_filtrado}).encode('utf-8')
            )
            print(f"[{data_type.upper()}] Batch filtrado enviado - {len(batch_filtrado)} registros")
            return len(batch_filtrado)
        
        print(f"[{data_type.upper()}] Batch vazio após filtragem")
        return 0
        
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
    except Exception as e:
        print(f"Erro inesperado ao processar batch: {e}")
    return None

def main():
    spark = None
    
    # Configuração do Kafka Consumer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000
    })

    # Configuração do Kafka Producer
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "message.timeout.ms": 5000,
        "queue.buffering.max.messages": 100000
    })

    # Inscreve nos tópicos de entrada
    topics_in = [v['in'] for v in TOPICS_CONFIG.values()]
    consumer.subscribe(topics_in)
    print(f"Inscrito nos tópicos: {topics_in}\nAguardando mensagens...")

    try:
        # Configuração do Spark
        spark = SparkSession.builder \
            .appName("filtrador") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("ERROR")
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production")

        batch_count = total_processed = 0
        start_time = time.time()
        batch_messages = []
        BATCH_SIZE = 50  # Número de mensagens para agrupar antes de processar

        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                if batch_messages:  # Processa mensagens pendentes
                    processed = sum(process_batch(m, spark, producer) or 0 for m in batch_messages)
                    total_processed += processed
                    batch_count += 1
                    producer.flush()
                    consumer.commit()
                    batch_messages = []
                    print(f"Lote {batch_count} processado - {processed} registros")
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Erro no consumidor: {msg.error()}")
                continue

            batch_messages.append(msg)
            if len(batch_messages) >= BATCH_SIZE:
                processed = sum(process_batch(m, spark, producer) or 0 for m in batch_messages)
                total_processed += processed
                batch_count += 1
                producer.flush()
                consumer.commit()
                batch_messages = []
                print(f"Lote {batch_count} processado - {processed} registros")

    # except KeyboardInterrupt:
    #     print("\nInterrupção solicitada")
    except Exception as e:
        print(f"Erro crítico: {str(e)}")
    finally:
        # Estatísticas finais
        duration = time.time() - start_time
        print(f"\nEstatísticas finais:")
        print(f"- Total de lotes processados: {batch_count}")
        print(f"- Total de registros filtrados: {total_processed}")
        print(f"- Tempo total: {duration:.2f} segundos")
        print(f"- Throughput: {total_processed/max(duration, 1):.2f} registros/segundo")

        # Libera recursos
        producer.flush()
        consumer.close()
        if spark:
            spark.stop()
        print("Recursos liberados")

if __name__ == "__main__":
    main()