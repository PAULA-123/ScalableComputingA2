import json
import time
import os
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests

# Configurações
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_merge_group"
TOPIC_HOSPITAL = "filtered_hospital"  # Consome dados filtrados
TOPIC_SECRETARY = "filtered_secretary"  # Consome dados filtrados
OUTPUT_DIR = "/app/databases_mock"  #Caminho dentro do container
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "merge_batch.json")
API_URL = "http://api:8000/merge-cep"

# Schemas para dados filtrados
hospital_schema = StructType([
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

secretary_schema = StructType([
    StructField("Diagnostico", IntegerType(), True),
    StructField("Vacinado", IntegerType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("Escolaridade", IntegerType(), True),
    StructField("Populacao", IntegerType(), True),
    StructField("Data", StringType(), True)
])

async def enviar_para_api(dados):
    try:
        if not await verificar_api():
            print("[ERRO] API não está disponível")
            return False
            
        response = requests.post(API_URL, json=dados, timeout=10)
        response.raise_for_status()
        print(f"[API] Dados enviados. Status: {response.status_code}")
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao enviar para API: {str(e)}")
        return False

async def verificar_api():
    try:
        response = requests.get(f"{API_URL.replace('/merge-cep', '')}", timeout=5)
        return response.status_code == 200
    except Exception as e:
        print(f"[ERRO] API não disponível: {str(e)}")
        return False

def salvar_resultado(dados):
    try:
        print(f"[ARQUIVO] Tentando salvar em {OUTPUT_FILE}")
        
        # Garante que o diretório existe
        os.makedirs(OUTPUT_DIR, exist_ok=True, mode=0o777)
        
        # Teste de escrita
        test_file = os.path.join(OUTPUT_DIR, "test_write.tmp")
        with open(test_file, "w") as f:
            f.write("test")
        os.remove(test_file)
        
        # Escreve em arquivo temporário primeiro
        temp_file = OUTPUT_FILE + ".tmp"
        with open(temp_file, "w") as f:
            json.dump(dados, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        
        # Move atomicamente
        os.replace(temp_file, OUTPUT_FILE)
        print(f"[ARQUIVO] Dados salvos com sucesso em {OUTPUT_FILE}")
        return True
    except Exception as e:
        print(f"[ERRO ARQUIVO] Falha ao salvar: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def processar_batch(dados_hosp, dados_secr, spark):
    # Criar DataFrames
    df_hosp = spark.createDataFrame(dados_hosp, schema=hospital_schema)
    df_secr = spark.createDataFrame(dados_secr, schema=secretary_schema)

    # Agregações por CEP
    agg_hosp = df_hosp.groupBy("CEP").agg(
        spark_sum("Internado").alias("Total_Internados"),
        spark_sum("Idade").alias("Soma_Idade"),
        spark_sum("Sintoma1").alias("Total_Sintoma1"),
        spark_sum("Sintoma2").alias("Total_Sintoma2"),
        spark_sum("Sintoma3").alias("Total_Sintoma3"),
        spark_sum("Sintoma4").alias("Total_Sintoma4")
    )

    agg_secr = df_secr.groupBy("CEP").agg(
        spark_sum("Diagnostico").alias("Total_Diagnosticos"),
        spark_sum("Vacinado").alias("Total_Vacinados"),
        spark_sum("Escolaridade").alias("Soma_Escolaridade"),
        spark_sum("Populacao").alias("Soma_Populacao")
    )

    # Merge e tratamento de nulos
    merged = agg_hosp.join(agg_secr, on="CEP", how="outer").fillna(0)
    return [row.asDict() for row in merged.collect()]

def main():
    print("\n=== INICIANDO TRATADOR DE MERGE (BATCH) ===")
    spark = SparkSession.builder \
        .appName("TratadorMerge") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC_HOSPITAL, TOPIC_SECRETARY])
    print(f"Inscrito nos tópicos: {TOPIC_HOSPITAL}, {TOPIC_SECRETARY}")

    try:
        dados_hosp, dados_secr = [], []
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Erro no consumidor: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                batch = payload.get("batch", [])
                
                if msg.topic() == TOPIC_HOSPITAL:
                    dados_hosp.extend(batch)
                    print(f"[HOSPITAL] +{len(batch)} registros (total: {len(dados_hosp)})")
                elif msg.topic() == TOPIC_SECRETARY:
                    dados_secr.extend(batch)
                    print(f"[SECRETARIA] +{len(batch)} registros (total: {len(dados_secr)})")

                # Processar quando tiver dados de ambos
                if dados_hosp and dados_secr:
                    resultado = processar_batch(dados_hosp, dados_secr, spark)
                    salvar_resultado(resultado)
                    enviar_para_api(resultado)
                    print(f"[MERGE] Batch processado - {len(resultado)} CEPs consolidados")
                    dados_hosp, dados_secr = [], []  # Reset
                    consumer.commit()

            except Exception as e:
                print(f"Erro no processamento: {str(e)}")

    except KeyboardInterrupt:
        print("\n=== FINALIZANDO CONSUMER ===")
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()