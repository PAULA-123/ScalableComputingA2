"""
Este script consome batches de dados já agrupados da secretaria de saúde (pelo tópico Kafka `grouped_secretary`),
calcula a correlação entre escolaridade média e número total de vacinados por CEP (ou por CEP e data),
e envia o resultado para uma API REST para armazenamento ou visualização.

A correlação é útil para entender a relação entre variáveis sociodemográficas e vacinação.

Fluxo geral:
1. Consome mensagens no formato de batch JSON.
2. Converte para DataFrame com Spark.
3. Calcula a correlação entre 'media_escolaridade' e 'total_vacinados'.
4. Envia o valor calculado para uma API REST.
"""

import json
import requests
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType

# Configurações de conexão Kafka e API
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_correlacao_group"
SOURCE_TOPIC = "grouped_secretary"
API_URL = "http://api:8000/correlacao"

# Esquema dos dados esperados no batch agrupado
schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("total_diagnostico", FloatType(), True),
    StructField("media_escolaridade", FloatType(), True),
    StructField("media_populacao", FloatType(), True),
    StructField("data", StringType(), True),
    StructField("total_vacinados", FloatType(), True)
])

# Função para calcular a correlação e enviar para a API
def calcular_e_enviar(df):
    try:
        # Calcula a correlação de Pearson entre escolaridade média e total de vacinados
        esc_vac = df.stat.corr("media_escolaridade", "total_vacinados")
        print(f"Correlação Escolaridade x Vacinado: {esc_vac:.4f}")

        # Cria payload JSON para a API
        payload = [{"Escolaridade": round(esc_vac, 4), "Vacinado": round(esc_vac, 4)}]

        # Envia para a API REST
        try:
            response = requests.post(API_URL, json=payload)
            if response.status_code == 200:
                print("Correlação enviada para API com sucesso")
            else:
                print(f"Erro ao enviar para API: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Erro ao fazer requisição para API: {e}")
    except Exception as e:
        print(f"Erro ao calcular correlação: {e}")

# Função principal
def main():
    # Inicializa sessão Spark
    spark = SparkSession.builder.appName("tratador_correlacao_batch").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Inicializa consumidor Kafka
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",  
        "enable.auto.commit": False 
    })

    # Inscreve no tópico com os dados agrupados
    consumer.subscribe([SOURCE_TOPIC])

    try:
        while True:
            # Consome mensagem do Kafka
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {msg.error()}")
                continue

            try:
                # Converte mensagem Kafka para dicionário
                conteudo = json.loads(msg.value().decode("utf-8"))
                dados = conteudo.get("batch", [])


                # Se houver dados no batch, converte para DataFrame e calcula correlação
                if dados:
                    # Cria RDD e aplica schema
                    df = spark.read.schema(schema).json(
                        spark.sparkContext.parallelize([json.dumps(d) for d in dados])
                    )
                    calcular_e_enviar(df)
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"Erro ao processar mensagem Kafka: {e}")

    except KeyboardInterrupt:
        print("Interrompido pelo usuário")
    finally:
        consumer.close()
        spark.stop()

# Ponto de entrada
if __name__ == "__main__":
    main()
