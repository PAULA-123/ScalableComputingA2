import json
import os
import time
from statistics import mean
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import StructType, StructField, IntegerType
# import requests

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_metricas_group"
SOURCE_TOPIC = "filtered_secretary"
OUTPUT_FILE = "databases_mock/resultados_metrica.json"

###########################################################
# O ERRO É QUE A API NÃO FICA ONLINE [METRICA] Aviso: não conseguiu conectar na API.
# tratador-metrica-1    | [METRICA] Iniciando tratador de métricas com Spark
###########################################################

import requests

API_URL = "http://api:8000/metricas"  # Ajuste conforme a URL da API

def enviar_resultado_api(resultados):
    try:
        # Convertendo dict para lista de modelos compatíveis
        response = requests.post(API_URL, json=resultados)
        if response.status_code == 200:
            print("[METRICA] Resultados enviados para API com sucesso")
        else:
            print(f"[METRICA] Falha ao enviar resultados: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[METRICA] Erro ao enviar resultados para API: {e}")

# Schema para os dados de entrada
schema = StructType([
    StructField("Diagnostico", IntegerType(), True)
])

def salvar_resultado(resultados):
    try:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(resultados, f, indent=4)
    except Exception as e:
        print(f"[ERRO] Falha ao salvar arquivo JSON: {e}")

def main():

    ### API NUNCA FICA ONLINE
    # def esperar_api_online(url, tentativas=10, intervalo=2):
    #     for _ in range(tentativas):
    #         try:
    #             r = requests.get(url)
    #             if r.status_code == 200:
    #                 print("[METRICA] API está online.")
    #                 return
    #         except Exception:
    #             pass
    #         print("[METRICA] Aguardando API ficar online...")
    #         time.sleep(intervalo)
    #     print("[METRICA] Aviso: não conseguiu conectar na API.")

    # # dentro do main, antes de criar SparkSession:
    # esperar_api_online("http://api:8000/metricas")
    print("[METRICA] Iniciando tratador de métricas com Spark")
    spark = SparkSession.builder.appName("tratador_metricas").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([SOURCE_TOPIC])
    resultados = []
    acumulado = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[METRICA] Erro no Kafka: {msg.error()}")
                    continue

            try:
                dado = json.loads(msg.value().decode("utf-8"))
                
                print(f"[METRICA] Dado recebido do Kafka: {dado}", flush=True)  # <-- NOVO PRINT
                
                if "Diagnostico" in dado and dado["Diagnostico"] is not None:
                    try:
                        dado["Diagnostico"] = int(dado["Diagnostico"])
                    except Exception as e:
                        print(f"[METRICA] Ignorando dado com Diagnostico inválido: {dado['Diagnostico']} ({e})")
                        continue

                    acumulado.append(dado)

                    if len(acumulado) % 5 == 0:
                        df = spark.createDataFrame(acumulado, schema=schema)
                        media_row = df.select(avg(col("Diagnostico")).alias("media")).collect()[0]
                        media = media_row["media"]

                        resultado = {
                            "quantidade": len(acumulado),
                            "media_diagnostico": round(media, 4)
                        }
                        resultados.append(resultado)

                        print(f"[METRICA] Resultado #{len(resultados)}: {resultado}")

                        salvar_resultado(resultados)
                        # COM OS COMENTÁRIOS FUNCIONA MAS SEM NÃO
                        #################################
                        enviar_resultado_api(resultados)
                        #################################

                consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"[METRICA] Erro ao processar mensagem: {e}")

    except KeyboardInterrupt:
        print("\n[METRICA] Interrompido pelo usuário")
    finally:
        consumer.close()
        spark.stop()
        print("[METRICA] Tratador finalizado")


if __name__ == "__main__":
    main()
