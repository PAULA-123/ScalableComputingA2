import json
import requests
import os
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType

# Configurações de conexão Kafka e API
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "tratador_correlacao_group"
SOURCE_TOPICS = ["grouped_secretary", "merge_hospital_secretary"]
API_URL = "http://api:8000/correlacao"

# Esquema dos dados esperados no batch agrupado
schema = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("total_diagnosticos", IntegerType(), True),
    StructField("media_escolaridade", FloatType(), True),
    StructField("total_vacinados", IntegerType(), True),
    StructField("populacao", IntegerType(), True)
])

def salvar_resultado_em_json(payload, output_path="/app/databases_mock/correlacao.json"):
    try:
        # Garante que o diretório existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Se o arquivo já existe, carrega o conteúdo atual
        if os.path.exists(output_path):
            try:
                with open(output_path, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                # Verifica se é uma lista para poder fazer extend
                if isinstance(existing_data, list):
                    existing_data.extend(payload)
                    payload = existing_data
                else:
                    # Se não for lista, cria uma nova lista com os dados existentes e os novos
                    payload = [existing_data] + payload
            except json.JSONDecodeError:
                # Se o arquivo estiver corrompido, começa do zero
                payload = payload
        else:
            # Se o arquivo não existe, payload permanece como está
            pass

        # Escreve os dados no arquivo
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4, ensure_ascii=False)
        
        print(f"Resultado salvo com sucesso em {output_path}")
        return True
    except Exception as e:
        print(f"Erro ao salvar resultado em JSON: {str(e)}")
        return False

def calcular_e_enviar(df):
    try:
        # Verifica se os desvios padrões são diferentes de zero
        stats = df.selectExpr(
            "stddev(Media_Escolaridade) as std_esc",
            "stddev(Total_Vacinados) as std_vac"
        ).collect()[0]

        std_esc = stats['std_esc']
        std_vac = stats['std_vac']

        if std_esc == 0 or std_vac == 0 or std_esc is None or std_vac is None:
            print(f"Impossível calcular correlação: desvio padrão zero ou nulo - Escolaridade={std_esc}, Vacinado={std_vac}")
            return

        # Calcula a correlação de Pearson entre escolaridade média e total de vacinados
        esc_vac = df.stat.corr("Media_Escolaridade", "Total_Vacinados")
        print(f"Correlação Escolaridade x Vacinado: {esc_vac:.4f}")

        # Cria payload JSON para a API
        payload = {
            "correlacao_escolaridade_vacinacao": round(esc_vac, 4),
            "timestamp": datetime.now().isoformat()
        }
        
        # Salva localmente
        if not salvar_resultado_em_json([payload]):
            print("Falha ao salvar resultado localmente")
        
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

# ... [mantém o código acima igual até a função main] ...

def main():
    print("\n[INÍCIO] Tratador de correlação iniciado")

    # Inicializa sessão Spark
    spark = SparkSession.builder \
        .appName("tratador_correlacao_batch") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Inicializa consumidor Kafka
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(SOURCE_TOPICS)
    print(f"[KAFKA] Subscrito aos tópicos: {SOURCE_TOPICS}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[KAFKA ERRO] {msg.error()}")
                continue

            print("\n[MENSAGEM] Mensagem recebida")

            try:
                raw = msg.value().decode("utf-8")
                print(f"[DEBUG] Conteúdo bruto da mensagem: {raw[:150]}...")  # Limita para evitar poluição

                conteudo = json.loads(raw)
                dados = conteudo.get("batch", [])
                origem = conteudo.get("source", "desconhecida")
                print(f"[DEBUG] Origem: {origem} | Tamanho do batch: {len(dados)}")

                if dados:
                    print(f"[PROCESSAMENTO] Iniciando com {len(dados)} registros")

                    # Converte para DataFrame Spark
                    df = spark.createDataFrame(dados, schema=schema)
                    print(f"[SPARK] Schema do DataFrame:")
                    df.printSchema()

                    calcular_e_enviar(df)

                    # Commit manual do offset
                    consumer.commit(asynchronous=False)
                    print("[KAFKA] Offset commitado com sucesso")

                else:
                    print("[AVISO] Nenhum dado no batch")

            except json.JSONDecodeError as e:
                print(f"[ERRO JSON] Falha ao decodificar: {e}")
            except Exception as e:
                print(f"[ERRO PROCESSAMENTO] {e}")

    except KeyboardInterrupt:
        print("\n[PARADA] Interrompido pelo usuário")
    finally:
        print("[ENCERRAMENTO] Fechando consumidor e SparkSession")
        consumer.close()
        spark.stop()


if __name__ == "__main__":
    main()