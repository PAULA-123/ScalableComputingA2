"""
Este script é responsável por enviar dados históricos simulados em larga escala
para os tópicos Kafka configurados, com foco em testar o balanceamento de carga
e a escalabilidade dos tratadores que consomem essas mensagens.
"""

import json
import os
import time
from confluent_kafka import Producer

# Configurações
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# Tópicos separados para cada fonte de histórico
TOPIC_HOSPITAL_H = os.getenv("TOPIC_HOSPITAL_H", "raw_hospital_h")
TOPIC_SECRETARY_H = os.getenv("TOPIC_SECRETARY_H", "raw_secretary_h")

HOSPITAL_FILE = "hospital_mock.json"
SECRETARY_FILE = "secretary_mock.json"

CHUNK_SIZE = 1000

# Cria um produtor Kafka com compressão gzip (melhora desempenho em envios grandes)
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "compression.type": "gzip"
})

# Função de envio
def send_batch_in_chunks(data, topic, source_tag):
    """
    Envia os dados em lotes (chunks) para o tópico Kafka especificado.

    Parâmetros:
        data (list): lista de registros a serem enviados
        topic (str): nome do tópico Kafka
        source_tag (str): identificador da origem dos dados (ex: "hospital", "secretary")
    """
    total_chunks = (len(data) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(total_chunks):
        chunk = data[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE]

        # Mensagem padrão com batch
        message = {
            "batch": chunk,
            "source": source_tag,
            "type": "dados"
        }

        # Se for o último chunk, envia também o tipo fim_historico
        if i == total_chunks - 1:
            # Envia primeiro o último batch com dados
            producer.produce(topic, value=json.dumps(message).encode("utf-8"))
            producer.flush()
            print(f"[{source_tag.upper()}] Último chunk {i + 1} enviado com {len(chunk)} registros")

            # Envia mensagem final para avisar fim histórico
            fim_msg = {
                "batch": [],
                "source": source_tag,
                "type": "fim_historico"
            }
            producer.produce(topic, value=json.dumps(fim_msg).encode("utf-8"))
            print(f"[{source_tag.upper()}] Mensagem fim_historico enviada")
            producer.flush()
        else:
            # Envia batch normal
            producer.produce(topic, value=json.dumps(message).encode("utf-8"))
            print(f"[{source_tag.upper()}] Chunk {i + 1} enviado com {len(chunk)} registros")

    # Flush final para garantir envio
    producer.flush()

# Envio integral
def send_historical_data():
    """
    Lê os dados históricos dos arquivos JSON e envia para o Kafka em batches.
    """

    # Envia os dados do hospital
    try:
        with open(HOSPITAL_FILE, "r", encoding="utf-8") as f:
            hospital_data = json.load(f)
        send_batch_in_chunks(hospital_data, TOPIC_HOSPITAL_H, source_tag="hospital")
    except Exception as e:
        print("Erro ao ler ou enviar hospital_mock.json:", e)

    # Envia os dados da secretaria
    try:
        with open(SECRETARY_FILE, "r", encoding="utf-8") as f:
            secretary_data = json.load(f)
        send_batch_in_chunks(secretary_data, TOPIC_SECRETARY_H, source_tag="secretary")
    except Exception as e:
        print("Erro ao ler ou enviar secretary_mock.json:", e)

    # Garante que todas as mensagens foram enviadas
    producer.flush()
    print("Envio de dados históricos finalizado.")

# Execução
if __name__ == "__main__":
    send_historical_data()
