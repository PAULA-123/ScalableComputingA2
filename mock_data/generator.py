import random
from datetime import datetime, timedelta
import os
import json
import time
import socket
from confluent_kafka import Producer

# ==================== Aguardar Kafka ====================

def aguardar_kafka(host="kafka", port=9092, timeout=30):
    print(f"⏳ Aguardando Kafka em {host}:{port}...", flush=True)
    inicio = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"✅ Kafka disponível em {host}:{port}", flush=True)
                return
        except OSError:
            if time.time() - inicio > timeout:
                raise TimeoutError(f"❌ Kafka não respondeu após {timeout} segundos")
            time.sleep(1)

# ==================== Configurações ====================

minLinhas = int(os.getenv("MIN_LINHAS", 50))
maxLinhas = int(os.getenv("MAX_LINHAS", 75))
INTERVALO_CICLO = float(os.getenv("INTERVALO_CICLO", 3.0))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC_OMS = os.getenv("TOPIC_OMS", "raw_oms_batch")
TOPIC_HOSPITAL = os.getenv("TOPIC_HOSPITAL", "raw_hospital")
TOPIC_SECRETARY = os.getenv("TOPIC_SECRETARY", "raw_secretary")

producer = None
FLUSH = True

# ==================== CEPs ====================

cep_ilhas = list(range(11, 31))
cep_regioes = []
for ilha in cep_ilhas:
    cep_regioes += [int(f"{ilha:02d}{i:03d}") for i in range(1, 6)]

# ==================== Datas ====================

def gerar_data_aleatoria_na_semana():
    hoje = datetime.today()
    segunda = hoje - timedelta(days=hoje.weekday())
    dia = random.randint(0, 6)
    return (segunda + timedelta(days=dia)).strftime("%Y-%m-%d")

# ==================== Função para enviar mensagens para Kafka

def kafka_send(topic, data):
    producer.produce(topic, json.dumps(data).encode('utf-8'))
    producer.flush()

# ==================== Geração de dados ====================

def gerar_lote_oms(rows=None):
    rows = rows or random.randint(minLinhas, maxLinhas)
    batch = []
    for _ in range(rows):
        populacao = random.randint(1000, 1_000_000)
        batch.append({
            "N_obitos": random.randint(0, 1000),
            "Populacao": populacao,
            "CEP": random.choice(cep_ilhas),
            "N_recuperados": random.randint(0, 5000),
            "N_vacinados": random.randint(0, populacao),
            "Data": gerar_data_aleatoria_na_semana()
        })
    kafka_send(TOPIC_OMS, {"batch": batch})
    print(f"[OMS] {rows} registros enviados para '{TOPIC_OMS}'", flush=FLUSH)

def gerar_lote_hospital(rows=None):
    rows = random.randint(minLinhas, maxLinhas)
    batch = []
    for _ in range(rows):
        batch.append({
            "ID_Hospital": random.randint(1, 5),
            "Data": gerar_data_aleatoria_na_semana(),
            "Internado": random.choice([0, 1]),
            "Idade": random.randint(0, 100),
            "Sexo": random.choice([0, 1]),
            "CEP": random.choice(cep_regioes),
            "Sintoma1": random.randint(0, 1),
            "Sintoma2": random.randint(0, 1),
            "Sintoma3": random.randint(0, 1),
            "Sintoma4": random.randint(0, 1)
        })
    kafka_send(TOPIC_HOSPITAL, {"batch": batch})
    print(f"[HOSPITAL] {rows} registros enviados para '{TOPIC_HOSPITAL}'", flush=FLUSH)

def gerar_lote_secretaria(rows=None):
    rows = random.randint(minLinhas, maxLinhas)
    batch = []
    for _ in range(rows):
        batch.append({
            "Diagnostico": random.choice([0, 1]),
            "Vacinado": 1 if random.random() < 0.75 else 0, 
            "CEP": random.choice(cep_regioes),
            "Escolaridade": random.randint(0, 5),
            "Populacao": random.randint(1000, 10_000),
            "Data": gerar_data_aleatoria_na_semana()
        })
    kafka_send(TOPIC_SECRETARY, {"batch": batch})
    print(f"[SECRETARIA] {rows} registros enviados para '{TOPIC_SECRETARY}'", flush=FLUSH)

# ==================== LOOP PRINCIPAL ====================

if __name__ == "__main__":
    print("🚀 Mock generator rodando em loop contínuo (CTRL+C para parar)", flush=True)
    # Extrai host e porta da string kafka:9092
    host, port = KAFKA_BOOTSTRAP_SERVERS.split(":")
    aguardar_kafka(host=host, port=int(port))
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    while True:
        gerar_lote_secretaria()
        gerar_lote_oms()
        gerar_lote_hospital()
        time.sleep(INTERVALO_CICLO)