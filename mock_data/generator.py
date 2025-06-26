import random
from datetime import datetime, timedelta
import os
import json
import time
from confluent_kafka import Producer

# ==================== Configurações ====================

minLinhas = int(os.getenv("MIN_LINHAS", 50))
maxLinhas = int(os.getenv("MAX_LINHAS", 75))
INTERVALO_CICLO = float(os.getenv("INTERVALO_CICLO", 100))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC_OMS = os.getenv("TOPIC_OMS", "raw_oms_batch")
TOPIC_HOSPITAL = os.getenv("TOPIC_HOSPITAL", "raw_hospital")
TOPIC_SECRETARY = os.getenv("TOPIC_SECRETARY", "raw_secretary")


TOPIC_HOSPITAL_b = os.getenv("TOPIC_HOSPITAL_b", "raw_hospital_b")
TOPIC_SECRETARY_b = os.getenv("TOPIC_SECRETARY_b", "raw_secretary_b")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

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
    return (segunda + timedelta(days=dia)).strftime("%d-%m-%Y")

# ==================== Função para enviar mensagens para Kafka

def kafka_send(topic, data):
    producer.produce(topic, json.dumps(data).encode('utf-8'))
    producer.flush()

# ==================== OMS ====================

def oms_generate_mock_batch(rows=None, output_file="databases_mock/oms_mock.json"):
    rows = rows or random.randint(minLinhas, maxLinhas)
    batch_size = rows  # ou: random.randint(100, 300)
    
    dados = []
    batch = []

    for i in range(rows):
        populacao = random.randint(1000, 1_000_000)
        registro = {
            "N_obitos": random.randint(0, 1000),
            "Populacao": populacao,
            "CEP": random.choice(cep_ilhas),
            "N_recuperados": random.randint(0, 5000),
            "N_vacinados": random.randint(0, populacao),
            "Data": gerar_data_aleatoria_na_semana()
        }
        dados.append(registro)
        batch.append(registro)

    mensagem = {"batch": dados, "source": "oms", "type": "normal"}
    kafka_send(TOPIC_OMS, mensagem)

    print(f"[OMS] {rows} registros enviados em batch de tamanho {batch_size} para '{TOPIC_OMS}'", flush=FLUSH)



# ==================== HOSPITAL ====================

def hospital_generate_mock_batch(rows=None, output_file="databases_mock/hospital_mock.json"):
    # manda mensagem por batch
    rows = random.randint(minLinhas, maxLinhas)
    dados = []

    for _ in range(rows):

        registro = {
            "ID_Hospital": random.randint(1, 5),
            "Data": gerar_data_aleatoria_na_semana(),
            "Internado": random.choice([0, 1]),
            "Idade": random.randint(0, 100),
            "Sexo": random.choice([0, 1]),
            "CEP": random.choice(cep_regioes),
            "Sintoma1": random.randint(0, 1),
            "Sintoma2": random.randint(0, 1),
            "Sintoma3": random.randint(0, 1),
            "Sintoma4": random.randint(0, 1),
        }

        dados.append(registro)

    mensagem = {"batch": dados, "source": "hospital", "type": "normal"}
    kafka_send(TOPIC_HOSPITAL_b, mensagem)
    print(f"[HOSPITAL] {rows} registros enviados para tópico '{TOPIC_HOSPITAL_b}'", flush=FLUSH)

def hospital_generate_mock(rows=None, output_file="databases_mock/hospital_mock.json"):
    rows = rows or 100
    dados = []
    for _ in range(rows):
        registro = {
            "ID_Hospital": random.randint(1, 5),
            "Data": gerar_data_aleatoria_na_semana(),
            "Internado": random.choice([0, 1]),
            "Idade": random.randint(0, 100),
            "Sexo": random.choice([0, 1]),
            "CEP": random.choice(cep_regioes),
            "Sintoma1": random.randint(0, 1),
            "Sintoma2": random.randint(0, 1),
            "Sintoma3": random.randint(0, 1),
            "Sintoma4": random.randint(0, 1),
        }
        dados.append(registro)
        kafka_send(TOPIC_HOSPITAL, registro)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    print(f"[HOSPITAL] {rows} registros enviados para tópico '{TOPIC_HOSPITAL}'")

# ==================== SECRETARIA ====================

def secretary_generate_mock_batch(rows=None, output_file="databases_mock/secretary_mock.json"):
    rows = rows or random.randint(minLinhas, maxLinhas)
    dados = []

    for _ in range(rows):
        registro = {
            "Diagnostico": random.choice([0, 1]),
            "Vacinado": random.choice([0, 1]),
            "CEP": random.choice(cep_regioes),
            "Escolaridade": random.randint(0, 5),
            "Populacao": random.randint(1000, 1_000_000),
            "Data": gerar_data_aleatoria_na_semana()
        }
        dados.append(registro)

    mensagem = {"batch": dados, "source": "secretary", "type": "normal"}
    kafka_send(TOPIC_SECRETARY_b, mensagem)
    print(f"[SECRETARIA] {rows} registros enviados para tópico '{TOPIC_SECRETARY_b}'")

def secretary_generate_mock(rows=None, output_file="databases_mock/secretary_mock.json"):
    rows = rows or random.randint(minLinhas, maxLinhas)
    dados = []
    for _ in range(rows):
        registro = {
            "Diagnostico": random.choice([0, 1]),
            "Vacinado": random.choice([0, 1]),
            "CEP": random.choice(cep_regioes),
            "Escolaridade": random.randint(0, 5),
            "Populacao": random.randint(1000, 1_000_000),
            "Data": gerar_data_aleatoria_na_semana()
        }
        dados.append(registro)
        kafka_send(TOPIC_SECRETARY, registro)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    print(f"[SECRETARIA] {rows} registros enviados para tópico '{TOPIC_SECRETARY}'")

# ==================== LOOP PRINCIPAL ====================

if __name__ == "__main__":
    os.makedirs("databases_mock", exist_ok=True)
    print("Mock generator rodando em loop contínuo (CTRL+C para parar)\n")
    ciclo = 0
    while True:
        ciclo += 1
        # print(f"=== CICLO {ciclo} INICIADO ===", flush=FLUSH)
        # oms_generate_mock_batch()
        secretary_generate_mock_batch()
        hospital_generate_mock_batch()
        # print(f"=== CICLO {ciclo} COMPLETO, dormindo {INTERVALO_CICLO}s ===\n", flush=FLUSH)
        time.sleep(INTERVALO_CICLO)
