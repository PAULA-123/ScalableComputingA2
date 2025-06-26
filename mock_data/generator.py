import random
from datetime import datetime, timedelta
import os
import json
import time
from confluent_kafka import Producer

# ==================== Configurações ====================

minLinhas = int(os.getenv("MIN_LINHAS", 1))
maxLinhas = int(os.getenv("MAX_LINHAS", 5))
INTERVALO_CICLO = float(os.getenv("INTERVALO_CICLO", 9.0))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC_OMS = os.getenv("TOPIC_OMS", "raw_oms")
TOPIC_HOSPITAL = os.getenv("TOPIC_HOSPITAL", "raw_hospital")
TOPIC_SECRETARY = os.getenv("TOPIC_SECRETARY", "raw_secretary")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

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

# ==================== Função Kafka ====================

def kafka_send(topic, data):
    producer.produce(topic, json.dumps(data).encode('utf-8'))
    producer.flush()

# ==================== OMS ====================

def oms_generate_mock(rows=None, output_file="databases_mock/oms_mock.json"):
    rows = rows or random.randint(minLinhas, maxLinhas)
    dados = []
    for _ in range(rows):
        populacao = random.randint(20_000, 2_000_000)
        vacinados = random.randint(int(0.4 * populacao), populacao)
        recuperados = random.randint(int(0.001 * populacao), int(0.05 * populacao))
        obitos = random.randint(0, int(0.002 * populacao))

        registro = {
            "N_obitos": obitos,
            "Populacao": populacao,
            "CEP": random.choice(cep_ilhas),
            "N_recuperados": recuperados,
            "N_vacinados": vacinados,
            "Data": gerar_data_aleatoria_na_semana()
        }
        dados.append(registro)
        kafka_send(TOPIC_OMS, registro)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    print(f"[OMS] {rows} registros enviados para tópico '{TOPIC_OMS}'")

# ==================== HOSPITAL ====================

def hospital_generate_mock(rows=None, output_file="databases_mock/hospital_mock.json"):
    rows = rows or random.randint(minLinhas, maxLinhas)
    dados = []
    for _ in range(rows):
        idade = random.choices(
            population=range(0, 100),
            weights=[1]*15 + [2]*20 + [3]*25 + [2]*20 + [1]*20,
            k=1
        )[0]
        internado = int(idade > 65 or random.random() < 0.1)
        sintomas = [int(random.random() < 0.3) for _ in range(4)]

        registro = {
            "ID_Hospital": random.randint(1, 5),
            "Data": gerar_data_aleatoria_na_semana(),
            "Internado": internado,
            "Idade": idade,
            "Sexo": random.choice([0, 1]),
            "CEP": random.choice(cep_regioes),
            "Sintoma1": sintomas[0],
            "Sintoma2": sintomas[1],
            "Sintoma3": sintomas[2],
            "Sintoma4": sintomas[3],
        }
        dados.append(registro)
        kafka_send(TOPIC_HOSPITAL, registro)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    print(f"[HOSPITAL] {rows} registros enviados para tópico '{TOPIC_HOSPITAL}'")

# ==================== SECRETARIA ====================

def secretary_generate_mock(rows=None, output_file="databases_mock/secretary_mock.json"):
    rows = rows or random.randint(minLinhas, maxLinhas)
    dados = []
    for _ in range(rows):
        populacao = random.randint(10_000, 2_000_000)
        vacinado = int(random.random() < 0.7)
        diagnostico = int(random.random() < 0.05)  # 5% infectados

        registro = {
            "Diagnostico": diagnostico,
            "Vacinado": vacinado,
            "CEP": random.choice(cep_regioes),
            "Escolaridade": random.choices([0, 1, 2, 3, 4, 5], weights=[5, 10, 25, 30, 20, 10])[0],
            "Populacao": populacao,
            "Data": gerar_data_aleatoria_na_semana()
        }
        dados.append(registro)
        kafka_send(TOPIC_SECRETARY, registro)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    print(f"[SECRETARIA] {rows} registros enviados para tópico '{TOPIC_SECRETARY}'")

# ==================== LOOP ====================

if __name__ == "__main__":
    os.makedirs("databases_mock", exist_ok=True)
    print("Mock generator rodando em loop contínuo (CTRL+C para parar)\n")
    ciclo = 0
    while True:
        ciclo += 1
        print(f"=== CICLO {ciclo} INICIADO ===", flush=True)
        oms_generate_mock()
        secretary_generate_mock()
        hospital_generate_mock()
        print(f"=== CICLO {ciclo} COMPLETO, dormindo {INTERVALO_CICLO}s ===\n", flush=True)
        time.sleep(INTERVALO_CICLO)
