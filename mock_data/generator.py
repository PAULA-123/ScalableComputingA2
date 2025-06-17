import random
from datetime import datetime, timedelta
import os
import json
import redis
import time

# ==================== Configurações ====================

minLinhas = int(os.getenv("MIN_LINHAS", 5000))
maxLinhas = int(os.getenv("MAX_LINHAS", 7500))

# Quantos segundos esperar entre ciclos
INTERVALO_CICLO = float(os.getenv("INTERVALO_CICLO", 2.0))

# Configurações do Redis (ajustável por variáveis de ambiente)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_OMS_CHANNEL = os.getenv("REDIS_OMS_CHANNEL", "oms")
REDIS_HOSPITAL_CHANNEL = os.getenv("REDIS_HOSPITAL_CHANNEL", "hospital")
REDIS_SECRETARY_CHANNEL = os.getenv("REDIS_SECRETARY_CHANNEL", "raw_secretary")

# Instância Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

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

# ==================== OMS ====================

def oms_generate_mock(rows=None, output_file="databases_mock/oms_mock.json"):
    rows = rows or random.randint(minLinhas, maxLinhas)
    dados = []
    for _ in range(rows):
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
        r.publish(REDIS_OMS_CHANNEL, json.dumps(registro))
    # salvar localmente (sobrescreve a cada ciclo)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    print(f"[OMS] {rows} registros publicados em '{REDIS_OMS_CHANNEL}'")

# ==================== HOSPITAL ====================

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
        r.publish(REDIS_HOSPITAL_CHANNEL, json.dumps(registro))
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    print(f"[HOSPITAL] {rows} registros publicados em '{REDIS_HOSPITAL_CHANNEL}'")

# ==================== SECRETARIA ====================

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
        r.publish(REDIS_SECRETARY_CHANNEL, json.dumps(registro))
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    print(f"[SECRETARIA] {rows} registros publicados em '{REDIS_SECRETARY_CHANNEL}'")

# ==================== LOOP PRINCIPAL ====================

if __name__ == "__main__":
    os.makedirs("databases_mock", exist_ok=True)
    print("Mock generator rodando em loop contínuo (CTRL+C para parar)\n")
    ciclo = 0
    while True:
        ciclo += 1
        print(f"=== CICLO {ciclo} INICIADO ===")
        oms_generate_mock()
        secretary_generate_mock()
        # para hospitais, geramos múltiplos arquivos conforme antes:
        gerar_multiplos_arquivos_hospital = lambda: hospital_generate_mock()
        hospital_generate_mock()
        print(f"=== CICLO {ciclo} COMPLETO, dormindo {INTERVALO_CICLO}s ===\n")
        time.sleep(INTERVALO_CICLO)
