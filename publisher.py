import redis
import json
import time
import random
from datetime import datetime, timedelta

r = redis.Redis(host='localhost', port=6379, db=0)
STREAM_KEY = "data_stream"

# CEPs simulados
cep_ilhas = list(range(11, 31))
cep_regioes = [int(f"{ilha:02d}{i:03d}") for ilha in cep_ilhas for i in range(1, 6)]

# Datas aleatórias
def gerar_data_aleatoria_na_semana():
    hoje = datetime.today()
    inicio_semana = hoje - timedelta(days=hoje.weekday())
    dia_aleatorio = random.randint(0, 6)
    data_aleatoria = inicio_semana + timedelta(days=dia_aleatorio)
    return data_aleatoria.strftime("%d-%m-%Y")

# Geração de dados
def gerar_dado_hospital():
    return {
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

def gerar_dado_secretaria():
    return {
        "Diagnostico": random.choice([0, 1]),
        "Vacinado": random.choice([0, 1]),
        "CEP": random.choice(cep_regioes),
        "Escolaridade": random.randint(0, 5),
        "Populacao": random.randint(1000, 1000000),
        "Data": gerar_data_aleatoria_na_semana()
    }

def gerar_dado_oms():
    populacao = random.randint(1000, 1000000)
    return {
        "N_obitos": random.randint(0, 1000),
        "Populacao": populacao,
        "CEP": random.choice(cep_ilhas),
        "N_recuperados": random.randint(0, 5000),
        "N_vacinados": random.randint(0, populacao),
        "Data": gerar_data_aleatoria_na_semana()
    }

def gerar_dado_completo():
    origem = random.choice(["hospital", "secretary", "oms"])
    if origem == "hospital":
        return gerar_dado_hospital(), origem
    elif origem == "secretary":
        return gerar_dado_secretaria(), origem
    else:
        return gerar_dado_oms(), origem

# Publisher
def publish():
    while True:
        payload, origin = gerar_dado_completo()
        message = {
            "payload": json.dumps(payload),
            "origin": origin,
            "stage": "raw"
        }
        msg_id = r.xadd(STREAM_KEY, message)
        print(f"Publicado {msg_id} - origin: {origin}, payload: {payload}")
        time.sleep(5)

if __name__ == "__main__":
    publish()
