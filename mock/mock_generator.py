import redis
import random
import json
import time
from datetime import datetime, timedelta

# Redis Pub/Sub config
r = redis.Redis(host='redis', port=6379)
CANAL = "dados-saude"

# Dados simulados
cep_ilhas = list(range(11, 31))
cep_regioes = [int(f"{ilha:02d}{r:03d}") for ilha in cep_ilhas for r in range(1, 6)]

def gerar_dados_oms(rows=100):
    base_data = datetime.today()
    for _ in range(rows):
        yield {
            "origem": "oms",
            "nome_arquivo": "oms_virtual.txt",
            "num_obitos": random.randint(0, 1000),
            "populacao": random.randint(1000, 1000000),
            "cep": random.choice(cep_ilhas),
            "num_recuperados": random.randint(0, 5000),
            "num_vacinados": random.randint(0, 500000),
            "data": (base_data - timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y")
        }

def gerar_dados_hospital(rows=100):
    base_data = datetime.today()
    for _ in range(rows):
        yield {
            "origem": "hospital",
            "nome_arquivo": "hospital_virtual.csv",
            "id_hospital": random.randint(1, 5),
            "data": (base_data - timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y"),
            "internado": random.choice([1, 0]),
            "idade": random.randint(0, 100),
            "sexo": random.randint(0, 1),
            "cep": random.choice(cep_regioes),
            "sintoma1": random.choice([1, 0]),
            "sintoma2": random.choice([1, 0]),
            "sintoma3": random.choice([1, 0]),
            "sintoma4": random.choice([1, 0])
        }

def gerar_dados_secretaria(rows=100):
    base_data = datetime.today()
    for _ in range(rows):
        yield {
            "origem": "secretaria",
            "nome_arquivo": "secretaria_virtual.db",
            "diagnostico": random.choice([1, 0]),
            "vacinado": random.choice([1, 0]),
            "cep": random.choice(cep_regioes),
            "escolaridade": random.randint(0, 5),
            "populacao": random.randint(1000, 1000000),
            "data": (base_data - timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y"),
        }

def publicar(generator_func, nome):
    print(f"🔁 Publicando dados de: {nome}")
    for dado in generator_func():
        r.publish(CANAL, json.dumps(dado))
        print(f"📤 {nome}: {dado}")
        time.sleep(0.01)

def main():
    publicar(lambda: gerar_dados_oms(100), "OMS")
    publicar(lambda: gerar_dados_hospital(100), "Hospital")
    publicar(lambda: gerar_dados_secretaria(100), "Secretaria")
    print("✅ Publicação completa.")

if __name__ == "__main__":
    main()
