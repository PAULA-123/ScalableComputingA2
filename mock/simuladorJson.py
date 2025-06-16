import random
from datetime import datetime, timedelta
import os
import json


minLinhas = 50000 
maxLinhas = 75000


# ==================== CEPs ====================

cep_ilhas = list(range(11, 31))  # 20 ilhas (11 até 30)

cep_regioes = []
for cep_ilha_escolhida in cep_ilhas:
    cep_regioes.extend([int(f"{cep_ilha_escolhida:02d}{i:03d}") for i in range(1, 6)])


# ==================== Datas ====================

def gerar_data_aleatoria_na_semana():
    hoje = datetime.today()
    inicio_semana = hoje - timedelta(days=hoje.weekday())  # Segunda-feira
    dia_aleatorio = random.randint(0, 6)
    data_aleatoria = inicio_semana + timedelta(days=dia_aleatorio)
    return data_aleatoria.strftime("%d-%m-%Y")


# ==================== OMS ====================

def oms_generate_mock(rows=random.randint(minLinhas, maxLinhas), output_file="databases_mock/oms_mock.json"):
    dados = []

    for _ in range(rows):
        populacao = random.randint(1000, 1000000)
        registro = {
            "N_obitos": random.randint(0, 1000),
            "Populacao": populacao,
            "CEP": random.choice(cep_ilhas),
            "N_recuperados": random.randint(0, 5000),
            "N_vacinados": random.randint(0, populacao),
            "Data": gerar_data_aleatoria_na_semana()
        }
        dados.append(registro)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)


# ==================== HOSPITAL ====================

def hospital_generate_mock(rows=100, output_file="databases_mock/hospital_mock.json"):
    dados = []

    for _ in range(rows):
        registro = {
            "ID_Hospital": random.randint(1, 5),
            "Data": gerar_data_aleatoria_na_semana(),
            "Internado": random.choice([0, 1]),
            "Idade": random.randint(0, 100),
            "Sexo": random.choice([0, 1]),  # 1 = Feminino, 0 = Masculino
            "CEP": random.choice(cep_regioes),
            "Sintoma1": random.randint(0, 1),
            "Sintoma2": random.randint(0, 1),
            "Sintoma3": random.randint(0, 1),
            "Sintoma4": random.randint(0, 1),
        }
        dados.append(registro)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)


def gerar_multiplos_arquivos_hospital(qtde_arquivos=3, min_linhas=80, max_linhas=150):
    for i in range(1, qtde_arquivos + 1):
        num_linhas = random.randint(min_linhas, max_linhas)
        nome_arquivo = f"databases_mock/hospital_mock_{i}.json"
        hospital_generate_mock(rows=num_linhas, output_file=nome_arquivo)


# ==================== SECRETARIA ====================

def secretary_generate_mock(rows=random.randint(minLinhas, maxLinhas), output_file="databases_mock/secretary_mock.json"):
    dados = []

    for _ in range(rows):
        registro = {
            "Diagnostico": random.choice([0, 1]),
            "Vacinado": random.choice([0, 1]),
            "CEP": random.choice(cep_regioes),
            "Escolaridade": random.randint(0, 5),
            "Populacao": random.randint(1000, 1000000),
            "Data": gerar_data_aleatoria_na_semana()
        }
        dados.append(registro)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)


# ==================== Executar tudo ====================

os.makedirs("databases_mock", exist_ok=True)

oms_generate_mock()
secretary_generate_mock()
gerar_multiplos_arquivos_hospital(qtde_arquivos=10, min_linhas=minLinhas, max_linhas=maxLinhas)

print(f"Dados gerados em JSON: OMS, Secretaria e Hospitais.")
