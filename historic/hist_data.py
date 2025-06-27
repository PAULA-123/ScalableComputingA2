"""
Este script é responsável por simular um volume histórico de dados em larga escala
para as entidades associadas ao Hospital e à Secretaria de Saúde.

Diferentemente do script 'generator.py', que simula dados em tempo real com menor volume,
este script é focado em gerar uma carga maior de dados, ideal para testar o balanceamento
de carga e a escalabilidade dos tratadores que consomem essas mensagens.
"""

import random
from datetime import datetime, timedelta
import json


minLinhas = 20000
maxLinhas = 50000

# Códigos das regiões (CEP)
cep_ilhas = list(range(11, 31))
cep_regioes = []
for ilha in cep_ilhas:
    cep_regioes += [int(f"{ilha:02d}{i:03d}") for i in range(1, 6)]

# Geração das datas possíveis
def gerar_data_aleatoria_na_semana():
    hoje = datetime.today()
    segunda = hoje - timedelta(days=hoje.weekday())
    dia = random.randint(0, 200)
    return (segunda + timedelta(days=dia)).strftime("%d-%m-%Y")

# Gerar o arquivo que simula o histórico de dados de Hospital
def hospital_generate_mock_local(rows=None, output_file="hospital_mock.json"):
    rows = rows or random.randint(minLinhas, maxLinhas)
    dados = []

    # gerando cada linha
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

    # salvando em um arquivo local
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)

# Gerar o arquivo que simula o histórico de dados de Secretária
def secretary_generate_mock_local(rows=None, output_file="secretary_mock.json"):
    rows = rows or random.randint(minLinhas, maxLinhas)
    dados = []

    # gera cada linha
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

    # salva em um arquivo local
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4, ensure_ascii=False)
    print(f"[LOCAL SECRETARIA] {rows} registros salvos em '{output_file}'")

# Chama as funções de simular histórico
if __name__ == "__main__":
    hospital_generate_mock_local()
    secretary_generate_mock_local()
    print("Geração de histórico concluída!")