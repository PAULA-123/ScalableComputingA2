from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from pydantic import BaseModel
import random
from datetime import datetime, timedelta


# ===========================
# CONFIGURAÇÃO DA API
# ===========================
app = FastAPI(
    title="API de Dados Simulados para Dashboard",
    description="Retorna parâmetros da regressão e dados simulados para análises.",
    version="1.0.0"
)


# ===========================
# LIBERAR CORS
# ===========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite qualquer origem
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===========================
# ENDPOINT DE DADOS
# ===========================
@app.get("/dados")
def get_dados():
    # Parâmetros da regressão simulados
    beta0 = 1.5
    beta1 = 2.3

    regioes = ["Norte", "Nordeste", "Sul", "Sudeste", "Centro-Oeste"]

    data = []
    start_date = datetime(2022, 1, 1)

    for i in range(200):  # 200 registros simulados
        regiao = random.choice(regioes)
        date = start_date + timedelta(days=random.randint(0, 365))
        populacao = random.randint(50000, 300000)

        vacinados = int(populacao * random.uniform(0.4, 0.9))
        diagnosticados = random.randint(500, 8000)
        internacoes = int(diagnosticados * random.uniform(0.05, 0.2))
        escolaridade = round(random.uniform(3, 16), 1)  # média de anos de estudo

        # Dados para regressão linear
        x = round(random.uniform(0, 20), 2)
        noise = random.gauss(0, 2)
        y = beta1 * x + beta0 + noise

        data.append({
            "regiao": regiao,
            "data": date.strftime("%Y-%m-%d"),
            "populacao": populacao,
            "vacinados": vacinados,
            "diagnosticados": diagnosticados,
            "internacoes": internacoes,
            "escolaridade": escolaridade,
            "x": x,
            "y": round(y, 2)
        })

    return {
        "beta0": beta0,
        "beta1": beta1,
        "data": data
    }
