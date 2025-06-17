from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import random

#fast api simula a api rest que usaremos

app = FastAPI(
    title="API de Regressão Linear",
    description="Retorna os parâmetros da regressão linear e os dados simulados.",
    version="1.0.0"
)


# Liberar CORS para acesso externo (como o Streamlit)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite qualquer origem (ou restrinja se desejar)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Modelo de dado para documentação
class DataPoint(BaseModel):
    x: float
    y: float


# Endpoint principal
@app.get("/regressao")
def get_regressao():
    # Parâmetros da regressão simulados (reta y = beta1 * x + beta0 + ruído)
    beta0 = 1.5
    beta1 = 2.3

    # Gerar dados simulados
    data = []
    for i in range(50):
        x = round(random.uniform(0, 20), 2)
        noise = random.gauss(0, 2)  # ruído gaussiano
        y = beta1 * x + beta0 + noise
        data.append({"x": x, "y": round(y, 2)})

    # Resposta JSON
    return {
        "beta0": beta0,
        "beta1": beta1,
        "data": data
    }
