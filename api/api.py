from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "API is working"}

# Permitir acesso CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modelo atualizado para os dados da métrica
class Resultado(BaseModel):
    quantidade: int
    taxa_vacinacao: float
    media_escolaridade: float
    taxa_diagnostico: float  # mantendo compatibilidade com nome antigo
    media_populacao: float

# Armazena os resultados em memória
dados_metricas: List[Resultado] = []

@app.get("/metricas", response_model=List[Resultado])
async def get_metricas():
    return dados_metricas

@app.post("/metricas")
async def post_metricas(novos_resultados: List[Resultado]):
    global dados_metricas
    dados_metricas = novos_resultados
    return {"message": "Dados atualizados com sucesso"}