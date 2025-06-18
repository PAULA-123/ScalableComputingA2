from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List

app = FastAPI()

# Permitir acesso CORS para seu dashboard Streamlit (ajuste origem conforme necessário)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ou coloque o domínio do seu Streamlit
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modelo para os dados da métrica
class Resultado(BaseModel):
    quantidade: int
    media_diagnostico: float

# Armazena os resultados em memória (pode substituir por base real)
dados_metricas: List[Resultado] = []

@app.get("/metricas", response_model=List[Resultado])
async def get_metricas():
    return dados_metricas

@app.post("/metricas")
async def post_metricas(novos_resultados: List[Resultado]):
    global dados_metricas
    dados_metricas = novos_resultados
    return {"message": "Dados atualizados com sucesso"}
