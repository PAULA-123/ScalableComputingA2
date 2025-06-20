from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI()

# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "🚀 API está rodando com sucesso"}

# ==========================
# MODELOS DE DADOS
# ==========================
class Resultado(BaseModel):
    quantidade: int
    taxa_vacinacao: float
    media_escolaridade: float
    taxa_diagnostico: float
    media_populacao: float

class Agrupado(BaseModel):
    CEP: int
    data: Optional[str]
    media_diagnostico: float
    total_vacinados: int

class Correlacao(BaseModel):
    Escolaridade: float
    Vacinado: float

class Desvio(BaseModel):
    variavel: str
    desvio: float

class Regressao(BaseModel):
    variavel: str
    alfa: float
    beta: float

class MediaMovel(BaseModel):
    Data: str
    media_movel: float

# ==========================
# DADOS EM MEMÓRIA
# ==========================
dados_metricas: List[Resultado] = []
dados_agrupamento: List[Agrupado] = []
dados_correlacao: List[Correlacao] = []
dados_desvios: List[Desvio] = []
dados_regressao: List[Regressao] = []
dados_media_movel: List[MediaMovel] = []

# ==========================
# ENDPOINTS PARA DASHBOARD
# ==========================

@app.get("/metricas", response_model=List[Resultado])
def get_metricas():
    return dados_metricas

@app.post("/metricas")
def post_metricas(novos: List[Resultado]):
    global dados_metricas
    dados_metricas = novos
    return {"message": "✅ Métricas atualizadas"}

@app.get("/agrupamento", response_model=List[Agrupado])
def get_agrupamento():
    return dados_agrupamento

@app.post("/agrupamento")
def post_agrupamento(novos: List[Agrupado]):
    global dados_agrupamento
    dados_agrupamento = novos
    return {"message": "✅ Dados de agrupamento atualizados"}

@app.get("/correlacao", response_model=List[Correlacao])
def get_correlacao():
    return dados_correlacao

@app.post("/correlacao")
def post_correlacao(novos: List[Correlacao]):
    global dados_correlacao
    dados_correlacao = novos
    return {"message": "✅ Correlações atualizadas"}

@app.get("/desvios", response_model=List[Desvio])
def get_desvios():
    return dados_desvios

@app.post("/desvios")
def post_desvios(novos: List[Desvio]):
    global dados_desvios
    dados_desvios = novos
    return {"message": "✅ Desvios atualizados"}

@app.get("/regressao", response_model=List[Regressao])
def get_regressao():
    return dados_regressao

@app.post("/regressao")
def post_regressao(novos: List[Regressao]):
    global dados_regressao
    dados_regressao = novos
    return {"message": "✅ Regressões atualizadas"}

@app.get("/media-movel", response_model=List[MediaMovel])
def get_media_movel():
    return dados_media_movel

@app.post("/media-movel")
def post_media_movel(novos: List[MediaMovel]):
    global dados_media_movel
    dados_media_movel = novos
    return {"message": "✅ Média móvel atualizada"}
