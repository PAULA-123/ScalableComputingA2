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
    media_diagnostico: float
    data: Optional[str] = None
    total_vacinados: Optional[int] = None

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
##
class EvolucaoVacinacao(BaseModel):
    Data: str
    total_vacinados: int
    total_populacao: int
    taxa_vacinacao: float

class EvolucaoDiagnostico(BaseModel):
    Data: str
    total_diagnosticos: int


# ==========================
# DADOS EM MEMÓRIA
# ==========================
dados_metricas: List[Resultado] = []
dados_agrupamento: List[Agrupado] = []
dados_correlacao: List[Correlacao] = []
dados_desvios: List[Desvio] = []
dados_regressao: List[Regressao] = []
dados_media_movel: List[MediaMovel] = []
#
dados_evolucao_vacinacao: List[EvolucaoVacinacao] = []
dados_evolucao_diagnostico: List[EvolucaoDiagnostico] = []

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
#
@app.get("/evolucao-vacinacao", response_model=List[EvolucaoVacinacao])
def get_evolucao_vacinacao():
    return dados_evolucao_vacinacao

@app.post("/evolucao-vacinacao")
def post_evolucao_vacinacao(novos: List[EvolucaoVacinacao]):
    global dados_evolucao_vacinacao
    dados_evolucao_vacinacao = novos
    return {"message": "✅ Evolução da vacinação atualizada"}

@app.get("/evolucao-diagnostico", response_model=List[EvolucaoDiagnostico])
def get_evolucao_diagnostico():
    return dados_evolucao_diagnostico

@app.post("/evolucao-diagnostico")
def post_evolucao_diagnostico(novos: List[EvolucaoDiagnostico]):
    global dados_evolucao_diagnostico
    dados_evolucao_diagnostico = novos
    return {"message": "✅ Evolução de diagnosticados atualizada"}
