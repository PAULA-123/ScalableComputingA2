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
    return {"message": "API está rodando com sucesso"}

# ==========================
# MODELOS DE DADOS
# ==========================
    
class MergeCEP(BaseModel):
    CEP: int
    Total_Internados: int
    Media_Idade: float
    Total_Sintoma1: int
    Total_Sintoma2: int
    Total_Sintoma3: int
    Total_Sintoma4: int
    Total_Diagnosticos: int
    Total_Vacinados: int
    Media_Escolaridade: float
    Populacao: int
    
class AlertaObitos(BaseModel):
    N_obitos: int
    Populacao: int
    CEP: int
    N_recuperados: int
    N_vacinados: int
    Data: str
    Alerta: str

class Correlacao(BaseModel):
    Escolaridade: float
    Vacinado: float

class Desvio(BaseModel):
    variavel: str
    desvio: float

class Regressao(BaseModel):
    quantidade: int
    beta0: float
    beta1: float

class MediaMovel(BaseModel):
    Data: str
    media_movel: float

# ==========================
# DADOS EM MEMÓRIA
# ==========================

dados_merge_cep: List[MergeCEP] = []
dados_alerta_obitos: List[AlertaObitos] = []
dados_correlacao: List[Correlacao] = []
dados_desvios: List[Desvio] = []
dados_regressao: List[Regressao] = []
dados_media_movel: List[MediaMovel] = []

# ==========================
# ENDPOINTS PARA DASHBOARD
# ==========================

# Merge CEP
@app.get("/merge-cep", response_model=List[MergeCEP])
def get_merge_cep():
    return dados_merge_cep

@app.post("/merge-cep")
def post_merge_cep(novos: List[MergeCEP]):
    global dados_merge_cep
    dados_merge_cep = novos
    return {"message": "Dados de merge por CEP atualizados"}

# Alerta óbitos
@app.get("/alerta-obitos", response_model=List[AlertaObitos])
def get_alerta_obitos():
    return dados_alerta_obitos

@app.post("/alerta-obitos")
def post_alerta_obitos(novos: List[AlertaObitos]):
    global dados_alerta_obitos
    dados_alerta_obitos = novos
    return {"message": "Alertas de óbitos atualizados"}

# Correlação
@app.get("/correlacao", response_model=List[Correlacao])
def get_correlacao():
    return dados_correlacao

@app.post("/correlacao")
def post_correlacao(novos: List[Correlacao]):
    global dados_correlacao
    dados_correlacao = novos
    return {"message": "Correlações atualizadas"}

# Desvios
@app.get("/desvios", response_model=List[Desvio])
def get_desvios():
    return dados_desvios

@app.post("/desvios")
def post_desvios(novos: List[Desvio]):
    global dados_desvios
    dados_desvios = novos
    return {"message": "Desvios atualizados"}

# Regressões
@app.get("/regressao", response_model=List[Regressao])
def get_regressao():
    return dados_regressao

@app.post("/regressao")
def post_regressao(novos: List[Regressao]):
    global dados_regressao
    dados_regressao = novos
    return {"message": "Regressões atualizadas"}

# Média móvel
@app.get("/media-movel", response_model=List[MediaMovel])
def get_media_movel():
    return dados_media_movel

@app.post("/media-movel")
def post_media_movel(novos: List[MediaMovel]):
    global dados_media_movel
    dados_media_movel = novos
    return {"message": "Média móvel atualizada"}