import os
import streamlit as st
import pandas as pd
import requests
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="📊 Dashboard de Métricas de Saúde",
    page_icon="🩺",
    layout="wide"
)

# ============================
# 🔄 AUTO REFRESH (a cada 30 segundos)
# ============================
st_autorefresh(interval=30 * 1000, key="auto-refresh")

st.title("📊 Dashboard de Métricas de Saúde Pública")

# ============================
# 🔗 CONFIGURAÇÃO DE ENDPOINTS
# ============================
base_api = os.getenv("API_URL", "http://api:8000")
base_api = st.sidebar.text_input("🌐 API Base", value=base_api)

endpoints = {
    "Métricas Gerais": "/metricas",
    "Agrupamento por Região (T4)": "/agrupamento",
    "Correlação Escolaridade/Vacinação (T6)": "/correlacao",
    "Desvios por Região (T7)": "/desvios",
    "Regressão Linear (T8)": "/regressao",
    "Média Móvel Diária (T9)": "/media-movel",
    "Evolução Vacinação (T10)": "/evolucao-vacinacao",
    "Evolução de Diagnosticados": "/evolucao-diagnostico",
    "Alerta de Óbitos (T5)": "/alerta-obitos",
}

refresh = st.sidebar.button("🔄 Atualizar Manualmente")

@st.cache_data(ttl=60)
def carregar_dados(endpoint):
    try:
        url = base_api + endpoint
        response = requests.get(url)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        st.error(f"❌ Erro ao carregar {endpoint}: {e}")
        return pd.DataFrame()

if refresh:
    carregar_dados.clear()

st.markdown("""
<style>
    .stDataFrame { border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.15); }
    .block-container { padding: 2rem 1rem; }
    .stButton>button { background-color: #4CAF50; color: white; border-radius: 8px; }
    .stSidebar { background-color: #f8f9fa; }
</style>
""", unsafe_allow_html=True)

# MÉTRICAS GERAIS
st.header("📦 Métricas Gerais")
df_metricas = carregar_dados(endpoints["Métricas Gerais"])
if not df_metricas.empty:
    st.dataframe(df_metricas, use_container_width=True)
    col1, col2 = st.columns(2)
    with col1:
        fig1, ax1 = plt.subplots()
        ax1.plot(df_metricas["quantidade"], df_metricas["taxa_vacinacao"], marker='o')
        ax1.set_title("Taxa de Vacinação vs. Quantidade")
        st.pyplot(fig1)
    with col2:
        fig2, ax2 = plt.subplots()
        ax2.plot(df_metricas["quantidade"], df_metricas["media_escolaridade"], marker='o', color='green')
        ax2.set_title("Escolaridade Média vs. Quantidade")
        st.pyplot(fig2)

# AGRUPAMENTO
st.header("🧩 Agrupamento por Região")
df_agrupado = carregar_dados(endpoints["Agrupamento por Região (T4)"])
if not df_agrupado.empty:
    st.dataframe(df_agrupado, use_container_width=True)
    fig, ax = plt.subplots()
    df_agrupado.groupby("CEP")["media_diagnostico"].mean().plot(kind="bar", ax=ax)
    ax.set_title("Média de Diagnóstico por Região")
    st.pyplot(fig)

# CORRELAÇÃO
st.header("🔗 Correlação Escolaridade/Vacinação")
df_corr = carregar_dados(endpoints["Correlação Escolaridade/Vacinação (T6)"])
if not df_corr.empty:
    st.dataframe(df_corr, use_container_width=True)
    fig, ax = plt.subplots()
    ax.scatter(df_corr["Escolaridade"], df_corr["Vacinado"], alpha=0.6)
    ax.set_title("Correlação: Escolaridade vs. Vacinado")
    st.pyplot(fig)

# DESVIO PADRÃO
st.header("📉 Desvio Padrão por Região")
df_desvios = carregar_dados(endpoints["Desvios por Região (T7)"])
if not df_desvios.empty:
    st.dataframe(df_desvios, use_container_width=True)
    st.bar_chart(df_desvios.set_index("variavel")["desvio"])

# REGRESSÃO LINEAR
st.header("📏 Regressão Linear")
df_regressao = carregar_dados(endpoints["Regressão Linear (T8)"])
if not df_regressao.empty:
    st.dataframe(df_regressao, use_container_width=True)
    st.write("Inclinação das variáveis (beta):")
    st.bar_chart(df_regressao.set_index("variavel")["beta"])

# MÉDIA MÓVEL
st.header("📊 Média Móvel de Infectados")
df_movel = carregar_dados(endpoints["Média Móvel Diária (T9)"])
if not df_movel.empty:
    st.dataframe(df_movel, use_container_width=True)
    fig, ax = plt.subplots()
    ax.plot(df_movel["Data"], df_movel["media_movel"], marker='o')
    ax.set_title("Média Móvel de Diagnósticos Diários")
    st.pyplot(fig)

# EVOLUÇÃO VACINAÇÃO
st.header("🧬 Evolução da Vacinação")
df_evolucao = carregar_dados(endpoints["Evolução Vacinação (T10)"])
if not df_evolucao.empty:
    st.dataframe(df_evolucao, use_container_width=True)
    fig, ax = plt.subplots()
    ax.plot(df_evolucao["Data"], df_evolucao["taxa_vacinacao"], marker='o', color='purple')
    ax.set_title("Taxa de Vacinação por Data")
    ax.set_ylabel("Taxa (%)")
    ax.set_xlabel("Data")
    ax.set_ylim(0, 1)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1))
    ax.grid(True)
    st.pyplot(fig)

# EVOLUÇÃO DIAGNÓSTICOS
st.header("📈 Evolução de Diagnosticados")
df_diag = carregar_dados(endpoints["Evolução de Diagnosticados"])
if not df_diag.empty:
    st.dataframe(df_diag, use_container_width=True)
    fig, ax = plt.subplots()
    ax.plot(df_diag["Data"], df_diag["total_diagnosticos"], marker='o')
    ax.set_title("Total de Diagnosticados por Dia")
    st.pyplot(fig)

# ALERTA DE ÓBITOS
st.header("🚨 Monitoramento de Óbitos")
df_alerta = carregar_dados(endpoints["Alerta de Óbitos (T5)"])
if not df_alerta.empty:
    st.dataframe(df_alerta, use_container_width=True)
    df_alerta['Data'] = pd.to_datetime(df_alerta['Data'])
    df_alerta = df_alerta.sort_values("Data")
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df_alerta["Data"], df_alerta["N_obitos"], label="Óbitos", marker='o')
    ax.plot(df_alerta["Data"], df_alerta["Media_Movel"], label="Média Móvel", linestyle='--')
    vermelhos = df_alerta[df_alerta["Alerta"] == "Vermelho"]
    ax.scatter(vermelhos["Data"], vermelhos["N_obitos"], color='red', label="Alerta Vermelho", zorder=5)
    ax.set_title("Óbitos Diários com Alertas")
    ax.set_xlabel("Data")
    ax.set_ylabel("Nº de Óbitos")
    ax.legend()
    st.pyplot(fig)
