import streamlit as st
import pandas as pd
import requests
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="📊 Dashboard de Métricas de Saúde",
    page_icon="🩺",
    layout="wide"
)

st.title("📊 Dashboard de Métricas de Saúde Pública")

# ============================
# 🔗 CONFIGURAÇÃO DE ENDPOINTS
# ============================
base_api = st.sidebar.text_input("🌐 API Base", value="http://api:8000")

endpoints = {
    "Métricas Gerais": "/metricas",
    "Agrupamento por Região (T4)": "/agrupamento",
    "Correlação Escolaridade/Vacinação (T6)": "/correlacao",
    "Desvios por Região (T7)": "/desvios",
    "Regressão Linear (T8)": "/regressao",
    "Média Móvel Diária (T9)": "/media-movel"
}

refresh = st.sidebar.button("🔄 Atualizar Tudo")

# ============================
# 🔄 CARREGAMENTO DE DADOS
# ============================
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

# ============================
# 🔢 MÉTRICAS GERAIS (API /metricas)
# ============================
df_metricas = carregar_dados(endpoints["Métricas Gerais"])
if not df_metricas.empty:
    st.subheader("📦 Dados de Métricas Gerais")
    st.dataframe(df_metricas)

    st.subheader("📈 Evolução de Vacinação e Escolaridade")
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

# ============================
# 📊 AGRUPAMENTO (T4)
# ============================
df_agrupado = carregar_dados(endpoints["Agrupamento por Região (T4)"])
if not df_agrupado.empty:
    st.subheader("🧩 Agrupamento por Região")
    st.dataframe(df_agrupado)

    fig, ax = plt.subplots()
    df_agrupado.groupby("CEP")["media_diagnostico"].mean().plot(kind="bar", ax=ax)
    ax.set_title("Média de Diagnóstico por Região")
    st.pyplot(fig)

# ============================
# 📈 CORRELAÇÃO (T6)
# ============================
df_corr = carregar_dados(endpoints["Correlação Escolaridade/Vacinação (T6)"])
if not df_corr.empty:
    st.subheader("🔗 Correlação entre Variáveis")
    st.dataframe(df_corr)

    fig, ax = plt.subplots()
    ax.scatter(df_corr["Escolaridade"], df_corr["Vacinado"], alpha=0.6)
    ax.set_title("Correlação: Escolaridade vs. Vacinado")
    st.pyplot(fig)

# ============================
# 📐 DESVIO PADRÃO (T7)
# ============================
df_desvios = carregar_dados(endpoints["Desvios por Região (T7)"])
if not df_desvios.empty:
    st.subheader("📉 Desvios Padrão por Região")
    st.dataframe(df_desvios)

    st.bar_chart(df_desvios.set_index("variavel")["desvio"])

# ============================
# 📊 REGRESSÃO LINEAR (T8)
# ============================
df_regressao = carregar_dados(endpoints["Regressão Linear (T8)"])
if not df_regressao.empty:
    st.subheader("📏 Resultados da Regressão Linear")
    st.dataframe(df_regressao)

    st.write("Visualização de inclinação (beta):")
    st.bar_chart(df_regressao.set_index("variavel")["beta"])

# ============================
# 🔮 MÉDIA MÓVEL (T9)
# ============================
df_movel = carregar_dados(endpoints["Média Móvel Diária (T9)"])
if not df_movel.empty:
    st.subheader("📊 Média Móvel de Infectados (Últimos Dias)")
    st.dataframe(df_movel)

    fig, ax = plt.subplots()
    ax.plot(df_movel["Data"], df_movel["media_movel"], marker='o')
    ax.set_title("Média Móvel de Diagnósticos Diários")
    st.pyplot(fig)
