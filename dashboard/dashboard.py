import streamlit as st
import pandas as pd
import requests
import matplotlib.pyplot as plt

# ========================
# CONFIGURAÇÃO DO DASHBOARD
# ========================
st.set_page_config(
    page_title="📊 Dashboard de Diagnóstico",
    page_icon="🩺",
    layout="wide"
)

st.title("📊 Dashboard de Média de Diagnósticos")

# ========================
# CONFIGURAÇÃO DA API
# ========================
url_api = st.sidebar.text_input(
    "🔗 URL da API de métricas",
    value="http://api:8000/metricas"
)
refresh = st.sidebar.button("🔄 Atualizar Dados")

# ========================
# FUNÇÃO PARA CARREGAR DADOS
# ========================
@st.cache_data(ttl=60)
def carregar_dados(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()

# ========================
# CARREGAMENTO DOS DADOS
# ========================
if url_api:
    if refresh:
        carregar_dados.clear()

    df = carregar_dados(url_api)

    if df is not None and not df.empty:
        st.subheader("📄 Dados Recebidos da API")
        st.dataframe(df)

        # ========================
        # 📈 Gráfico de Linha
        # ========================
        st.subheader("📈 Evolução da Média de Diagnósticos")
        fig_line, ax_line = plt.subplots()
        ax_line.plot(df["quantidade"], df["media_diagnostico"], marker='o', color='blue')
        ax_line.set_xlabel("Quantidade de Registros")
        ax_line.set_ylabel("Média de Diagnóstico")
        ax_line.set_title("Média de Diagnóstico por Quantidade de Casos")
        st.pyplot(fig_line)

        # ========================
        # 📊 Gráfico de Barras
        # ========================
        st.subheader("📊 Comparativo - Média de Diagnóstico")
        fig_bar, ax_bar = plt.subplots()
        ax_bar.bar(df["quantidade"], df["media_diagnostico"], color='green')
        ax_bar.set_xlabel("Quantidade")
        ax_bar.set_ylabel("Média de Diagnóstico")
        ax_bar.set_title("Distribuição de Média de Diagnóstico")
        st.pyplot(fig_bar)

    else:
        st.warning("⚠️ Não foi possível carregar dados. Verifique a URL da API.")
