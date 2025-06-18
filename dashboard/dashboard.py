import streamlit as st
import pandas as pd
import requests
import matplotlib.pyplot as plt

# ========================
# CONFIGURAÇÃO DO DASHBOARD
# ========================
st.set_page_config(
    page_title="📊 Dashboard de Métricas de Saúde",
    page_icon="🩺",
    layout="wide"
)

st.title("📊 Dashboard de Métricas de Saúde")

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
        # 📈 Gráficos de Linha
        # ========================
        st.subheader("📈 Evolução das Métricas")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_line1, ax_line1 = plt.subplots()
            ax_line1.plot(df["quantidade"], df["taxa_vacinacao"], marker='o', color='blue')
            ax_line1.set_xlabel("Quantidade de Registros")
            ax_line1.set_ylabel("Taxa de Vacinação")
            ax_line1.set_title("Taxa de Vacinação por Quantidade de Casos")
            st.pyplot(fig_line1)

        with col2:
            fig_line2, ax_line2 = plt.subplots()
            ax_line2.plot(df["quantidade"], df["media_escolaridade"], marker='o', color='green')
            ax_line2.set_xlabel("Quantidade de Registros")
            ax_line2.set_ylabel("Escolaridade Média")
            ax_line2.set_title("Escolaridade Média por Quantidade de Casos")
            st.pyplot(fig_line2)

        # ========================
        # 📊 Gráficos de Barras
        # ========================
        st.subheader("📊 Comparativo de Métricas")
        
        col3, col4 = st.columns(2)
        
        with col3:
            fig_bar1, ax_bar1 = plt.subplots()
            ax_bar1.bar(df["quantidade"], df["taxa_diagnostico"], color='red')
            ax_bar1.set_xlabel("Quantidade")
            ax_bar1.set_ylabel("Taxa de Diagnóstico")
            ax_bar1.set_title("Distribuição de Taxa de Diagnóstico")
            st.pyplot(fig_bar1)

        with col4:
            fig_bar2, ax_bar2 = plt.subplots()
            ax_bar2.bar(df["quantidade"], df["media_populacao"], color='purple')
            ax_bar2.set_xlabel("Quantidade")
            ax_bar2.set_ylabel("População Média")
            ax_bar2.set_title("Distribuição de População Média")
            st.pyplot(fig_bar2)

    else:
        st.warning("⚠️ Não foi possível carregar dados. Verifique a URL da API.")