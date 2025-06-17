#  Fluxo do Dashboard
# - Faz uma requisição GET a uma API REST que retorna um JSON com dados.

# - Plota o gráfico de dispersão e a linha da regressão.

# - Atualiza os dados em tempo real (opcional com botão ou auto-refresh).

import streamlit as st
import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt


# ========================
# CONFIGURAÇÃO DO DASHBOARD
# ========================
st.set_page_config(
    page_title="Dashboard Regressão Linear",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Código do dashboard
st.title("📈 Dashboard de Regressão Linear")


# ========================
# CONFIGURAR API
# ========================
url_api = st.sidebar.text_input(
    "🔗 URL da API que retorna os parâmetros e dados",
    value="http://localhost:8000/regressao"  # Exemplo, ajuste para sua API
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
        json_data = response.json()

        beta0 = json_data.get("beta0")
        beta1 = json_data.get("beta1")
        data = json_data.get("data", [])

        df = pd.DataFrame(data)

        return beta0, beta1, df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return None, None, pd.DataFrame()


# ========================
# CARREGAR DADOS
# ========================
if url_api:
    if refresh:
        carregar_dados.clear()  # Limpa cache manualmente

    beta0, beta1, df = carregar_dados(url_api)  # Sempre carrega os dados

    if df is not None and not df.empty and beta0 is not None and beta1 is not None:
        st.subheader("📄 Dados Recebidos")
        st.dataframe(df)

        st.subheader("📜 Equação da Regressão Linear")
        st.markdown(f"**y = {beta1:.4f} * x + {beta0:.4f}**")

        # ========================
        # PLOTAR GRÁFICO
        # ========================
        fig, ax = plt.subplots(figsize=(8, 5))

        # Cor de fundo da área fora dos eixos (borda do gráfico)
        fig.patch.set_facecolor("#5C5B5B")

        # Cor de fundo da área dos dados (dentro dos eixos)
        ax.set_facecolor("#5C5B5B")

        # Dados
        ax.scatter(df["x"], df["y"], color="#2F89A5", label="Dados")

        # Linha de regressão
        x_vals = np.linspace(df["x"].min(), df["x"].max(), 100)
        y_vals = beta1 * x_vals + beta0
        ax.plot(x_vals, y_vals, color="#2FA56A", label="Regressão Linear")

        # Estilo dos textos
        ax.set_xlabel("X", color="white")
        ax.set_ylabel("Y", color="white")
        ax.set_title("Regressão Linear", color="white")

        # Cor dos ticks dos eixos
        ax.tick_params(colors="white")

        # Cor da legenda
        legend = ax.legend()
        for text in legend.get_texts():
            text.set_color("white")

        # Mostra no Streamlit
        st.pyplot(fig)

    else:
        st.warning("⚠️ Não foi possível carregar dados ou parâmetros da regressão. Verifique a URL da API.")
