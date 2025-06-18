import streamlit as st
import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt

## uvicorn api_regressao:app --reload --host 0.0.0.0 --port 8000
## streamlit run dashboard/dashboard.py

# ========================
# CONFIGURAÇÃO DO DASHBOARD
# ========================
st.set_page_config(
    page_title="Dashboard Saúde Pública",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🏥 Dashboard Saúde Pública - Análises e Tendências")


# ========================
# CONFIGURAÇÃO DA API
# ========================
url_api = st.sidebar.text_input(
    "🔗 URL da API que retorna os dados processados",
    value="http://localhost:8000/dados"
)

refresh = st.sidebar.button("🔄 Atualizar Dados")


# ========================
# FUNÇÃO PARA CARREGAR DADOS
# ========================
@st.cache_data(ttl=120)
def carregar_dados(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data.get("data", []))
        beta0 = data.get("beta0")
        beta1 = data.get("beta1")

        return df, beta0, beta1
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame(), None, None


# ========================
# CARREGAR DADOS
# ========================
if url_api:
    if refresh:
        carregar_dados.clear()

    df, beta0, beta1 = carregar_dados(url_api)

    if df is not None and not df.empty:
        st.subheader("📄 Dados Recebidos")
        st.dataframe(df)

        # ========================
        # 1️⃣ Evolução da taxa de vacinados
        # 2️⃣ Evolução de diagnosticados
        # ========================
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("1️⃣ Evolução da Taxa de Vacinados")
            df_vacina = df.dropna(subset=["data", "vacinados", "populacao"])
            df_vacina["data"] = pd.to_datetime(df_vacina["data"])
            taxa = (
                df_vacina.groupby("data")
                .apply(lambda x: x["vacinados"].sum() / x["populacao"].sum())
                .reset_index(name="taxa_vacinados")
            )
            st.line_chart(taxa.set_index("data"))

        with col2:
            st.subheader("2️⃣ Evolução de Diagnosticados")
            df_diag = df.dropna(subset=["data", "diagnosticados"])
            df_diag["data"] = pd.to_datetime(df_diag["data"])
            diag = df_diag.groupby("data")["diagnosticados"].sum().reset_index()
            st.line_chart(diag.set_index("data"))

        # ========================
        # 3️⃣ Escolaridade vs vacinação
        # 4️⃣ Média de diagnosticados
        # ========================
        col3, col4 = st.columns(2)

        with col3:
            st.subheader("3️⃣ Escolaridade vs Vacinação")
            corr_vacina = df[["escolaridade", "vacinados"]].dropna().corr().iloc[0, 1]
            st.metric("Correlação", f"{corr_vacina:.2f}")

            fig, ax = plt.subplots()
            ax.scatter(df["escolaridade"], df["vacinados"], alpha=0.5)
            ax.set_xlabel("Escolaridade")
            ax.set_ylabel("Vacinados")
            ax.set_title("Escolaridade vs Vacinação")
            st.pyplot(fig)

        with col4:
            st.subheader("4️⃣ Média de Diagnosticados")
            media_diag = diag["diagnosticados"].mean()
            st.metric("Média diária", f"{media_diag:.2f}")

            fig, ax = plt.subplots()
            ax.bar(diag["data"], diag["diagnosticados"], width=1, alpha=0.7)
            ax.axhline(media_diag, color="red", linestyle="--", label="Média")
            ax.set_title("Diagnosticados por Dia")
            ax.set_xlabel("Data")
            ax.set_ylabel("Diagnosticados")
            ax.legend()
            st.pyplot(fig)

        # ========================
        # 5️⃣ Vacinação vs internações
        # 6️⃣ Estatísticas por região
        # ========================
        col5, col6 = st.columns(2)

        with col5:
            st.subheader("5️⃣ Estatísticas Gerais do Sistema Hospitalar")
            stats_hosp = df.agg({
                "diagnosticados": ["mean", "std"],
                "internacoes": ["mean", "std"],
                "vacinados": ["mean", "std"]
            })
            st.dataframe(stats_hosp)


            

        with col6:
            st.subheader("6️⃣ Estatísticas por Região")
            stats_regiao = (
                df.groupby("regiao")
                .agg({
                    "diagnosticados": ["mean", "std"],
                    "internacoes": ["mean", "std"],
                    "vacinados": ["mean", "std"]
                })
            )
            st.dataframe(stats_regiao)

        # ========================
        # 7️⃣ Regressão Linear
        # 8️⃣ Estatísticas gerais hospital
        # ========================
        col7, col8 = st.columns(2)

        with col7:
            st.subheader("7️⃣ Vacinação vs Internações")
            df_corr = df.dropna(subset=["vacinados", "internacoes"])
            corr = df_corr[["vacinados", "internacoes"]].corr().iloc[0, 1]
            st.metric("Correlação", f"{corr:.2f}")

            fig, ax = plt.subplots()
            ax.scatter(df_corr["vacinados"], df_corr["internacoes"], alpha=0.5, color="orange")
            ax.set_xlabel("Vacinados")
            ax.set_ylabel("Internações")
            ax.set_title("Vacinação vs Internações")
            st.pyplot(fig)
           
            

        with col8:
            st.subheader("8️⃣ Regressão Linear dos Dados")
            if beta0 is not None and beta1 is not None and "x" in df.columns and "y" in df.columns:
                st.markdown(f"**y = {beta1:.4f} * x + {beta0:.4f}**")

                fig, ax = plt.subplots()
                ax.scatter(df["x"], df["y"], color="#2F89A5", label="Dados")

                x_vals = np.linspace(df["x"].min(), df["x"].max(), 100)
                y_vals = beta1 * x_vals + beta0
                ax.plot(x_vals, y_vals, color="#2FA56A", label="Regressão Linear")

                ax.set_xlabel("X")
                ax.set_ylabel("Y")
                ax.set_title("Regressão Linear")
                ax.legend()

                st.pyplot(fig)

            

        # ========================
        # 9️⃣ Ranking de vacinação
        # 🔟 Tendência (média móvel)
        # ========================
        col9, col10 = st.columns(2)

        with col9:
            st.subheader("9️⃣ Ranking de Vacinação por Região")
            rank_vac = (
                df.groupby("regiao")["vacinados"]
                .sum()
                .sort_values(ascending=False)
                .reset_index()
            )
            st.bar_chart(rank_vac.set_index("regiao"))

        with col10:
            st.subheader("🔟 Tendência de Diagnosticados (Média Móvel)")
            diag["media_movel"] = diag["diagnosticados"].rolling(window=7).mean()

            fig, ax = plt.subplots()
            ax.plot(diag["data"], diag["diagnosticados"], label="Diagnosticados", alpha=0.5)
            ax.plot(diag["data"], diag["media_movel"], label="Média Móvel (7 dias)", color="red")

            ax.set_xlabel("Data")
            ax.set_ylabel("Diagnosticados")
            ax.set_title("Tendência de Diagnosticados")
            ax.legend()

            st.pyplot(fig)

    else:
        st.warning("⚠️ Não foi possível carregar dados. Verifique a URL da API.")
