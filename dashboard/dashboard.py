import streamlit as st
import pandas as pd
import requests
import matplotlib.pyplot as plt

plt.style.use('ggplot')

st.set_page_config(
    page_title="Dashboard Integrado de Saúde Pública",
    layout="wide"
)

st.title("Dashboard de Métricas de Saúde Pública")

base_api = st.sidebar.text_input("API Base", value="http://api:8000")

endpoints = {
    "/merge-cep": "/merge-cep",
    "/alerta-obitos": "/alerta-obitos",
    "/correlacao": "/correlacao",
    "/desvios": "/desvios",
    "/regressao": "/regressao",
    "/media-movel": "/media-movel"
}

refresh = st.sidebar.button("Atualizar Tudo")

@st.cache_data(ttl=10)
def carregar_dados(endpoint_key):
    try:
        if endpoint_key not in endpoints:
            st.warning(f"Endpoint key '{endpoint_key}' não encontrado.")
            return pd.DataFrame()

        endpoint = endpoints[endpoint_key]
        url = base_api + endpoint
        response = requests.get(url, timeout=5)

        if response.status_code == 404:
            st.warning(f"Endpoint {endpoint} não encontrado. Verifique a API.")
            return pd.DataFrame()

        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
        return df

    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame()

if refresh:
    carregar_dados.clear()

# ============================
# MERGE POR CEP
# ============================
df_merge = carregar_dados("/merge-cep")
if not df_merge.empty:
    st.header("Merge por CEP - Dados Integrados")

    df_merge['CEP'] = df_merge['CEP'].astype(str)
    df_merge['Taxa_Internados'] = df_merge['Total_Internados'] / df_merge['Populacao'] * 100
    df_merge['Taxa_Vacinados'] = df_merge['Total_Vacinados'] / df_merge['Populacao'] * 100

    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader("Tabela Consolidada por CEP")
        st.dataframe(df_merge.style.format({
            'Taxa_Internados': '{:.2f}%',
            'Taxa_Vacinados': '{:.2f}%',
            'Media_Idade': '{:.1f}',
            'Media_Escolaridade': '{:.1f}'
        }).background_gradient(cmap='Blues', subset=['Taxa_Internados', 'Taxa_Vacinados']))

    with col2:
        st.subheader("Filtros")
        cep_selecionado = st.selectbox("Selecione um CEP:", df_merge['CEP'].unique())
        df_cep = df_merge[df_merge['CEP'] == cep_selecionado].iloc[0]

        st.metric("População", f"{df_cep['Populacao']:,}")
        st.metric("Internados", f"{df_cep['Total_Internados']:,}", delta=f"{df_cep['Taxa_Internados']:.2f}%")
        st.metric("Vacinados", f"{df_cep['Total_Vacinados']:,}", delta=f"{df_cep['Taxa_Vacinados']:.2f}%")
        st.metric("Média Escolaridade", f"{df_cep['Media_Escolaridade']:.1f} anos")

    st.subheader("Visualizações")
    tabs = st.tabs(["Vacinação vs Internações", "Sintomas", "Demografia"])

    with tabs[0]:
        fig, ax = plt.subplots(figsize=(10, 6))
        scatter = ax.scatter(df_merge['Taxa_Vacinados'], df_merge['Taxa_Internados'],
                             c=df_merge['Media_Idade'], s=df_merge['Populacao']/50,
                             cmap='viridis', alpha=0.7)
        ax.set_xlabel('Taxa de Vacinação (%)')
        ax.set_ylabel('Taxa de Internações (%)')
        ax.set_title('Relação Vacinação x Internações')
        plt.colorbar(scatter, label='Média de Idade')
        st.pyplot(fig)

    with tabs[1]:
        sintomas = ['Total_Sintoma1', 'Total_Sintoma2', 'Total_Sintoma3', 'Total_Sintoma4']
        df_sintomas = df_merge[['CEP'] + sintomas].set_index('CEP')
        df_sintomas.columns = ['Febre', 'Tosse', 'Respiração', 'Cefaleia']
        st.bar_chart(df_sintomas)

    with tabs[2]:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.scatter(df_merge['Media_Escolaridade'], df_merge['Media_Idade'],
                   c=df_merge['Taxa_Vacinados'], cmap='coolwarm', s=100, alpha=0.7)
        ax.set_xlabel('Média Escolaridade')
        ax.set_ylabel('Média Idade')
        ax.set_title('Idade vs Escolaridade')
        plt.colorbar(ax.collections[0], label='Taxa Vacinação')
        st.pyplot(fig)
        
# ============================
# ALERTA MÉDIA DE ÓBITOS (T5)
# ============================
st.header("Monitoramento de Óbitos - Tratador 5")
df_obitos = carregar_dados("/alerta-obitos")
if not df_obitos.empty:
    df_obitos['Data'] = pd.to_datetime(df_obitos['Data'])
    df_obitos = df_obitos.sort_values('Data')
    media_obitos = df_obitos['N_obitos'].mean()
    df_obitos['Media_Movel'] = df_obitos['N_obitos'].rolling(window=7).mean()
    df_obitos['Alerta'] = df_obitos['N_obitos'] > media_obitos

    col1, col2 = st.columns([1, 2])
    with col1:
        st.metric("Média Geral", f"{media_obitos:.1f}")
        st.metric("Total de Óbitos", f"{df_obitos['N_obitos'].sum():,}")
        ult = df_obitos.iloc[-1]
        delta = ult['N_obitos'] - media_obitos
        st.metric("Último Registro", f"{ult['N_obitos']}", delta=f"{delta:.1f}",
                  delta_color="inverse" if delta > 0 else "normal")

        if ult['Alerta']:
            st.error("⚠️ Alerta: Óbitos acima da média histórica")
        else:
            st.success("✅ Situação Normal")

    with col2:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df_obitos['Data'], df_obitos['N_obitos'], label="Óbitos Diários", marker='o')
        ax.plot(df_obitos['Data'], df_obitos['Media_Movel'], label="Média Móvel (7d)", linestyle="--")
        ax.axhline(media_obitos, color='r', linestyle=':', label="Média Geral")
        ax.legend()
        ax.set_title("Evolução de Óbitos")
        ax.set_xlabel("Data")
        ax.set_ylabel("Óbitos")
        st.pyplot(fig)

# ============================
# CORRELAÇÃO
# ============================
df_corr = carregar_dados("/correlacao")
if not df_corr.empty:
    st.header("Correlação Escolaridade x Vacinação - Tratador 6")
    valor = df_corr.iloc[-1]['Escolaridade']
    st.metric("Correlação de Pearson", f"{valor:.4f}",
              help="Correlação entre média de escolaridade e total de vacinados por CEP")
    if abs(valor) > 0.7:
        st.success("Correlação forte detectada")
    elif abs(valor) > 0.4:
        st.info("Correlação moderada")
    else:
        st.warning("Correlação fraca")

# ============================
# DESVIOS
# ============================
# df_desvio = carregar_dados("/desvios")
# if not df_desvio.empty:
#     st.header("Desvios Identificados")
#     st.dataframe(df_desvio.style.background_gradient(cmap='Purples', subset=['desvio']))

# ============================
# REGRESSÃO
# ============================
# df_reg = carregar_dados("/regressao")
# if not df_reg.empty:
#     st.header("Modelos de Regressão")
#     st.dataframe(df_reg)

# ============================
# MÉDIA MÓVEL DE ÓBITOS
# ============================
# df_media = carregar_dados("/media-movel")
# if not df_media.empty:
#     st.header("Média Móvel de Óbitos")
#     df_media['Data'] = pd.to_datetime(df_media['Data'])
#     df_media = df_media.sort_values('Data')
#     fig, ax = plt.subplots(figsize=(10, 4))
#     ax.plot(df_media['Data'], df_media['media_movel'], label='Média Móvel 7 dias')
#     ax.set_xlabel('Data')
#     ax.set_ylabel('Óbitos')
#     ax.set_title('Tendência de Óbitos')
#     ax.legend()
#     st.pyplot(fig)