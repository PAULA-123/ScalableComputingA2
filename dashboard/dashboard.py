import streamlit as st
import pandas as pd
import requests
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="Dashboard Integrado de Saúde Pública",
    layout="wide"
)

st.title("Dashboard de Métricas de Saúde Pública")

# ============================
# CONFIGURAÇÃO DE ENDPOINTS
# ============================
base_api = st.sidebar.text_input("API Base", value="http://api:8000")

endpoints = {
    "/merge-cep": "/merge-cep",
    # "Agrupamento por Região (T4)": "/agrupamento",
    "/alerta-obitos": "/alerta-obitos"
    # "Correlação Escolaridade/Vacinação (T6)": "/correlacao",
    # "Desvios por Região (T7)": "/desvios",
    # "Regressão Linear (T8)": "/regressao",
    # "Média Móvel Diária (T9)": "/media-movel"
}

refresh = st.sidebar.button("Atualizar Tudo")

# ============================
# CARREGAMENTO DE DADOS
# ============================
@st.cache_data(ttl=10)  # Atualiza a cada 10 segundos
def carregar_dados(endpoint_key):
    try:
        # Primeiro verifica se a endpoint_key existe
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
        
        #Tratamento para diferentes formatos de resposta
        if isinstance(data, dict) and 'data' in data:
            df = pd.DataFrame(data['data'])
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame([data])
        return df
    
    except requests.exceptions.RequestException as e:
        st.error(f"Erro na requisição para {endpoint_key}: {str(e)}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame()

if refresh:
    carregar_dados.clear()
        
# ============================
# MERGE POR CEP (T3)
# ============================
df_merge = carregar_dados(endpoints["/merge-cep"])
if not df_merge.empty:
    st.header("Merge por CEP - Tratador 3")
    
    # Convertendo CEP para string para melhor visualização
    df_merge['CEP'] = df_merge['CEP'].astype(str)
    
    # Calculando métricas adicionais
    df_merge['Taxa_Internados'] = df_merge['Total_Internados'] / df_merge['Soma_Populacao'] * 100
    df_merge['Taxa_Vacinados'] = df_merge['Total_Vacinados'] / df_merge['Soma_Populacao'] * 100
    df_merge['Media_Idade'] = df_merge['Soma_Idade'] / df_merge['Soma_Populacao']
    df_merge['Media_Escolaridade'] = df_merge['Soma_Escolaridade'] / df_merge['Soma_Populacao']
    
    # Exibindo os dados
    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader("Dados Consolidados por CEP")
        st.dataframe(df_merge.style.format({
            'Taxa_Internados': '{:.2f}%',
            'Taxa_Vacinados': '{:.2f}%',
            'Media_Idade': '{:.1f}',
            'Media_Escolaridade': '{:.1f}'
        }))
    
    with col2:
        st.subheader("Filtros")
        cep_selecionado = st.selectbox(
            "Selecione um CEP para detalhar:",
            df_merge['CEP'].unique()
        )
        
        df_filtrado = df_merge[df_merge['CEP'] == cep_selecionado].iloc[0]
        
        st.metric("População Total", f"{int(df_filtrado['Soma_Populacao']):,}")
        st.metric("Total Internados", f"{int(df_filtrado['Total_Internados']):,}")
        st.metric("Taxa de Vacinação", f"{df_filtrado['Taxa_Vacinados']:.2f}%")
        st.metric("Média Escolaridade", f"{df_filtrado['Media_Escolaridade']:.1f} anos")

    # Visualizações
    st.subheader("Visualizações por Região (CEP)")
    
    tabs = st.tabs(["Internações vs Vacinação", "Sintomas por Região", "Demografia"])
    
    with tabs[0]:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(df_merge['Taxa_Vacinados'], df_merge['Taxa_Internados'], 
                  s=df_merge['Soma_Populacao']/100, alpha=0.6)
        
        for i, row in df_merge.iterrows():
            ax.text(row['Taxa_Vacinados'], row['Taxa_Internados'], row['CEP'], 
                   fontsize=8, ha='center', va='center')
        
        ax.set_xlabel('Taxa de Vacinação (%)')
        ax.set_ylabel('Taxa de Internados (%)')
        ax.set_title('Relação entre Vacinação e Internações por Região')
        st.pyplot(fig)
    
    with tabs[1]:
        sintomas = ['Total_Sintoma1', 'Total_Sintoma2', 'Total_Sintoma3', 'Total_Sintoma4']
        df_sintomas = df_merge[['CEP'] + sintomas].set_index('CEP')
        
        fig, ax = plt.subplots(figsize=(12, 6))
        df_sintomas.plot(kind='bar', stacked=True, ax=ax)
        ax.set_title('Distribuição de Sintomas por Região')
        ax.set_ylabel('Total de Casos')
        ax.legend(['Sintoma 1', 'Sintoma 2', 'Sintoma 3', 'Sintoma 4'])
        st.pyplot(fig)
    
    with tabs[2]:
        col1, col2 = st.columns(2)
        with col1:
            fig, ax = plt.subplots()
            df_merge.plot.scatter(x='Media_Idade', y='Media_Escolaridade', 
                                s=df_merge['Soma_Populacao']/1000, ax=ax)
            ax.set_title('Idade Média vs Escolaridade Média')
            st.pyplot(fig)
        
        with col2:
            fig, ax = plt.subplots()
            df_merge['Soma_Populacao'].plot.pie(ax=ax, labels=df_merge['CEP'], 
                                              autopct='%1.1f%%')
            ax.set_title('Distribuição Populacional por CEP')
            st.pyplot(fig)

# ============================
# AGRUPAMENTO (T4)
# ============================
# df_agrupado = carregar_dados(endpoints["Agrupamento por Região (T4)"])
# if not df_agrupado.empty:
#     st.subheader("Agrupamento por Região")
#     st.dataframe(df_agrupado)

#     fig, ax = plt.subplots()
#     df_agrupado.groupby("CEP")["media_diagnostico"].mean().plot(kind="bar", ax=ax)
#     ax.set_title("Média de Diagnóstico por Região")
#     st.pyplot(fig)

# ============================
# ALERTA MÉDIA DE ÓBITOS (T5)
# ============================
st.header("Monitoramento de Óbitos - Tratador 5")
df_obitos = carregar_dados(endpoints["/alerta-obitos"])

if not df_obitos.empty:
    # Processamento dos dados
    df_obitos['Data'] = pd.to_datetime(df_obitos['Data'])
    df_obitos = df_obitos.sort_values('Data')
    
    # Cálculos similares ao tratador
    media_obitos = df_obitos['N_obitos'].mean()
    df_obitos['Media_Movel'] = df_obitos['N_obitos'].rolling(window=7, min_periods=1).mean()
    df_obitos['Alerta'] = df_obitos['N_obitos'] > media_obitos
    
    # Layout principal
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Indicadores Chave")
        st.metric("Média Global", f"{media_obitos:.1f} óbitos/dia")
        
        ultimo_registro = df_obitos.iloc[-1]
        st.metric("Último Registro", 
                 f"{ultimo_registro['N_obitos']} óbitos",
                 delta=f"{ultimo_registro['N_obitos'] - media_obitos:.1f} vs média")
        
        # Status do alerta
        if ultimo_registro['Alerta']:
            st.error("ALERTA: Óbitos acima da média")
        else:
            st.success("NORMAL: Óbitos dentro da média")
    
    with col2:
        st.subheader("Tendência Temporal")
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df_obitos['Data'], df_obitos['N_obitos'], 
               label='Óbitos Diários', marker='o', alpha=0.7)
        ax.plot(df_obitos['Data'], df_obitos['Media_Movel'], 
               label='Média Móvel (7 dias)', color='orange', linestyle='--')
        ax.axhline(y=media_obitos, color='red', 
                  linestyle=':', label='Média Global')
        
        # Destacar alertas
        alertas = df_obitos[df_obitos['Alerta']]
        if not alertas.empty:
            ax.scatter(alertas['Data'], alertas['N_obitos'], 
                      color='red', s=100, label='Alertas', zorder=5)
        
        ax.set_xlabel('Data')
        ax.set_ylabel('Número de Óbitos')
        ax.legend()
        ax.grid(True)
        st.pyplot(fig)
    
    # Análise por CEP (se disponível)
    if 'CEP' in df_obitos.columns:
        st.subheader("Distribuição por Região")
        
        cep_stats = df_obitos.groupby('CEP').agg({
            'N_obitos': ['sum', 'mean', 'count'],
            'Alerta': 'sum'
        }).sort_values(('N_obitos', 'sum'), ascending=False)
        
        st.dataframe(cep_stats.style.background_gradient(cmap='Reds'))

# ============================
# CORRELAÇÃO (T6)
# ============================
# df_corr = carregar_dados(endpoints["Correlação Escolaridade/Vacinação (T6)"])
# if not df_corr.empty:
#     st.subheader("Correlação entre Variáveis")
#     st.dataframe(df_corr)

#     fig, ax = plt.subplots()
#     ax.scatter(df_corr["Escolaridade"], df_corr["Vacinado"], alpha=0.6)
#     ax.set_title("Correlação: Escolaridade vs. Vacinado")
#     st.pyplot(fig)

# ============================
# DESVIO PADRÃO (T7)
# ============================
# df_desvios = carregar_dados(endpoints["Desvios por Região (T7)"])
# if not df_desvios.empty:
#     st.subheader("Desvios Padrão por Região")
#     st.dataframe(df_desvios)

#     st.bar_chart(df_desvios.set_index("variavel")["desvio"])

# ============================
# REGRESSÃO LINEAR (T8)
# ============================
# df_regressao = carregar_dados(endpoints["Regressão Linear (T8)"])
# if not df_regressao.empty:
#     st.subheader("Resultados da Regressão Linear")
#     st.dataframe(df_regressao)

#     st.write("Visualização de inclinação (beta):")
#     st.bar_chart(df_regressao.set_index("variavel")["beta"])

# ============================
# MÉDIA MÓVEL (T9)
# ============================
# df_movel = carregar_dados(endpoints["Média Móvel Diária (T9)"])
# if not df_movel.empty:
#     st.subheader("Média Móvel de Infectados (Últimos Dias)")
#     st.dataframe(df_movel)

#     fig, ax = plt.subplots()
#     ax.plot(df_movel["Data"], df_movel["media_movel"], marker='o')
#     ax.set_title("Média Móvel de Diagnósticos Diários")
#     st.pyplot(fig)