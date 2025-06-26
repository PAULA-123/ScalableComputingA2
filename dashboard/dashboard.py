import streamlit as st
import pandas as pd
import requests
import matplotlib.pyplot as plt

# Configuração de estilo
plt.style.use('ggplot')  # Usando um estilo built-in do matplotlib

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
    "/alerta-obitos": "/alerta-obitos"
}

refresh = st.sidebar.button("Atualizar Tudo")

# ============================
# CARREGAMENTO DE DADOS
# ============================
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
    
    df_merge['CEP'] = df_merge['CEP'].astype(str)
    df_merge['Taxa_Internados'] = df_merge['Total_Internados'] / df_merge['Soma_Populacao'] * 100
    df_merge['Taxa_Vacinados'] = df_merge['Total_Vacinados'] / df_merge['Soma_Populacao'] * 100
    df_merge['Media_Idade'] = df_merge['Soma_Idade'] / df_merge['Soma_Populacao']
    df_merge['Media_Escolaridade'] = df_merge['Soma_Escolaridade'] / df_merge['Soma_Populacao']
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader("Dados Consolidados por CEP")
        st.dataframe(df_merge.style.format({
            'Taxa_Internados': '{:.2f}%',
            'Taxa_Vacinados': '{:.2f}%',
            'Media_Idade': '{:.1f}',
            'Media_Escolaridade': '{:.1f}'
        }).background_gradient(cmap='Blues', subset=['Taxa_Internados', 'Taxa_Vacinados']))
    
    with col2:
        st.subheader("Filtros")
        cep_selecionado = st.selectbox(
            "Selecione um CEP para detalhar:",
            df_merge['CEP'].unique()
        )
        
        df_filtrado = df_merge[df_merge['CEP'] == cep_selecionado].iloc[0]
        
        st.metric("População Total", f"{int(df_filtrado['Soma_Populacao']):,}",
                 help="Total de habitantes na região")
        st.metric("Total Internados", f"{int(df_filtrado['Total_Internados']):,}",
                 delta=f"{df_filtrado['Taxa_Internados']:.2f}% da população",
                 delta_color="inverse")
        st.metric("Taxa de Vacinação", f"{df_filtrado['Taxa_Vacinados']:.2f}%",
                 help="Percentual da população vacinada")
        st.metric("Média Escolaridade", f"{df_filtrado['Media_Escolaridade']:.1f} anos",
                 help="Média de anos de estudo")

    st.subheader("Visualizações por Região (CEP)")
    
    tabs = st.tabs(["Internações vs Vacinação", "Sintomas por Região", "Demografia"])
    
    with tabs[0]:
        fig, ax = plt.subplots(figsize=(10, 6))
        scatter = ax.scatter(df_merge['Taxa_Vacinados'], df_merge['Taxa_Internados'], 
                           s=df_merge['Soma_Populacao']/100, alpha=0.7,
                           c=df_merge['Media_Idade'], cmap='viridis')
        
        for i, row in df_merge.iterrows():
            ax.text(row['Taxa_Vacinados'], row['Taxa_Internados'], row['CEP'], 
                   fontsize=8, ha='center', va='center', bbox=dict(facecolor='white', alpha=0.7))
        
        ax.set_xlabel('Taxa de Vacinação (%)', fontsize=10)
        ax.set_ylabel('Taxa de Internados (%)', fontsize=10)
        ax.set_title('Relação entre Vacinação e Internações por Região', fontsize=12, pad=20)
        
        cbar = plt.colorbar(scatter)
        cbar.set_label('Idade Média', rotation=270, labelpad=15)
        
        ax.grid(True, linestyle='--', alpha=0.7)
        st.pyplot(fig)
    
    with tabs[1]:
        sintomas = ['Total_Sintoma1', 'Total_Sintoma2', 'Total_Sintoma3', 'Total_Sintoma4']
        sintomas_labels = ['Febre', 'Tosse', 'Dificuldade Respiratória', 'Dor de Cabeça']
        df_sintomas = df_merge[['CEP'] + sintomas].set_index('CEP')
        df_sintomas.columns = sintomas_labels
        
        fig, ax = plt.subplots(figsize=(12, 6))
        df_sintomas.plot(kind='bar', stacked=True, ax=ax, width=0.8)
        ax.set_title('Distribuição de Sintomas por Região', fontsize=12, pad=20)
        ax.set_ylabel('Total de Casos', fontsize=10)
        ax.set_xlabel('CEP', fontsize=10)
        ax.legend(title='Sintomas', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        st.pyplot(fig)
    
    with tabs[2]:
        col1, col2 = st.columns(2)
        with col1:
            fig, ax = plt.subplots(figsize=(8, 6))
            scatter = ax.scatter(df_merge['Media_Idade'], df_merge['Media_Escolaridade'],
                               s=df_merge['Soma_Populacao']/1000,
                               c=df_merge['Taxa_Internados'], cmap='coolwarm',
                               alpha=0.8)
            ax.set_title('Idade Média vs Escolaridade Média', fontsize=12, pad=20)
            ax.set_xlabel('Idade Média (anos)', fontsize=10)
            ax.set_ylabel('Escolaridade Média (anos)', fontsize=10)
            
            # Adicionando legenda manualmente
            cbar = plt.colorbar(scatter)
            cbar.set_label('Taxa de Internados (%)', rotation=270, labelpad=15)
            st.pyplot(fig)
        
        with col2:
            fig, ax = plt.subplots(figsize=(8, 6))
            explode = [0.05] * len(df_merge)
            wedges, texts, autotexts = ax.pie(df_merge['Soma_Populacao'], 
                                            labels=df_merge['CEP'], 
                                            autopct='%1.1f%%',
                                            startangle=90,
                                            explode=explode,
                                            shadow=True,
                                            colors=plt.cm.Pastel1.colors,
                                            textprops={'fontsize': 8})
            
            ax.set_title('Distribuição Populacional por CEP', fontsize=12, pad=20)
            plt.setp(autotexts, size=8, weight="bold")
            ax.axis('equal')
            st.pyplot(fig)

# ============================
# ALERTA MÉDIA DE ÓBITOS (T5)
# ============================
st.header("Monitoramento de Óbitos - Tratador 5")
df_obitos = carregar_dados(endpoints["/alerta-obitos"])

if not df_obitos.empty:
    df_obitos['Data'] = pd.to_datetime(df_obitos['Data'])
    df_obitos = df_obitos.sort_values('Data')
    
    media_obitos = df_obitos['N_obitos'].mean()
    df_obitos['Media_Movel'] = df_obitos['N_obitos'].rolling(window=7, min_periods=1).mean()
    df_obitos['Alerta'] = df_obitos['N_obitos'] > media_obitos
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Indicadores Chave")
        
        with st.container():
            cols = st.columns(2)
            with cols[0]:
                st.metric("Média Histórica", 
                         f"{media_obitos:.1f}",
                         help="Média diária de óbitos no período")
            with cols[1]:
                st.metric("Total de Óbitos", 
                         f"{int(df_obitos['N_obitos'].sum()):,}",
                         help="Acumulado no período")
        
        ultimo_registro = df_obitos.iloc[-1]
        delta = ultimo_registro['N_obitos'] - media_obitos
        
        with st.container():
            st.metric("Último Registro", 
                     f"{ultimo_registro['N_obitos']} óbitos",
                     delta=f"{delta:.1f} vs média",
                     delta_color="inverse" if delta > 0 else "normal",
                     help=f"Registro de {ultimo_registro['Data'].strftime('%d/%m/%Y')}")
        
        alert_container = st.container()
        if ultimo_registro['Alerta']:
            alert_container.error("""
            ⚠️ **ALERTA ATIVO**  
            Óbitos acima da média histórica
            """)
        else:
            alert_container.success("""
            ✅ **SITUAÇÃO NORMAL**  
            Óbitos dentro da média esperada
            """)
    
    with col2:
        st.subheader("Tendência Temporal")
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Linha principal
        ax.plot(df_obitos['Data'], df_obitos['N_obitos'], 
               label='Óbitos Diários', marker='o', 
               color='#4c72b0', linewidth=2, markersize=5)
        
        # Média móvel
        ax.plot(df_obitos['Data'], df_obitos['Media_Movel'], 
               label='Média Móvel (7 dias)', 
               color='#dd8452', linestyle='--', linewidth=2)
        
        # Média global
        ax.axhline(y=media_obitos, color='#c44e52', 
                  linestyle=':', label='Média Histórica', linewidth=2)
        
        # Alertas
        alertas = df_obitos[df_obitos['Alerta']]
        if not alertas.empty:
            ax.scatter(alertas['Data'], alertas['N_obitos'], 
                      color='#c44e52', s=100, label='Dias de Alerta', 
                      edgecolors='black', linewidth=0.5, zorder=5)
        
        ax.set_xlabel('Data', fontsize=10)
        ax.set_ylabel('Número de Óbitos', fontsize=10)
        ax.set_title('Evolução Diária de Óbitos com Indicadores', fontsize=12, pad=20)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True, linestyle='--', alpha=0.5)
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)
    
    if 'CEP' in df_obitos.columns:
        st.subheader("Distribuição por Região")
        
        cep_stats = df_obitos.groupby('CEP').agg({
            'N_obitos': ['sum', 'mean', 'count'],
            'Alerta': 'sum'
        }).sort_values(('N_obitos', 'sum'), ascending=False)
        
        cep_stats.columns = ['Total Óbitos', 'Média Diária', 'Dias Registrados', 'Dias em Alerta']
        cep_stats['% Dias em Alerta'] = (cep_stats['Dias em Alerta'] / cep_stats['Dias Registrados']) * 100
        
        st.dataframe(
            cep_stats.style
            .background_gradient(cmap='Reds', subset=['Total Óbitos', 'Média Diária'])
            .format({
                'Média Diária': '{:.1f}',
                '% Dias em Alerta': '{:.1f}%'
            })
            .bar(subset=['Dias em Alerta'], color='#ff686b'))