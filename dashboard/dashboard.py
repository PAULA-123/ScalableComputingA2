import streamlit as st
import pandas as pd
import requests
import matplotlib.pyplot as plt
import time
from datetime import datetime
import pytz  # Adicionando a biblioteca de fuso horário

plt.style.use('ggplot')

st.set_page_config(
    page_title="Dashboard Integrado de Saúde Pública",
    layout="wide"
)

st.title("Dashboard de Métricas de Saúde Pública")

# Configuração do fuso horário
timezone = pytz.timezone('America/Sao_Paulo')

base_api = st.sidebar.text_input("API Base", value="http://api:8000")

endpoints = {
    "/merge-cep": "/merge-cep",
    "/alerta-obitos": "/alerta-obitos",
    "/correlacao": "/correlacao",
    "/desvios": "/desvios",
    "/regressao": "/regressao",
    "/media-movel": "/media-movel"
}

# Configuração do auto-refresh
refresh_interval = 7  # segundos
last_refresh = st.sidebar.empty()
auto_refresh = st.sidebar.checkbox('Atualização Automática', value=True, key="auto_refresh_checkbox")

@st.cache_data(ttl=5)
def carregar_dados(endpoint_key):
    try:
        if endpoint_key not in endpoints:
            return pd.DataFrame()

        endpoint = endpoints[endpoint_key]
        url = base_api + endpoint
        response = requests.get(url, timeout=5)

        if response.status_code != 200:
            return pd.DataFrame()

        data = response.json()
        if isinstance(data, list):
            return pd.DataFrame(data)
        return pd.DataFrame([data])

    except Exception:
        return pd.DataFrame()

# Função para gerar keys únicas apenas para elementos que precisam
def generate_key(base_name):
    return f"{base_name}_{int(time.time())}"

def main_dashboard():
    realtime_container = st.empty()
    
    while True:
        if auto_refresh:
            with realtime_container.container():
                # Obtém o horário correto com o fuso horário
                now = datetime.now(timezone)
                last_refresh.text(f"Última atualização: {now.strftime('%H:%M:%S')}")
                
                # ============================
                # MERGE POR CEP
                # ============================
                df_merge = carregar_dados("/merge-cep")
                if not df_merge.empty:
                    st.header("Merge por CEP - Dados Integrados (Tempo Real)")

                    df_merge['CEP'] = df_merge['CEP'].astype(str)
                    df_merge['Taxa_Internados'] = df_merge['Total_Internados'] / df_merge['Populacao'] * 100
                    df_merge['Taxa_Vacinados'] = df_merge['Total_Vacinados'] / df_merge['Populacao'] * 100

                    col1, col2 = st.columns([2, 1])
                    with col1:
                        st.subheader("Tabela Consolidada")
                        st.dataframe(df_merge.style.format({
                            'Taxa_Internados': '{:.2f}%',
                            'Taxa_Vacinados': '{:.2f}%',
                            'Media_Idade': '{:.1f}',
                            'Media_Escolaridade': '{:.1f}'
                        }).background_gradient(cmap='Blues'))

                    with col2:
                        st.subheader("Filtros")
                        # Apenas o selectbox precisa de key única
                        cep_selecionado = st.selectbox(
                            "Selecione um CEP:", 
                            df_merge['CEP'].unique(), 
                            key=generate_key("selectbox_cep")
                        )
                        df_cep = df_merge[df_merge['CEP'] == cep_selecionado].iloc[0]

                        # Métricas não precisam de key
                        st.metric("População", f"{df_cep['Populacao']:,}")
                        st.metric("Internados", f"{df_cep['Total_Internados']:,}", 
                                 delta=f"{df_cep['Taxa_Internados']:.2f}%")
                        st.metric("Vacinados", f"{df_cep['Total_Vacinados']:,}", 
                                 delta=f"{df_cep['Taxa_Vacinados']:.2f}%")
                        st.metric("Escolaridade", f"{df_cep['Media_Escolaridade']:.1f} anos")

                    tabs = st.tabs(["Vacinação vs Internações", "Sintomas", "Demografia"])
                    
                    with tabs[0]:
                        fig, ax = plt.subplots(figsize=(10, 6))
                        scatter = ax.scatter(df_merge['Taxa_Vacinados'], df_merge['Taxa_Internados'],
                                           c=df_merge['Media_Idade'], s=df_merge['Populacao']/50,
                                           cmap='viridis', alpha=0.7)
                        ax.set_xlabel('Taxa de Vacinação (%)')
                        ax.set_ylabel('Taxa de Internações (%)')
                        ax.set_title('Relação Vacinação x Internações (Tempo Real)')
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
                                   c=df_merge['Taxa_Vacinados'], cmap='coolwarm', 
                                   s=100, alpha=0.7)
                        ax.set_xlabel('Média Escolaridade')
                        ax.set_ylabel('Média Idade')
                        ax.set_title('Idade vs Escolaridade')
                        plt.colorbar(ax.collections[0], label='Taxa Vacinação')
                        st.pyplot(fig)

                # ============================
                # ALERTA DE ÓBITOS
                # ============================
                df_obitos = carregar_dados("/alerta-obitos")
                if not df_obitos.empty:
                    st.header("Monitoramento de Óbitos (Tempo Real)")
                    
                    df_obitos['Data'] = pd.to_datetime(df_obitos['Data'])
                    df_obitos = df_obitos.sort_values('Data')
                    media_obitos = df_obitos['N_obitos'].mean()
                    df_obitos['Media_Movel'] = df_obitos['N_obitos'].rolling(7).mean()
                    
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.metric("Média Histórica", f"{media_obitos:.1f}")
                        st.metric("Total Óbitos", f"{df_obitos['N_obitos'].sum():,}")
                        
                        ult = df_obitos.iloc[-1]
                        delta = ult['N_obitos'] - media_obitos
                        st.metric("Último Registro", f"{ult['N_obitos']}", 
                                 delta=f"{delta:.1f}",
                                 delta_color="inverse" if delta > 0 else "normal")
                        
                        if ult['N_obitos'] > media_obitos:
                            st.error("⚠️ Alerta: Óbitos acima da média")
                        else:
                            st.success("✅ Situação Normal")

                    with col2:
                        fig, ax = plt.subplots(figsize=(12, 6))
                        ax.plot(df_obitos['Data'], df_obitos['N_obitos'], 
                                label="Óbitos Diários", marker='o')
                        ax.plot(df_obitos['Data'], df_obitos['Media_Movel'], 
                                label="Média Móvel (7d)", linestyle="--")
                        ax.axhline(media_obitos, color='r', linestyle=':', 
                                  label="Média Histórica")
                        ax.legend()
                        ax.set_title("Evolução de Óbitos (Tempo Real)")
                        ax.set_xlabel("Data")
                        ax.set_ylabel("Óbitos")
                        st.pyplot(fig)

                # ============================
                # CORRELAÇÃO
                # ============================
                df_corr = carregar_dados("/correlacao")
                if not df_corr.empty:
                    st.header("Correlação Escolaridade x Vacinação")
                    valor = df_corr.iloc[-1]['Escolaridade']
                    
                    col1, col2, col3 = st.columns([1,2,1])
                    with col2:
                        st.metric("Correlação de Pearson", f"{valor:.4f}",
                                help="Entre escolaridade média e vacinação por CEP")
                    
                    if abs(valor) > 0.7:
                        st.success("Correlação forte detectada")
                    elif abs(valor) > 0.4:
                        st.info("Correlação moderada")
                    else:
                        st.warning("Correlação fraca")

                # ============================
                # DESVIOS PADRÃO
                # ============================
                df_desvios = carregar_dados("/desvios")
                if not df_desvios.empty:
                    st.header("Desvios Padrão das Variáveis")
                    st.bar_chart(data=df_desvios.set_index("variavel")["desvio"])
                
                # ============================
                # REGRESSÃO LINEAR
                # ============================                
                df_reg = carregar_dados("/regressao")
                df_merge_reg = carregar_dados("/merge-cep")

                if not df_reg.empty and not df_merge_reg.empty:
                    st.header("Regressão Linear - Escolaridade vs Vacinação")
                    
                    st.subheader("Coeficientes Estimados")
                    st.dataframe(df_reg)

                    ultima = df_reg.iloc[-1]
                    beta0, beta1 = ultima["beta0"], ultima["beta1"]

                    st.metric("Intercepto (β₀)", f"{beta0:.4f}")
                    st.metric("Inclinação (β₁)", f"{beta1:.4f}")

                    st.markdown(f"""
                    **Equação estimada:**  
                    `Total_Vacinados = {beta0:.4f} + {beta1:.4f} * Media_Escolaridade`
                    """)

                    st.subheader("Visualização da Regressão Linear")

                    df_plot = df_merge_reg[["Media_Escolaridade", "Total_Vacinados"]].dropna()
                    df_plot = df_plot.sort_values("Media_Escolaridade")

                    # Geração dos pontos da reta estimada
                    x_vals = df_plot["Media_Escolaridade"]
                    y_vals = beta0 + beta1 * x_vals

                    fig, ax = plt.subplots(figsize=(10, 6))
                    ax.scatter(df_plot["Media_Escolaridade"], df_plot["Total_Vacinados"], color="blue", alpha=0.6, label="Observações")
                    ax.plot(x_vals, y_vals, color="red", linewidth=2, label="Reta Estimada")

                    ax.set_xlabel("Média de Escolaridade")
                    ax.set_ylabel("Total de Vacinados")
                    ax.set_title("Regressão Linear: Vacinados vs Escolaridade")
                    ax.legend()
                    st.pyplot(fig)
                
                # ============================
                # MÉDIA MÓVEL
                # ============================
                df_media = carregar_dados("/media-movel")
                if not df_media.empty:
                    st.header("Tendência de Óbitos (Média Móvel)")
                    df_media['Data'] = pd.to_datetime(df_media['Data'])
                    df_media = df_media.sort_values('Data')
                    
                    fig, ax = plt.subplots(figsize=(10, 4))
                    ax.plot(df_media['Data'], df_media['media_movel'], 
                            label='Média Móvel 7 dias', color='purple')
                    ax.set_xlabel('Data')
                    ax.set_ylabel('Óbitos')
                    ax.set_title('Tendência de Óbitos (Suavizada)')
                    ax.legend()
                    st.pyplot(fig)
                    
                # ============================
                # DADOS HISTÓRICOS
                # ============================
                df_historico = carregar_dados("/historico")
                if not df_historico.empty:
                    st.header("Análise Histórica")
                    
                    # Processa os dados históricos
                    df_historico['AnoMes'] = pd.to_datetime(df_historico['AnoMes'], format='%Y%m')
                    df_historico = df_historico.sort_values('AnoMes')
                    
                    # Cria abas para diferentes visualizações
                    tab1, tab2, tab3 = st.tabs(["Internações", "Vacinação", "Sintomas"])
                    
                    with tab1:
                        fig, ax = plt.subplots(figsize=(12, 6))
                        ax.plot(df_historico['AnoMes'], df_historico['Total_Internados'], 
                                label='Internações', color='red')
                        ax.set_title('Evolução Mensal de Internações')
                        ax.set_xlabel('Mês/Ano')
                        ax.set_ylabel('Número de Internações')
                        ax.grid(True, linestyle='--', alpha=0.7)
                        ax.legend()
                        st.pyplot(fig)
                        
                    with tab2:
                        fig, ax = plt.subplots(figsize=(12, 6))
                        ax.bar(df_historico['AnoMes'], df_historico['Total_Vacinados'], 
                            color='green', alpha=0.7, label='Vacinados')
                        ax.set_title('Evolução Mensal de Vacinação')
                        ax.set_xlabel('Mês/Ano')
                        ax.set_ylabel('Número de Vacinados')
                        ax.grid(True, linestyle='--', alpha=0.7)
                        ax.legend()
                        st.pyplot(fig)
                        
                    with tab3:
                        fig, ax = plt.subplots(figsize=(12, 6))
                        sintomas = ['Total_Sintoma1', 'Total_Sintoma2', 'Total_Sintoma3', 'Total_Sintoma4']
                        labels = ['Febre', 'Tosse', 'Dificuldade Respiratória', 'Dor de Cabeça']
                        
                        bottom = None
                        for i, sintoma in enumerate(sintomas):
                            ax.bar(df_historico['AnoMes'], df_historico[sintoma], 
                                bottom=bottom, label=labels[i])
                            if bottom is None:
                                bottom = df_historico[sintoma]
                            else:
                                bottom += df_historico[sintoma]
                        
                        ax.set_title('Distribuição Mensal de Sintomas')
                        ax.set_xlabel('Mês/Ano')
                        ax.set_ylabel('Número de Casos')
                        ax.legend()
                        ax.grid(True, linestyle='--', alpha=0.7)
                        st.pyplot(fig)

                        if auto_refresh:
                            time.sleep(refresh_interval)
                        else:
                            break

if __name__ == "__main__":
    main_dashboard()