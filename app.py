import os
import logging
from typing import Optional
from dotenv import load_dotenv

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- 1. CONFIGURAÇÃO DE LOGS ---
logger = logging.getLogger("Dashboard_Regulacao")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- 2. ESTÉTICA E CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Monitor Regulação RJ", layout="wide", page_icon="📈")

CORES_GRAVIDADE = {
    "Vermelho": "#FF4B4B",
    "Laranja": "#FFA500",
    "Amarelo": "#F1C40F",
    "Verde": "#2ECC71"
}


@st.cache_resource
def iniciar_conexao() -> Optional[Engine]:
    """Cria a conexão inicial (em cache) com o banco de dados via SQLAlchemy e psycopg2."""
    load_dotenv()
    db_url = os.getenv('SUPABASE_DB_URL') or os.getenv('DATABASE_URL')
    
    if not db_url:
        st.error("⚠️ Configuração Incompleta: Variável 'SUPABASE_DB_URL' não encontrada.")
        logger.error("A URL de conexão não foi inserida no .env ou nas Variáveis de Servidor.")
        return None

    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql+psycopg2://', 1)
    
    try:
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        st.error("⚠️ Erro Crítico: Falha de comunicação com o Banco de Dados PostgreSQL.")
        logger.error(f"Stack Trace de Banco: {e}")
        return None


@st.cache_data(ttl=60)
def consultar_dados() -> pd.DataFrame:
    """Extrai os dados unidos da fila e das unidades mestre."""
    engine = iniciar_conexao()
    if engine is None:
        return pd.DataFrame()
        
    query =text("""
    SELECT 
        f.id_paciente, f.nome_anonimo, f.gravidade, f.procedimento_solicitado, 
        f.unidade_origem, f.data_solicitacao,
        u.cap, u.latitude, u.longitude 
    FROM fila_regulacao f
    LEFT JOIN unidades_saude u ON f.unidade_origem = u.nome_unidade
    """)
    
    try:
        with engine.connect() as conexao:
             df = pd.read_sql(query, conexao)
             
        # Tratamento seguro da CAP do Rio
        if 'cap' in df.columns:
             df['cap'] = df['cap'].fillna('N/I').astype(str)
        return df
        
    except Exception as e:
        logger.error(f"Falha na consulta cruzada (JOIN) via SQLAlchemy: {e}")
        st.warning("🔄 O banco de dados no momento não contém a tabela 'fila_regulacao'. Simule rodando os scripts Python de ETL primeiro.")
        return pd.DataFrame()


# --- INTERFACE PRINCIPAL ---
def renderizar_dashboard():
    df = consultar_dados()

    st.title("🏥 Gestão de Fluxo por CAP - Regulação Rio")
    st.markdown("Monitoramento avançado do fluxo de pacientes na fila do Sistema Único de Saúde (Rede Municipal/Estadual).")
    st.markdown("---")

    if df.empty:
        st.info("Nenhuma fila ativa processada nesse instante pela Secretaria de Saúde. Aguardando a carga de dados do ETL.")
        # Pode ter um botão manual para o cliente recarregar a visualização
        if st.button("🔄 Ver novamente"):
            st.cache_data.clear()
            st.rerun()
        return

    # --- MÉTRICAS DE TOPO ---
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total de Pacientes na Fila", f"{len(df):,}".replace(",", "."))
    
    with c2:
        criticos = len(df[df['gravidade'] == 'Vermelho'])
        st.metric("🔴 Alerta Máximo (Vermelho)", criticos)
        
    with c3:
        cap_mais_lotada = df['cap'].value_counts().idxmax() if not df.empty else "N/A"
        st.metric("📍 R. Administrativa em Alerta (CAP)", f"CAP {cap_mais_lotada}")
        
    with c4:
        st.metric("Hospitais / Clínicas Restritas", df['unidade_origem'].nunique())

    st.markdown("---")

    # --- ÁREA INTERATIVA: MAPA E FILTROS ---
    colunas_mapa, colunas_info = st.columns([2, 1])

    with colunas_info:
        st.subheader("🔍 Filtros Operacionais", divider='red')
        
        # Filtro de Regionais / CAPs
        lista_caps = sorted(df['cap'].unique())
        caps_selecionadas = st.multiselect("Filtrar Administrativamente (CAPs):", options=lista_caps, default=lista_caps)
        
        # Filtro de Risco
        grav_selecionada = st.multiselect(
            "Protocolo de Manchester (Risco):", 
            options=["Vermelho", "Laranja", "Amarelo", "Verde"], 
            default=["Vermelho", "Laranja", "Amarelo", "Verde"]
        )

        # Atualizando visualização (Slice)
        df_filtrado = df[df['cap'].isin(caps_selecionadas) & df['gravidade'].isin(grav_selecionada)]

    with colunas_mapa:
        st.subheader("📍 Distribuição Geográfica em Tempo Real", divider='red')
        df_mapa = df_filtrado.dropna(subset=['latitude', 'longitude'])
        
        if not df_mapa.empty:
            fig_mapa = px.scatter_mapbox(
                df_mapa, lat="latitude", lon="longitude", 
                color="gravidade", size_max=14, zoom=9.5,
                hover_name="unidade_origem",
                hover_data={"latitude": False, "longitude": False, "cap": True, "nome_anonimo": True, "procedimento_solicitado": True},
                color_discrete_map=CORES_GRAVIDADE,
                mapbox_style="carto-darkmatter"
            )
            fig_mapa.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=450)
            st.plotly_chart(fig_mapa, use_container_width=True)
        else:
            st.warning("⚠️ O mapa não pode ser exibido: As unidades não possuem licença ou coordenadas geospaciais catalogadas.")

    # --- ANÁLISE COMPLEMENTARES GRÁFICAS ---
    st.markdown("---")
    graf_col_1, graf_col_2 = st.columns(2)

    with graf_col_1:
        st.subheader("📊 Fila Bruta Por Coordenação (CAP)", divider="blue")
        cap_dist = df_filtrado['cap'].value_counts().reset_index()
        fig_barras = px.bar(cap_dist, x='cap', y='count', color='cap',
                            labels={'count': 'Indivíduos na Fila', 'cap': 'Micro Área'}, 
                            text='count')
        fig_barras.update_traces(textposition='outside')
        fig_barras.update_layout(showlegend=False, xaxis_title=None, yaxis_title="Pacientes")
        st.plotly_chart(fig_barras, use_container_width=True)

    with graf_col_2:
        st.subheader("⚖️ Mix da Sala Vermelha versus Protocolo", divider="blue")
        fig_pizza = px.pie(df_filtrado, names='gravidade', color='gravidade', 
                           color_discrete_map=CORES_GRAVIDADE, hole=0.55)
        fig_pizza.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pizza, use_container_width=True)

    # --- TABELA LOGÍSTICA (DataGrid) ---
    st.subheader("📋 Logística Hospitalar - Visão do Prontuário Clínico", divider='green')
    
    st.dataframe(
        df_filtrado[['nome_anonimo', 'gravidade', 'cap', 'unidade_origem', 'procedimento_solicitado', 'data_solicitacao']], 
        use_container_width=True, 
        hide_index=True,
        column_config={
              "nome_anonimo": "Paciente (Abreviado)",
              "gravidade": "Classificação",
              "cap": "Zona (CAP)",
              "unidade_origem": "Unidade Solicitante",
              "procedimento_solicitado": "Recurso / Especialidade",
              "data_solicitacao": st.column_config.DatetimeColumn("Horário da Entrada", format="DD/MM/YYYY hh:mm a")
        }
    )

    # Fica um rodapé opcional interativo da sidebar apenas para forçar sync
    st.sidebar.title("⚙️ Painel de Operações")
    st.sidebar.info("A sincronização ocorre automaticamente no Postgres. Utilize o botão para um carregamento emergencial.")
    if st.sidebar.button("🔄 Atualizar Cache de Servidor"):
         st.cache_data.clear()
         st.rerun()

if __name__ == '__main__':
    renderizar_dashboard()