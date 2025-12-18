import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Configuração da página
st.set_page_config(page_title="Painel Regulação RJ", layout="wide", page_icon="🏥")

# Conexão com o Banco (Supabase)
load_dotenv()
db_url = os.getenv('SUPABASE_DB_URL')
if db_url and db_url.startswith('postgres://'):
    db_url = db_url.replace('postgres://', 'postgresql+psycopg2://', 1)

engine = create_engine(db_url)

# Função para buscar dados cruzados (JOIN)
@st.cache_data(ttl=60) 
def get_combined_data():
    query = """
    SELECT 
        f.nome_anonimo, 
        f.gravidade, 
        f.procedimento_solicitado, 
        f.data_solicitacao,
        u.nome_unidade, 
        u.bairro, 
        u.telefone, 
        u.endereco,
        u.latitude,
        u.longitude
    FROM public.fila_regulacao f
    JOIN public.unidades_saude u ON f.unidade_origem = u.nome_unidade;
    """
    return pd.read_sql(query, engine)

st.title("🏥 Gestão de Fluxo e Regulação - Rio de Janeiro")
st.markdown(f"**Status da Rede:** Conectado ao Supabase (Ohio-US)")
st.markdown("---")

# Buscando os dados
try:
    df = get_combined_data()

    # 1. Métricas de Impacto
    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Pacientes em Fila", len(df))
    with m2:
        # Conta pacientes com gravidade máxima (Vaga Zero)
        vaga_zero = len(df[df['gravidade'].str.upper().str.contains('MÁXIMA|ALTA', na=False)])
        st.metric("Prioridade Crítica", vaga_zero, delta_color="inverse")
    with m3:
        st.metric("Unidades com Pendência", df['nome_unidade'].nunique())

    st.markdown("---")

    # 2. Mapa de Calor (Onde estão os pacientes)
    st.subheader("📍 Mapa de Concentração da Fila")
    df_mapa = df.dropna(subset=['latitude', 'longitude'])
    if not df_mapa.empty:
        st.map(df_mapa, latitude='latitude', longitude='longitude', size=20, color='#FF0000')
    else:
        st.info("Nenhum paciente na fila possui coordenadas mapeadas.")

    # 3. Gráfico de Gravidade e Tabela Detalhada
    col_esq, col_dir = st.columns([1, 2])

    with col_esq:
        st.subheader("📊 Nível de Gravidade")
        fig = px.pie(df, names='gravidade', hole=0.4, color_discrete_sequence=px.colors.sequential.Reds)
        st.plotly_chart(fig, use_container_width=True)

    with col_dir:
        st.subheader("📋 Lista Detalhada para Contato")
        # Filtro rápido por bairro
        bairro = st.selectbox("Filtrar por Bairro da Unidade", ["Todos"] + list(df['bairro'].unique()))
        
        df_display = df.copy()
        if bairro != "Todos":
            df_display = df_display[df_display['bairro'] == bairro]
            
        st.dataframe(
            df_display[['nome_anonimo', 'gravidade', 'nome_unidade', 'bairro', 'telefone', 'procedimento_solicitado']], 
            use_container_width=True
        )

except Exception as e:
    st.error(f"Erro ao carregar dashboard: {e}")
    st.info("Dica: Verifique se os nomes das unidades na Fila de Regulação são idênticos aos da tabela Unidades de Saúde.")

st.sidebar.button("🔄 Atualizar Dados")