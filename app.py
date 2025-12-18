import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# 1. Estética e Configuração
st.set_page_config(page_title="Monitor Regulação RJ", layout="wide", page_icon="📈")

CORES_GRAVIDADE = {
    "Vermelho": "#FF4B4B",
    "Laranja": "#FFA500",
    "Amarelo": "#F1C40F",
    "Verde": "#2ECC71"
}

# Conexão
load_dotenv()
db_url = os.getenv('SUPABASE_DB_URL')
if db_url and db_url.startswith('postgres://'):
    db_url = db_url.replace('postgres://', 'postgresql+psycopg2://', 1)
engine = create_engine(db_url)

@st.cache_data(ttl=60)
def get_data():
    # JOIN focado em CAP e Coordenadas
    query = """
    SELECT f.*, u.cap, u.latitude, u.longitude 
    FROM fila_regulacao f
    LEFT JOIN unidades_saude u ON f.unidade_origem = u.nome_unidade
    """
    df = pd.read_sql(query, engine)
    # Garante que a CAP seja tratada como texto/categoria
    if 'cap' in df.columns:
        df['cap'] = df['cap'].fillna('N/I').astype(str)
    return df

# --- INTERFACE ---
df = get_data()

st.title("🏥 Gestão de Fluxo por CAP - Regulação Rio")
st.markdown("---")

# --- MÉTRICAS DE TOPO (Focado em CAP e Criticidade) ---
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Total de Pacientes", len(df))
with c2:
    # Ajuste para capturar a prioridade máxima corretamente
    criticos = len(df[df['gravidade'] == 'Vermelho'])
    st.metric("🔴 Vaga Zero (Vermelho)", criticos)
with c3:
    cap_mais_lotada = df['cap'].value_counts().idxmax() if not df.empty else "N/A"
    st.metric("📍 CAP em Alerta", f"CAP {cap_mais_lotada}")
with c4:
    st.metric("Unidades Solicitantes", df['unidade_origem'].nunique())

st.markdown("---")

# --- ÁREA INTERATIVA: MAPA E FILTROS ---
col_mapa, col_info = st.columns([2, 1])

with col_info:
    st.subheader("🔍 Filtros Estratégicos")
    # Filtro por CAP (O que você sugeriu como mais importante)
    lista_caps = sorted(df['cap'].unique())
    caps_selecionadas = st.multiselect("Selecionar CAPs", options=lista_caps, default=lista_caps)
    
    # Filtro por Gravidade
    grav_selecionada = st.multiselect("Nível de Urgência", options=["Vermelho", "Laranja", "Amarelo", "Verde"], default=["Vermelho", "Laranja", "Amarelo", "Verde"])

    # Aplicando filtros
    df_filtrado = df[df['cap'].isin(caps_selecionadas) & df['gravidade'].isin(grav_selecionada)]

with col_mapa:
    st.subheader("📍 Distribuição Geográfica")
    df_mapa = df_filtrado.dropna(subset=['latitude', 'longitude'])
    
    if not df_mapa.empty:
        fig_mapa = px.scatter_mapbox(
            df_mapa, lat="latitude", lon="longitude", 
            color="gravidade", size_max=12, zoom=9,
            hover_name="unidade_origem",
            hover_data={"latitude": False, "longitude": False, "cap": True, "nome_anonimo": True},
            color_discrete_map=CORES_GRAVIDADE,
            mapbox_style="carto-darkmatter"
        )
        fig_mapa.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=450)
        st.plotly_chart(fig_mapa, use_container_width=True)
    else:
        st.warning("⚠️ Mapa indisponível: As unidades filtradas não possuem coordenadas GPS cadastradas.")

# --- ANÁLISE POR CAP ---
st.markdown("---")
col_graf1, col_graf2 = st.columns(2)

with col_graf1:
    st.subheader("📊 Pacientes por CAP")
    # Gráfico de barras por CAP
    cap_dist = df_filtrado['cap'].value_counts().reset_index()
    fig_bar = px.bar(cap_dist, x='cap', y='count', color='cap', title="Volume por Região Administrativa")
    st.plotly_chart(fig_bar, use_container_width=True)

with col_graf2:
    st.subheader("⚖️ Mix de Gravidade")
    fig_pizza = px.pie(df_filtrado, names='gravidade', color='gravidade', 
                       color_discrete_map=CORES_GRAVIDADE, hole=0.5)
    st.plotly_chart(fig_pizza, use_container_width=True)

# --- TABELA DE OPERAÇÃO ---
st.subheader("📋 Fila de Espera Detalhada (Logística)")
st.dataframe(
    df_filtrado[['nome_anonimo', 'gravidade', 'cap', 'unidade_origem', 'procedimento_solicitado', 'data_solicitacao']], 
    use_container_width=True, hide_index=True
)

st.sidebar.button("🔄 Atualizar Banco")