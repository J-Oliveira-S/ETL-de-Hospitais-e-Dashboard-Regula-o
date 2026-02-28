import textwrap
import logging
import os
import re
from pathlib import Path
from typing import Optional, Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- CONFIGURAÇÃO DE LOGS ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)
logger = logging.getLogger("Transform_Unidades")

# --- 1. Configuração de Caminhos ---
BASE_DIR: Path = Path(__file__).resolve().parent.parent
IN_PATH: Path = BASE_DIR / "data" / "raw_unidades.csv"
OUT_PATH: Path = BASE_DIR / "data" / "unidades_transformed.csv"

# Mapeamento: X -> latitude, Y -> longitude
MAPA_COLUNAS: dict = {
    'X': 'latitude', 
    'Y': 'longitude',
    'NOME': 'nome_unidade', 
    'TIPO_UNIDADE': 'tipo', 
    'ENDERECO': 'endereco',
    'BAIRRO': 'bairro', 
    'TELEFONE': 'telefone', 
    'EMAIL': 'email',
    'HORARIO_SEMANA': 'horario_semana', 
    'HORARIO_SABADO': 'horario_sabado',
    'TIPO_ABC': 'tipo_abc', 
    'CNES': 'cnes', 
    'DATA_INAUGURACAO': 'data_inauguracao',
    'Flg_Ativo': 'ativo', 
    'OBJECTID': 'objectid', 
    'GlobalID': 'globalid',
    'CAP': 'cap', 
    'EQUIPES': 'equipes'
}


def para_booleano(valor: Any) -> Optional[bool]:
    """
    Converte diferentes representações de verdadeiro/falso em Booleanos nativos do Python.
    """
    if pd.isna(valor) or str(valor).strip() == '' or str(valor).lower() == 'nan':
        return None
    
    texto = str(valor).strip().lower()
    
    if texto in ('1', '1.0', 'true', 't', 'sim', 's', 'yes', 'y'):
        return True
    if texto in ('0', '0.0', 'false', 'f', 'nao', 'não', 'n', 'no'):
        return False
        
    return None


def transformar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Executa a higienização dos dados vindos da tabela bruta de unidades de saúde.
    """
    logger.info("Aplicando mapeamento de colunas...")
    df = df.rename(columns=MAPA_COLUNAS)
    
    # Tratamento de Coordenadas (Latitude e Longitude)
    logger.info("Ajustando formato das coordenadas geográficas...")
    for coord in ['latitude', 'longitude']:
        if coord in df.columns:
            # Substitui vírgula por ponto (caso modelo PT-BR local) e converte para número (float)
            df[coord] = df[coord].astype(str).str.replace(',', '.')
            df[coord] = pd.to_numeric(df[coord], errors='coerce')

    # Tratamento de Datas
    if 'data_inauguracao' in df.columns:
        logger.info("Convertendo colunas de data (Inauguração)...")
        df['data_inauguracao'] = pd.to_datetime(df['data_inauguracao'], errors='coerce')
        df['data_inauguracao'] = df['data_inauguracao'].apply(lambda x: x.date() if pd.notnull(x) else None)

    # Tratamento Numérico (Correções de ID e Cnes)
    if 'objectid' in df.columns:
        df['objectid'] = pd.to_numeric(df['objectid'], errors='coerce').astype('Int64')
    
    if 'cnes' in df.columns:
        df['cnes'] = df['cnes'].astype(str).str.replace(r'\D', '', regex=True)
        # remove "nan" string se vazou no regexp
        df.loc[df['cnes'] == 'nan', 'cnes'] = None

    # Tratamento Booleano
    if 'ativo' in df.columns:
        logger.info("Padronizando a coluna Ativo/Inativo...")
        df['ativo'] = df['ativo'].apply(para_booleano)

    # Limpeza Geral de Texto (Padronização para facilitar visualização no Dashboard)
    df['nome_unidade'] = df['nome_unidade'].astype(str).str.title()
    df['municipio'] = 'Rio de Janeiro'
    
    # Validação fundamental:
    # 1. Remover unidades sem Nome.
    # 2. Retornar apenas as colunas válidas (que estão no MAPA) para o modelo.
    linhas_antes = len(df)
    df = df.dropna(subset=['nome_unidade'])
    logger.info(f"Omitidas {linhas_antes - len(df)} linhas invalidas por ausência do Nome da Unidade.")
    
    colunas_validas = [valor for valor in MAPA_COLUNAS.values() if valor in df.columns]
    
    logger.info("Transformação nos dados Concluída com sucesso!")
    return df[colunas_validas]


def garantir_url_banco(db_url: str) -> str:
    """Verifica e adapta o sufixo postgres->postgresql."""
    if db_url.startswith('postgres://'):
        return db_url.replace('postgres://', 'postgresql+psycopg2://', 1)
    if db_url.startswith('postgresql://'):
         return db_url.replace('postgresql://', 'postgresql+psycopg2://', 1)
    return db_url


def carregar_dados_no_banco(df: pd.DataFrame, nome_tabela: str = 'unidades_saude') -> None:
    """
    Carrega o DataFrame consolidado no Supabase/PostgreSQL sobrescrevendo a tabela anterior (TRUNCATE).
    """
    load_dotenv()
    db_url = os.getenv('SUPABASE_DB_URL') or os.getenv("DATABASE_URL")
    
    if not db_url:
        logger.error("A URL do Banco (SUPABASE_DB_URL) não foi localizada no .env.")
        raise ValueError("Configuração Inválida.")

    url_formatada = garantir_url_banco(db_url)
    try:
        engine = create_engine(url_formatada)
    except Exception as e:
        logger.critical(f"Falha na criação da conexão SQLAlchemy: {e}")
        raise

    # Como são dados mestre (Master Data) muitas vezes sobrescrevemos na carga
    logger.info(f"Iniciando Truncate/Limpeza na tabela '{nome_tabela}'...")
    try:
         with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {nome_tabela} RESTART IDENTITY CASCADE;"))
    except Exception as e:
         logger.warning(f"Ocorreu um aviso durante a execução do TRUNCATE. Possivelmente a tabela não existia: {e}")

    logger.info(f"Inserindo novos os {len(df)} registros atualizados...")
    
    try:
        # Inserção direta utilizando To_Sql
        df.to_sql(nome_tabela, con=engine, if_exists='append', index=False, method='multi')
        logger.info(f"🚀 SUCESSO! Carga finalizada com {len(df)} registros integrados nativamente.")
    except Exception as erro_carga:
        logger.error(f"⚠️ Erro Crítico durante a injeção do To_Sql: {erro_carga}")
        raise


def iniciar_fluxo_unidades():
    """Script Mestre que carrega da Origem, Trata e Envia ao Destino."""
    logger.info("--- Iniciando Serviço: Tratamento Base (Unidades Saúde) ---")

    if not IN_PATH.exists():
        logger.error(f"O Arquivo CSV bruto (RAW) não pôde ser encontrado: {IN_PATH}")
        return
    
    logger.info("Realizando Leitura Inicial de Dados (Raw CSV)...")
    try:
        df_bruto = pd.read_csv(IN_PATH, sep=None, engine='python', on_bad_lines='skip', encoding='utf-8-sig')
    except Exception as e:
         logger.error(f"Falha na leitura do arquivo pandas: {e}")
         return
         
    # Transformar para o layout limpo (Ouro)
    df_limpo = transformar_dados(df_bruto)
    
    # Backup local
    df_limpo.to_csv(OUT_PATH, index=False)
    logger.info(f"Cópia de segurança salva em formato CSV: {OUT_PATH}")
    
    try:
        carregar_dados_no_banco(df_limpo)
    except Exception as banco_erro:
        logger.critical(f"A execução foi interrrompida: Erro no Banco -> {banco_erro}")

if __name__ == '__main__':
    iniciar_fluxo_unidades()
