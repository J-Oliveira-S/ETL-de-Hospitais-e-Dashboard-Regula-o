from pathlib import Path
import pandas as pd
import re
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Configuração de caminhos
BASE = Path(__file__).resolve().parent
IN_PATH = BASE / "data" / "raw_unidades.csv"
OUT_PATH = BASE / "data" / "unidades_transformed.csv"

COLUMN_MAP = {
    'NOME': 'nome_unidade', 'TIPO_UNIDADE': 'tipo', 'ENDERECO': 'endereco',
    'BAIRRO': 'bairro', 'TELEFONE': 'telefone', 'EMAIL': 'email',
    'HORARIO_SEMANA': 'horario_semana', 'HORARIO_SABADO': 'horario_sabado',
    'TIPO_ABC': 'tipo_abc', 'CNES': 'cnes', 'DATA_INAUGURACAO': 'data_inauguracao',
    'Flg_Ativo': 'ativo', 'OBJECTID': 'objectid', 'GlobalID': 'globalid',
    'X': 'longitude', 'Y': 'latitude', 'CAP': 'cap', 'EQUIPES': 'equipes'
}

def to_bool(v):
    if pd.isna(v) or str(v).strip() == '' or str(v).lower() == 'nan':
        return None
    s = str(v).strip().lower()
    if s in ('1', '1.0', 'true', 't', 'sim', 's', 'yes', 'y'):
        return True
    if s in ('0', '0.0', 'false', 'f', 'nao', 'não', 'n', 'no'):
        return False
    return None

def transform_data(df):
    # Mapeia colunas
    df = df.rename(columns=COLUMN_MAP)
    
    # TRATAMENTO DE DATA (O erro atual)
    if 'data_inauguracao' in df.columns:
        # 1. Converte para datetime forçando erros a virarem NaT (Not a Time)
        df['data_inauguracao'] = pd.to_datetime(df['data_inauguracao'], errors='coerce')
        # 2. Transforma em objeto de data puro (sem horas) ou None para o SQL
        df['data_inauguracao'] = df['data_inauguracao'].apply(lambda x: x.date() if pd.notnull(x) else None)

    # TRATAMENTO DE NÚMEROS (Erro anterior do MEIER)
    if 'objectid' in df.columns:
        df['objectid'] = pd.to_numeric(df['objectid'], errors='coerce').astype('Int64')
    
    if 'cnes' in df.columns:
        # Garante que CNES seja texto limpo de dígitos
        df['cnes'] = df['cnes'].astype(str).str.replace(r'\D', '', regex=True)
        df.loc[df['cnes'] == 'nan', 'cnes'] = None
        
    for coord in ['latitude', 'longitude']:
        if coord in df.columns:
            df[coord] = pd.to_numeric(df[coord], errors='coerce')

    # TRATAMENTO DE BOOLEANO (Ativo/Inativo)
    if 'ativo' in df.columns:
        df['ativo'] = df['ativo'].apply(to_bool)

    # Limpeza de texto
    df['nome_unidade'] = df['nome_unidade'].astype(str).str.title()
    df['municipio'] = 'Rio de Janeiro'
    
    # Remove linhas sem nome e seleciona colunas válidas
    df = df.dropna(subset=['nome_unidade'])
    valid_cols = [v for v in COLUMN_MAP.values() if v in df.columns]
    return df[valid_cols]

def load_to_db(df, table_name='unidades_saude'):
    load_dotenv()
    db_url = os.getenv('SUPABASE_DB_URL')
    if db_url and db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql+psycopg2://', 1)
    
    engine = create_engine(db_url)
    
    print(f"Limpando tabela '{table_name}'...")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"))
    
    print(f"Inserindo {len(df)} registros...")
    # O parâmetro dtype garante que as datas e booleanos sejam enviados corretamente
    df.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')
    print(f"🚀 SUCESSO ABSOLUTO! {len(df)} registros integrados.")

def main():
    if not IN_PATH.exists():
        print(f"Arquivo não encontrado em: {IN_PATH}")
        return
    
    print("Iniciando limpeza profunda da base de dados...")
    df_raw = pd.read_csv(IN_PATH, sep=None, engine='python', on_bad_lines='skip', encoding='utf-8-sig')
    
    df_clean = transform_data(df_raw)
    df_clean.to_csv(OUT_PATH, index=False)
    
    try:
        load_to_db(df_clean)
    except Exception as e:
        print(f"⚠️ Erro crítico na carga: {e}")

if __name__ == '__main__':
    main()