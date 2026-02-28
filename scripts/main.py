#!/usr/bin/env python3
"""
ETL principal para lista de regulação hospitalar.

Passos:
- Lê `data/dados_regulacao.csv`
- Remove duplicatas
- Anonimiza nomes (iniciais)
- Converte `data_solicitacao` para datetime
- Insere registros limpos em `fila_regulacao` no Supabase (Postgres) via SQLAlchemy

Configure seu `.env` com a variável `SUPABASE_DB_URL` antes de rodar.
"""
import os
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- CONFIGURAÇÃO DE LOGS ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ETL_Principal")


def anonimizar_nome(nome: Optional[str]) -> Optional[str]:
    """
    Substitui o nome completo do paciente por suas iniciais por motivos de LGPD.
    Exemplo: 'João Silva Oliveira' -> 'J. O.'
    """
    if pd.isna(nome) or nome is None:
        return None
    
    partes = str(nome).strip().split()
    if not partes:
        return ""
    if len(partes) == 1:
        return f"{partes[0][0].upper()}."
    
    return f"{partes[0][0].upper()}. {partes[-1][0].upper()}."


def ajustar_url_sqlalchemy(db_url: str) -> str:
    """
    Formata a string de conexão para que o SQLAlchemy utilize o driver psycopg2 corretamente.
    """
    if db_url.startswith("postgres://"):
        return db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return db_url


def extrair_e_transformar(caminho_csv: Path) -> pd.DataFrame:
    """
    Lê o arquivo CSV, converte tipos de dados, remove duplicatas e anonimiza dados sensíveis (LGPD).
    """
    logger.info(f"Lendo dados brutos do arquivo: {caminho_csv.name}")
    try:
        df = pd.read_csv(caminho_csv)
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {caminho_csv}")
        raise
    except Exception as e:
        logger.error(f"Erro ao ler o CSV: {e}")
        raise

    # 1. Converter datas
    logger.info("Convertendo colunas de data/hora...")
    df["data_solicitacao"] = pd.to_datetime(df["data_solicitacao"], errors="coerce")

    # 2. Remover duplicatas exatas
    total_antes = len(df)
    df = df.drop_duplicates()
    total_depois = len(df)
    if total_antes - total_depois > 0:
        logger.info(f"Removidas {total_antes - total_depois} linhas duplicatas.")
    else:
        logger.info("Nenhuma linha duplicada encontrada.")

    # 3. Anonimizar nomes (LGPD)
    logger.info("Aplicando anonimização nos nomes dos pacientes (LGPD)...")
    df["nome_anonimo"] = df["nome_paciente"].apply(anonimizar_nome)

    # 4. Selecionar colunas finais estruturadas para o Banco de Dados
    colunas_finais = [
        "id_paciente",
        "nome_anonimo",
        "gravidade",
        "unidade_origem",
        "procedimento_solicitado",
        "data_solicitacao"
    ]
    
    # Garantir que as colunas existam no dataframe
    colunas_presentes = [col for col in colunas_finais if col in df.columns]
    df_final = df[colunas_presentes]
    
    logger.info(f"Transformação concluída. Total de registros prontos: {len(df_final)}")
    return df_final


def obter_conexao_banco(db_url: str) -> Engine:
    """
    Cria a engine de conexão do SQLAlchemy.
    """
    url_formatada = ajustar_url_sqlalchemy(db_url)
    try:
        engine = create_engine(url_formatada)
        return engine
    except Exception as e:
        logger.error(f"Falha ao criar a engine de conexão com o banco de dados: {e}")
        raise


def carregar_no_banco(df: pd.DataFrame, engine: Engine):
    """
    Garante a estrutura da tabela no PostgreSQL e insere (append) os dados transformados.
    """
    comando_criar_tabela = """
    CREATE TABLE IF NOT EXISTS public.fila_regulacao (
        id bigserial PRIMARY KEY,
        id_paciente integer NOT NULL,
        nome_anonimo text,
        gravidade text,
        procedimento_solicitado text,
        unidade_origem text,
        data_solicitacao timestamp without time zone
    );
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(comando_criar_tabela))
            logger.info("Tabela 'fila_regulacao' verificada/criada com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao verificar/criar a tabela no banco de dados: {e}")
        raise

    logger.info("Inciando inserção de dados na tabela 'fila_regulacao'...")
    try:
        # Inserir dados em lotes (multi) para maior performance e appending aos existentes.
        inseridos = df.to_sql("fila_regulacao", con=engine, if_exists="append", index=False, method="multi")
        logger.info(f"Sucesso! {len(df)} registros foram inseridos no banco de dados.")
    except Exception as e:
        logger.error(f"Falha crítica durante a inserção de dados (to_sql): {e}")
        raise


def executar_pipeline_etl():
    """
    Fluxo principal que orquestra a Extração, Transformação e Carga (ETL).
    """
    logger.info("--- Iniciando o Processo ETL Principal ---")
    
    load_dotenv()
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    
    if not db_url:
        logger.critical("Variável de ambiente SUPABASE_DB_URL não localizada! Verifique seu arquivo .env.")
        return

    caminho_csv = Path(__file__).resolve().parent.parent / "data" / "dados_regulacao.csv"
    
    if not caminho_csv.exists():
        logger.error(f"Arquivo de origem de dados não encontrado no caminho esperado: {caminho_csv}")
        return

    try:
        # Puxar e tratar dados do CSV
        dataframe = extrair_e_transformar(caminho_csv)
        
        # Iniciar conexão com o DB
        engine = obter_conexao_banco(db_url)
        
        # Enviar (Load) para nuvem/DB Local
        carregar_no_banco(dataframe, engine)
        
        logger.info("--- Processo ETL finalizado com Sucesso! ---")

    except Exception as erro_geral:
        logger.error(f"⚠️ O Processo ETL falhou devido a um erro inesperado: {erro_geral}")


if __name__ == "__main__":
    executar_pipeline_etl()
