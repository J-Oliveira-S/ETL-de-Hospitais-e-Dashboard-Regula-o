#!/usr/bin/env python3
"""
ETL de exemplo para limpeza de lista de regulação hospitalar e carga no Supabase (Postgres).

Passos:
- Lê `data/dados_regulacao.csv`
- Remove duplicatas
- Anonimiza o nome do paciente
- Converte `data_solicitacao` para datetime
- Gera a tabela (se não existir) e insere os registros no Postgres (Supabase)

Configurar via `.env` com `SUPABASE_DB_URL`.
"""
from pathlib import Path
import os
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def anonymize_name(name: str, method: str = "initials") -> str:
    if pd.isna(name) or name is None:
        return None
    name = name.strip()
    if method == "hash":
        return hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
    # initials (e.g., "J. S.")
    parts = name.split()
    if len(parts) == 0:
        return ""
    if len(parts) == 1:
        return parts[0][0].upper() + "."
    first = parts[0][0].upper()
    last = parts[-1][0].upper()
    return f"{first}. {last}."


def ensure_sqlalchemy_url(db_url: str) -> str:
    # Supabase/DATABASE_URL sometimes starts with 'postgres://' — SQLAlchemy prefers 'postgresql+psycopg2://'
    if db_url.startswith("postgres://"):
        return db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return db_url


def run_etl(csv_path: Path, anonymize_method: str = "initials"):
    # Load data
    df = pd.read_csv(csv_path)

    # Parse datetime
    df["data_solicitacao"] = pd.to_datetime(df["data_solicitacao"], errors="coerce")

    # Remove exact duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"Removidas {before - after} duplicatas (linhas idênticas).")

    # Anonimizar
    df["nome_anonimo"] = df["nome_paciente"].apply(lambda s: anonymize_name(s, method=anonymize_method))

    # Selecionar/renomear colunas para inserir
    df = df[["id_paciente", "nome_anonimo", "gravidade", "procedimento_solicitado", "unidade_origem", "data_solicitacao"]]

    # Conexão com Supabase (Postgres)
    load_dotenv()
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Variável de ambiente SUPABASE_DB_URL ou DATABASE_URL não encontrada. Use .env ou defina no ambiente.")
    db_url = ensure_sqlalchemy_url(db_url)

    engine = create_engine(db_url)

    # Criar tabela se não existir
    create_table_sql = """
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

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
        print("Tabela 'fila_regulacao' garantida no banco (IF NOT EXISTS).")

    # Inserir dados (append)
    try:
        df.to_sql("fila_regulacao", con=engine, if_exists="append", index=False, method="multi")
        print(f"Inseridos {len(df)} registros em 'fila_regulacao'.")
    except Exception as e:
        print("Erro ao inserir dados:", e)
        raise


def main():
    base = Path(__file__).resolve().parents[1]
    csv_path = base / "data" / "dados_regulacao.csv"
    if not csv_path.exists():
        print(f"Arquivo CSV não encontrado: {csv_path}")
        return
    run_etl(csv_path, anonymize_method="initials")


if __name__ == "__main__":
    main()
