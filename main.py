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
from pathlib import Path
import os
import pandas as pd
import sqlalchemy as sqla
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def initials(name: str) -> str:
    if pd.isna(name) or name is None:
        return None
    parts = str(name).strip().split()
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0][0].upper() + "."
    return f"{parts[0][0].upper()}. {parts[-1][0].upper()}."


def ensure_sqlalchemy_url(db_url: str) -> str:
    if db_url.startswith("postgres://"):
        return db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return db_url


def load_and_transform(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # Converter data
    df["data_solicitacao"] = pd.to_datetime(df["data_solicitacao"], errors="coerce")

    # Remover duplicatas completas
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"Removidas {before - after} duplicatas (linhas idênticas).")

    # Anonimizar
    df["nome_anonimo"] = df["nome_paciente"].apply(initials)

    # Selecionar colunas finais para inserção
    df = df[["id_paciente", "nome_anonimo", "gravidade", "unidade_origem", "data_solicitacao"]]
    return df


def insert_into_db(df: pd.DataFrame, db_url: str):
    db_url = ensure_sqlalchemy_url(db_url)
    engine = create_engine(db_url)

    # Certificar a criação da tabela (caso queira executar via script SQL separado, isso é redundante)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.fila_regulacao (
        id bigserial PRIMARY KEY,
        id_paciente integer NOT NULL,
        nome_anonimo text,
        gravidade text,
        unidade_origem text,
        data_solicitacao timestamp without time zone
    );
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(create_table_sql))
            print("Tabela 'fila_regulacao' verificada/criada (IF NOT EXISTS).")

            # Inserir dados usando a conexão ativa (transaction)
            df.to_sql("fila_regulacao", con=conn, if_exists="append", index=False, method="multi")
            print(f"Inseridos {len(df)} registros em 'fila_regulacao'.")
    except sqla.exc.OperationalError as e:
        print("\n[ERRO DE CONEXÃO]")
        print("Falha ao conectar no banco de dados. Se você está usando o Supabase, certifique-se de:")
        print("1. Usar a porta 6543 (Pooler Mode) ao invés de 5432 se a sua rede for IPv4.")
        print("2. O seu usuário deve ser no formato: postgres.[seu-project-ref] (para a porta 6543).")
        print("3. A URL deve terminar com ?sslmode=require ou pgbouncer=true.")
        print(f"\nDetalhes técnicos: {e}")
    except Exception as e:
        print(f"\n[ERRO] Ocorreu um problema ao inserir no banco: {e}")


def main():
    load_dotenv()
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        print("Variável SUPABASE_DB_URL não encontrada. Preencha o arquivo .env ou exporte a variável.")
        return

    csv_path = Path(__file__).resolve().parent / "dados_regulacao.csv"
    if not csv_path.exists():
        print(f"Arquivo CSV não encontrado: {csv_path}")
        return

    df = load_and_transform(csv_path)
    insert_into_db(df, db_url)


if __name__ == "__main__":
    main()
