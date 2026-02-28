import pandas as pd
import random
import os
import logging
from datetime import datetime, timedelta
from typing import List

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- CONFIGURAÇÃO DE LOGS ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)
logger = logging.getLogger("Criador_Dados_Falsos")

def obter_engine() -> create_engine:
    """Carrega as variáveis e disponibiliza a conexão com o PostgreSQL"""
    load_dotenv()
    db_url = os.getenv('SUPABASE_DB_URL') or os.getenv('DATABASE_URL')
    
    if not db_url:
        logger.critical("Variável de ambiente SUPABASE_DB_URL não encontrada. Certifique-se de configurar o arquivo .env.")
        raise ValueError("URL de banco de dados inexistente.")

    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql+psycopg2://', 1)
    
    try:
        engine = create_engine(db_url)
        return engine
    except Exception as erro_conexao:
         logger.critical(f"Falha na formatação com a API SQLAlchemy: {erro_conexao}")
         raise

def gerar_fila_ficticia(n_registros: int = 350) -> None:
    """
    Função útil para Demonstração e Portfolio:
    Gera dados sintéticos (fakes) de pacientes integrados com as Unidades de Saúde Reais cadastradas no banco.
    """
    logger.info("Buscando lista de unidades válidas para o cruzamento Relacional (JOINs)...")
    
    try:
        engine = obter_engine()
    except ValueError as val_error:
        return

    # Busca as descrições literais para inserção em fila_regulacao
    query_unidades = text("SELECT nome_unidade FROM public.unidades_saude")
    try:
        with engine.connect() as conn:
            unidades_df = pd.read_sql(query_unidades, conn)
            unidades_banco: List[str] = unidades_df['nome_unidade'].dropna().tolist()
    except Exception as e:
         logger.error(f"Falha ao checar Unidades de Saúde no banco no momento do mapeamento: {e}")
         return

    if not unidades_banco:
        logger.error("❌ Nenhuma unidade mestre cadastrada. Por Favor, rode o script `transform_unidades.py` antes dessa automação.")
        return

    logger.info(f"Produzindo cerca de {n_registros} ocorrências médicas fictícias...")
    
    lista_gravidades = ['Verde', 'Amarelo', 'Laranja', 'Vermelho']
    lista_procedimentos = [
        'Tomografia de Tórax', 'Internação Clínica', 'Vaga de UTI Adulto', 
        'Parecer Cardiologia', 'Ecocardiograma', 'Cirurgia Geral', 
        'Internação Pediátrica', 'Transferência para Especialidade', 'Resolução de Fratura (Ortopedia/Trauma)'
    ]
    
    dados_compilados = []
    # Usaremos um pacote fixo de Letras do Alfabeto
    alfabeto = 'ABCDEFGHIJKLMNOPRSTUVZ'
    for _ in range(n_registros):
        # Gera nome com iniciais aleatórias - mantendo LGPD Fake (Ex: A.J.P)
        init = f"{random.choice(alfabeto)}.{random.choice(alfabeto)}."
        
        # Sorteia uma ocorrência de até 5 dias inteiros passados.
        data_aleatoria = datetime.now() - timedelta(days=random.randint(0, 5), hours=random.randint(0, 23))
        
        dados_compilados.append({
            "id_paciente": random.randint(10000, 99999),
            "nome_anonimo": init,
            "gravidade": random.choice(lista_gravidades),
            "procedimento_solicitado": random.choice(lista_procedimentos),
            "unidade_origem": random.choice(unidades_banco), # Match Exato para o Inner Join local Funcionar.
            "data_solicitacao": data_aleatoria
        })

    df_fake = pd.DataFrame(dados_compilados)

    logger.info("Sintetizador: Submetendo a Fila nova ao Serviço de Banco de Dados...")
    try:
         with engine.begin() as conn:
             # TRUNCATE esvazia a fila para demonstração ficar Clean.
             conn.execute(text("TRUNCATE TABLE fila_regulacao RESTART IDENTITY;"))
             # Executa o Pandas Loader Multiplo
             df_fake.to_sql('fila_regulacao', conn, if_exists='append', index=False, method='multi')
             
         logger.info(f"🚀 FEITO! {n_registros} pacientes (Simulação) adicionados com total precisão geográfica no Supabase.")
    except Exception as e:
         logger.error(f"Erro Crítico durante o Envio de pacotes ao Supabase PostgreSQL: {e}")


if __name__ == "__main__":
    gerar_fila_ficticia()
