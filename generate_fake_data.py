import pandas as pd
import random
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. Configurações Iniciais
load_dotenv()
db_url = os.getenv('SUPABASE_DB_URL')
if db_url and db_url.startswith('postgres://'):
    db_url = db_url.replace('postgres://', 'postgresql+psycopg2://', 1)

engine = create_engine(db_url)

def generate_fake_fila(n_registros=350):
    print("Buscando lista oficial de unidades no banco...")
    # Buscamos os nomes exatos para o JOIN não falhar
    query_unidades = text("SELECT nome_unidade FROM unidades_saude")
    with engine.connect() as conn:
        unidades = pd.read_sql(query_unidades, conn)['nome_unidade'].tolist()

    if not unidades:
        print("❌ Erro: Nenhuma unidade encontrada no banco. Rode o transform_unidades.py primeiro.")
        return

    print(f"Gerando {n_registros} pacientes fictícios...")
    
    gravidades = ['Verde', 'Amarelo', 'Laranja', 'Vermelho']
    procedimentos = [
        'Tomografia de Tórax', 'Internação Clínica', 'Vaga de UTI Adulto', 
        'Parecer Cardiologia', 'Ecocardiograma', 'Cirurgia Geral', 
        'Internação Pediátrica', 'Transferência para Especialidade'
    ]
    
    dados = []
    for i in range(n_registros):
        # Gerando iniciais aleatórias (ex: J.M.S)
        nome = f"{random.choice('ABCDEFGHIJKLMNOPRSTUV')}.{random.choice('ABCDEFGHIJKLMNOPRSTUV')}."
        
        # Data aleatória nos últimos 5 dias
        data_aleatoria = datetime.now() - timedelta(days=random.randint(0, 5), hours=random.randint(0, 23))
        
        dados.append({
            "id_paciente": random.randint(10000, 99999),
            "nome_anonimo": nome,
            "gravidade": random.choice(gravidades),
            "procedimento_solicitado": random.choice(procedimentos),
            "unidade_origem": random.choice(unidades), # Casamento perfeito com a tabela unidades_saude
            "data_solicitacao": data_aleatoria
        })

    df_fake = pd.DataFrame(dados)

    print("Limpando fila antiga e inserindo novos dados...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fila_regulacao RESTART IDENTITY;"))
        df_fake.to_sql('fila_regulacao', conn, if_exists='append', index=False, method='multi')

    print(f"🚀 SUCESSO! {n_registros} pacientes inseridos na regulação.")

if __name__ == "__main__":
    generate_fake_fila()