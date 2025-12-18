# Projeto ETL - Lista de Regulação Hospitalar

Este repositório contém um pequeno projeto ETL (Extração, Transformação e Carga) que:

- Lê `data/dados_regulacao.csv` contendo uma lista de regulação hospitalar (exemplo fictício).
- Remove duplicatas e anonimiza nomes (por questões de LGPD).
- Converte datas e envia os registros limpos para uma tabela Postgres no Supabase.

**Conteúdo criado**

 - `scripts/etl.py` - Script Python anterior de exemplo (mantido).
 - `scripts/main.py` - Script principal solicitado para rodar o ETL (leitura CSV -> limpeza -> anonimização -> carga SQLAlchemy).
- `sql/create_table.sql` - SQL para criar a tabela `fila_regulacao` no Supabase.
- `requirements.txt` - dependências Python.
- `.env.example` - exemplo de variável de ambiente para conexão.
- Configure a conexão no `.env` (preencha o arquivo `.env` criado) ou exporte `SUPABASE_DB_URL`.
-
- Exemplo de valor (não coloque a chave pública aqui em chats):
```
SUPABASE_DB_URL=postgresql://usuario:senha@seu-host:6543/postgres?sslmode=verify-full
```

## Instalação rápida (Windows PowerShell)

```powershell
- Execute o ETL (script principal `main.py`):

```powershell
python .\scripts\main.py
```

Se preferir testar sem inserir no banco, você pode executar apenas a transformação em um REPL:

```powershell
python -c "from scripts.main import load_and_transform; import pathlib; print(load_and_transform(pathlib.Path('data/dados_regulacao.csv')).head())"
```
## Configuração

1. Copie `.env.example` para `.env` e coloque sua URL do Supabase (ou exporte a variável `SUPABASE_DB_URL`).

## Rodando o ETL

No PowerShell, com o ambiente ativado e a variável `SUPABASE_DB_URL` configurada:

```powershell
python .\scripts\etl.py
```

O script executa os seguintes passos:

- Garante que a tabela `fila_regulacao` exista (com `CREATE TABLE IF NOT EXISTS`).
- Insere os registros limpos e anonimados.

## Observações de LGPD

- O script implementa uma função de anonimização (`anonymize_name`) que por padrão gera iniciais (ex.: `J. S.`).
- Se preferir um hash irreversível, altere `anonymize_method` para `hash` no `scripts/etl.py`.

## Próximos passos sugeridos

- Adicionar validação/normalização adicional (ex.: checar formatos de procedimento, unidade).
- Implementar testes unitários para a transformação.
- Automatizar via agendador (cron / Cloud Functions) para execução periódica.
