# ETL de Hospitais e Dashboard de Regulação 🏥📈

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)

Um projeto completo de Automação de Dados (ETL) acoplado a um Dashboard Analítico para gestão logística de filas de saúde pública no Rio de Janeiro. 

Este repositório foi construído para demonstrar boas práticas de Engenharia de Dados, garantindo segurança (LGPD), performance e visualização iterativa de dados complexos através do Pandas e do Streamlit. O destino dos dados é processado diretamente na nuvem utilizando o Supabase (PostgreSQL).

## 🚀 Funcionalidades

- **Extração e Carga Inteligente:** Lê os dados de Origem (CSV BRUTOS) ou geradores Fake Automáticos, limpa, cruza com bancos locais e joga as métricas diretamente no Postgres na Nuvem.
- **Conformidade LGPD:** Possui módulos nativos (no processo de transformação) para assegurar o Anonimato do Paciente através das rubricas iniciais.
- **Padronização Geográfica (Geo Mapping):** Trata unidades do SUS em Latitude / Longitude seguras convertendo campos mal formatados para renderização do Mapa Carto-Dark do Streamlit.
- **Logs Escaláveis:** Todos os fluxos em Python utilizam o `logging` padrão (ao invés de prints primitivos), pronto para debugs robustos.
- **Relatórios de Risco em Tempo Real:** Dashboard Interativo que segmenta os casos por *Protocolo de Manchester* e por Coordenações de Área Planejada (CAP).

## 🗂 Estrutura do Projeto

```bash
📦 ETL-de-Hospitais-e-Dashboard-Regulacao
 ┣ 📂 data/                     # Dados Brutos ou Refinados (Inclusos no .gitignore)
 ┣ 📂 scripts/
 ┃ ┣ 📜 main.py                 # Orquestrador do ETL Master (Limpa e manda pro Banco)
 ┃ ┣ 📜 transform_unidades.py   # Refina e padroniza a Tabela Meste de Hospitais/UPS
 ┃ ┗ 📜 generate_fake_data.py   # Gerador massivo de filas com integridade Relacional
 ┣ 📜 app.py                    # A Interface Central (Dashboard desenvolvido com Streamlit)
 ┣ 📜 requirements.txt          # Suíte de Dependências Locais
 ┣ 📜 .env.example              # Exemplo da Chave de Segurança Oculta
 ┗ 📜 README.md                 # Este documento
```

## 🛠️ Como Instalar e Rodar

### 1. Requisitos Prévios 
- Python 3.9+ Instalado no Path
- Pip funcional.

### 2. Configure seu Ambiente
Clone o repositório na sua máquina e crie o interpretador base (Virtual Env):
```powershell
git clone https://github.com/J-Oliveira-S/ETL-de-Hospitais-e-Dashboard-Regula-o
cd ETL-de-Hospitais-e-Dashboard-Regula-o
python -m venv .venv

# Ativando no Windows Powershell:
.\.venv\Scripts\Activate.ps1
```

### 3. Instale os Pacotes
```powershell
pip install -r requirements.txt
```

### 4. Configure o Banco de Dados (Ambiente)
NUNCA salve dados bancários dentro de um repositório git. Para nossa aplicação rodar você criará na raiz o arquivo `.env`:

1. Copie o arquivo providenciado `.env.example`.
2. Renomeie o novo arquivo EXCLUSIVAMENTE para `.env`.
3. Preencha os campos copiando a connection string da plataforma de banco (Supabase / Neon / Local PgAdmin).

> **⚠️ Atenção: Conexão Supabase (IPv4 vs IPv6)**
> O Supabase agora usa **IPv6** por padrão na porta `5432`. Se a sua rede de internet for **IPv4** (a maioria no Brasil), você obrigatoriamente precisa usar o **Connection Pooler** (porta `6543`).
> 
> Quando usar a porta **6543**, o seu usuário do banco deixa de ser apenas `postgres` e passa a ser `postgres.[seu-project-ref]`.
> 
> **Exemplo de URL Correta para IPv4:**
```env
SUPABASE_DB_URL=postgresql://postgres.ab12cd34ef56:MINHA_SENHA@aws-0-sa-east-1.pooler.supabase.com:6543/postgres?sslmode=require
```

## 📈 Como Executar a Solução

Dado o Ambiente já conectado e preenchido, siga os comandos em sequência:

**A. Carregue as unidades de Mestre no banco de Dados (Geolocalização Base):**
Este passo fará uma varredura nas planilhas matrizes de Unidade de Saúde (SUS):
```powershell
python scripts/transform_unidades.py
```

**B. Gere o Movimento (A Fila de Regulação e Teste de Carga)**
Uma vez que o banco reconhece as unidades cadastradas, podemos lançar centenas de ocorrências falsas nela pra forçar o sistema:
```powershell
python scripts/generate_fake_data.py
```

**(Opcional) C. O Orquestrador Geral:**
Se quiser validar se sua listagem mestre antiga consegue ir ao banco corretamente via Pipeline ETL Oficial:
```powershell
python scripts/main.py
```

**D. Abra a Interface Visual de Regulação (O DASHBOARD)**
No mesmo terminal que está com as variáveis ligadas, digite a porta local interativa da Web:
```powershell
streamlit run app.py
```
*O sistema web vai compilar instantaneamente o fluxo SQL com o banco no endereço nativo (Ex: http://localhost:8501).*

---
💼 **Desenvolvido com o intuito acadêmico técnico de Engenharia e Controle de Fluxos.** Se esse portfólio te ajudou a visualizar operações em larga escala do SUS com Python, fique à vontade para testar as features localmente.