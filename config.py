import os

# CONFIGURAÇÕES DO BANCO DE DADOS
# O os.getenv tenta ler do GitHub Actions. Se não encontrar, usa os seus dados locais.
DB_CONFIG = {
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "357159dodo"),
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "database": os.getenv("DB_NAME", "dbprojetos")
}

# CONFIGURAÇÕES DO TELEGRAM
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8096205039:AAGz3TqmfyXGI__NGdyvf6TnMDNA--pvAWc")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "7035974555")

# TOKEN DO MERCADO LIVRE (Caso ainda use)
MERCADO_LIVRE_ACCESS_TOKEN = os.getenv("ML_ACCESS_TOKEN", "SEU_ACCESS_TOKEN_DO_ML_AQUI")

# CAMINHOS DE ARQUIVOS (Estado do script)
# Se estiver no Windows, usa o caminho local. Se estiver no Linux (GitHub), usa o atual.
arquivo_indice = "ultimo_indice.txt"
arquivo_notas_parciais = "notas_parciais.csv"
arquivo_lojas_parciais = "lojas_parciais.csv"

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImdpYW5wZWRyb2RldkBnbWFpbC5jb20iLCJtZFJlZ2lvbiI6ImF3cy11cy1lYXN0LTEiLCJzZXNzaW9uIjoiZ2lhbnBlZHJvZGV2LmdtYWlsLmNvbSIsInBhdCI6InZpVFJRdUVRZjRKNG8tYmZxSUhkc1JXcGpOVXQ0OVIwZVpHazJ6YjlaWm8iLCJ1c2VySWQiOiIwZGQzYjI3MS0zN2FhLTRlYjUtOTYzNi0wMDFlZjgzNWQxMjciLCJpc3MiOiJtZF9wYXQiLCJyZWFkT25seSI6ZmFsc2UsInRva2VuVHlwZSI6InJlYWRfd3JpdGUiLCJpYXQiOjE3Njc2NzU3OTR9.oD1y-N8uxYt9zXCL58hwNqmR7_E7VIlwnHhXidWbrlo")