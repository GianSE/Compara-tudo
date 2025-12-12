# setup_ml_products.py (Código corrigido)

import sys
import os
import requests
import pandas as pd
from datetime import datetime
import logging

# Configura o path para encontrar os módulos
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# IMPORTAÇÕES CORRIGIDAS: Agora importa o token do ML
from config import DB_CONFIG, MERCADO_LIVRE_ACCESS_TOKEN 
from MP_Feeder.db_manager import insert_produtos_manuais
from MP_Feeder.etl_utils import setup_logging

def get_ml_trends(limit: int = 50) -> list:
    """
    Busca as buscas mais desejadas na API de Tendências do Mercado Livre (MLB - Brasil).
    """
    url = "https://api.mercadolibre.com/orders/search" 
    
    # --- CORREÇÃO AQUI: Adiciona o cabeçalho de autenticação ---
    headers = {
        "Authorization": f"Bearer {MERCADO_LIVRE_ACCESS_TOKEN}",
        "User-Agent": "ComparaTudoApp/1.0 (contato@compara.tudo)" # Boas práticas de API
    }
    
    try:
        # Passa o cabeçalho na requisição
        response = requests.get(url, headers=headers, timeout=10) 
        response.raise_for_status() 
        data = response.json()
        
        produtos_ml = []
        
        for item in data:
            keyword = item.get("keyword")
            if keyword:
                produtos_ml.append({
                    # O GTIN fictício é usado para popular a tabela.
                    "gtin": f"ML-{keyword[:10]}".zfill(14), 
                    "descricao": keyword.upper(),
                    "tipo": "TREND_ML",
                    "fabricante": "NA_ML"
                })

        return produtos_ml[:limit]
        
    except requests.exceptions.RequestException as e:
        # Se a falha for 403, a mensagem será mais informativa
        print(f"❌ Erro ao buscar Tendências do Mercado Livre: {e}")
        logging.error(f"Erro ao buscar Tendências do Mercado Livre: {e}", exc_info=True)
        return []

def main_setup_ml():
    setup_logging()
    
    if not MERCADO_LIVRE_ACCESS_TOKEN or MERCADO_LIVRE_ACCESS_TOKEN == "SEU_ACCESS_TOKEN_DO_ML_AQUI":
        print("❌ ERRO CRÍTICO: MERCADO_LIVRE_ACCESS_TOKEN não está configurado. Configure em config.py.")
        return

    print("Iniciando injeção de produtos populares do Mercado Livre...")
    
    produtos_para_inserir = get_ml_trends(limit=50) 
    
    if not produtos_para_inserir:
        print("❌ Não foi possível obter a lista de produtos populares do Mercado Livre. Verifique o token e permissões.")
        return

    insert_produtos_manuais(DB_CONFIG, produtos_para_inserir)
    print("Execução de setup concluída. O pipeline principal pode começar a rodar.")

if __name__ == "__main__":
    main_setup_ml()