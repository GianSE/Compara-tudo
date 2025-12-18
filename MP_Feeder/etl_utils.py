# etl_utils.py
import pandas as pd
import os
import logging
from datetime import datetime

def setup_logging():
    """
    Configura o logging para salvar em um arquivo com data/hora.
    """
    log_dir = "logs/"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    agora = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging.basicConfig(
        filename=os.path.join(log_dir, f"requests-{agora}.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

# ============================================
# SEÇÃO DE GERENCIAMENTO DE ESTADO (ÍNDICE)
# ============================================

def recuperar_ultimo_indice(arquivo_indice):
    """
    Lê o arquivo de texto e recupera o último índice salvo.
    """
    try:
        with open(arquivo_indice, "r") as f:
            ultimo_indice = int(f.read().strip())
            print(f"##### ÚLTIMO ÍNDICE RECUPERADO: {ultimo_indice} #####")
    except FileNotFoundError:
        ultimo_indice = 0
    return ultimo_indice
    
def finalizar_indice(arquivo_indice):
    """
    Remove o arquivo de índice em caso de uma execução bem-sucedida.
    """
    if os.path.exists(arquivo_indice):
        os.remove(arquivo_indice)
        print(f"##### ARQUIVO DE ÍNDICE {arquivo_indice} REMOVIDO COM SUCESSO #####")

# ============================================
# SEÇÃO DE LÓGICA DE NEGÓCIO E TRANSFORMAÇÃO
# ============================================

def transformar_dados_produtos(produtos_por_valor, produtos_por_qtd):
    """
    (ETL - Transform) Agora apenas limpa os dados sem forçar 14 dígitos.
    """
    print("3. Processando lista de produtos...")
    logging.info("3. Processando lista de produtos")
    
    Produtos = pd.merge(produtos_por_qtd, produtos_por_valor, how="inner")
    
    # --- AJUSTE: Removemos o .str.zfill(14) para manter o GTIN original ---
    Produtos["GTIN"] = Produtos["GTIN"].astype(str)
    
    # Dededuplica para garantir integridade
    Produtos.drop_duplicates(subset=['GTIN'], keep='first', inplace=True)
    
    print(f"Transformação concluída. Total: {len(Produtos)} produtos.")
    return Produtos

def gerar_consultas(Geohashs, EANs):
    """
    Gera o produto cartesiano entre os 54 GTINs e os 2 Geohashs.
    """
    print("##### CALCULANDO CONSULTAS (Londrina/Cornélio x 54 GTINs) #####")
    
    if EANs.empty or Geohashs.empty:
        logging.warning("Lista de EANs ou Geohashs vazia.")
        return pd.DataFrame(columns=["gtin", "geohash", "index"])

    EANs = EANs.copy()
    EANs.loc[:, "key"] = 1
    Geohashs = Geohashs.copy()
    Geohashs.loc[:, "key"] = 1

    consultas = pd.merge(
        EANs,
        Geohashs,
        on="key",
    ).drop("key", axis=1)
    
    consultas.reset_index(drop=True, inplace=True)
    consultas["index"] = consultas.index
    
    print(f"##### {len(consultas)} CONSULTAS GERADAS NO TOTAL #####")
    return consultas