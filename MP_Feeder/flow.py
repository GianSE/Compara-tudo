# flow.py
import os
import logging
import pandas as pd

# Importa as ferramentas de cada módulo
from MP_Feeder.db_manager import (
    pegar_ultima_att_gtins, pegar_geohashs_BD,
    coletar_produtos_no_banco, coletar_lojas_do_banco,
    inserir_lojas_sc, inserir_notas
)
from MP_Feeder.api_services import buscar_notas, buscar_lat_lon_lojas_sc_nominatim
from MP_Feeder.etl_utils import (
    recuperar_ultimo_indice, gerar_consultas
)

def run_recovery_flow(configs, now_gmt3):
    """
    Executa a lógica de recuperação de dados parciais (CSV).
    """
    print("##### ⚠️ DADOS PARCIAIS .CSV ENCONTRADOS. TENTANDO CARREGAR... #####")
    logging.warning("##### DADOS PARCIAIS .CSV ENCONTRADOS. TENTANDO CARREGAR... #####")
    
    DB_CONFIG = configs['DB_CONFIG']
    arquivo_indice = configs['arquivo_indice']
    arquivo_notas_parciais = configs['arquivo_notas_parciais']
    arquivo_lojas_parciais = configs['arquivo_lojas_parciais']
    
    Notas_csv = pd.read_csv(arquivo_notas_parciais)
    Lojas_csv = pd.DataFrame()
    if os.path.exists(arquivo_lojas_parciais):
        Lojas_csv = pd.read_csv(arquivo_lojas_parciais)
    
    ultimo_indice = recuperar_ultimo_indice(arquivo_indice)
    
    print("##### Verificando lojas já cadastradas no banco... #####")
    Lojas_no_banco_df = coletar_lojas_do_banco(DB_CONFIG)
    lojas_ja_cadastradas_set = set(Lojas_no_banco_df["id_loja"])
    
    if not Lojas_csv.empty:
        Lojas_csv_sem_duplicatas = Lojas_csv.drop_duplicates(subset=["id_loja"]).reset_index(drop=True)
        lojas_para_buscar_latlon = Lojas_csv_sem_duplicatas[
            ~Lojas_csv_sem_duplicatas['id_loja'].isin(lojas_ja_cadastradas_set)
        ]
        
        if not lojas_para_buscar_latlon.empty:
            print(f"##### 💾 RECUPERANDO {len(lojas_para_buscar_latlon)} LOJAS DO CSV... #####")
            Lojas_SC_com_latlon = buscar_lat_lon_lojas_sc_nominatim(lojas_para_buscar_latlon)
            inserir_lojas_sc(Lojas_SC_com_latlon, now_gmt3, DB_CONFIG)
    
    if not Notas_csv.empty:
        Notas_csv_sem_duplicatas = Notas_csv.drop_duplicates(subset=["id_nota"]).reset_index(drop=True)
        print(f"##### 💾 RECUPERANDO {len(Notas_csv_sem_duplicatas)} NOTAS DO CSV... #####")
        inserir_notas(Notas_csv_sem_duplicatas, now_gmt3, DB_CONFIG)
    
    print("##### SUCESSO AO SALVAR DADOS DO CSV. LIMPANDO ARQUIVOS. #####")
    os.remove(arquivo_notas_parciais)
    if os.path.exists(arquivo_lojas_parciais):
        os.remove(arquivo_lojas_parciais)
    
    return ultimo_indice

def run_normal_flow(configs, now_gmt3, today_gmt3):
    """
    Fluxo otimizado para 54 produtos e Geohashs fixos.
    """
    print("##### NENHUM DADO PARCIAL ENCONTRADO. INICIANDO COLETA NORMAL... #####")
    
    DB_CONFIG = configs['DB_CONFIG']
    TELEGRAM_TOKEN = configs['TELEGRAM_TOKEN']
    TELEGRAM_CHAT_ID = configs['TELEGRAM_CHAT_ID']
    arquivo_indice = configs['arquivo_indice']

    print("##### USANDO LISTA FIXA DE 54 PRODUTOS #####")
    
    # 1. Coleta os Geohashs fixos (Londrina/Cornélio) definidos no db_manager
    Geohashs = pegar_geohashs_BD(DB_CONFIG) 
    
    # 2. Coleta os 54 GTINs da tabela bronze_bluesoft_produtos
    EANs = coletar_produtos_no_banco(DB_CONFIG) 
    
    # 3. Gera as consultas (Geohash x GTIN)
    # Com 2 geohashs e 54 produtos, serão apenas 108 consultas.
    Consultas = gerar_consultas(Geohashs, EANs)
    
    # 4. Busca lojas conhecidas para evitar geocodificação repetida
    Lojas = coletar_lojas_do_banco(DB_CONFIG) 

    # 5. Recupera o índice caso tenha havido erro na última execução
    ultimo_indice = recuperar_ultimo_indice(arquivo_indice)

    # 6. Coleta de Notas na API
    Notas_geral, Lojas_SC_geral, run_completo, indice_para_salvar = buscar_notas(
        Consultas, Lojas, ultimo_indice, arquivo_indice,
        TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
    )
    
    return Notas_geral, Lojas_SC_geral, run_completo, indice_para_salvar