# main.py
import os
import logging
from datetime import datetime, timezone, timedelta
import pandas as pd

# Importa as configs
from config import DB_CONFIG, GOOGLE_API_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

# Importa os módulos de execução
from flow import run_recovery_flow, run_normal_flow
from error_handler import handle_execution_error, handle_api_fail, handle_success
from etl_utils import setup_logging

def main():
    # --- CONFIGURAÇÕES DE EXECUÇÃO ---
    gmt_menos_3 = timezone(timedelta(hours=-3))
    now_gmt3 = datetime.now(gmt_menos_3)
    today_gmt3 = now_gmt3.date()
    
    # Agrupa todas as configs em um dicionário para passar para as funções
    configs = {
        "DB_CONFIG": DB_CONFIG,
        "GOOGLE_API_KEY": GOOGLE_API_KEY,
        "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
        "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
        "arquivo_indice": "ultimo_indice.txt",
        "arquivo_notas_parciais": "notas_parciais.csv",
        "arquivo_lojas_parciais": "lojas_parciais.csv"
    }
    
    # --- LOOP PRINCIPAL DE EXECUÇÃO ---
    while True:
        Notas_geral = pd.DataFrame()
        Lojas_SC_geral = pd.DataFrame()
        run_completo = False
        indice_para_salvar = 0

        try:
            # --- PASSO 1: VERIFICAR/EXECUTAR RECUPERAÇÃO DE CSV ---
            if os.path.exists(configs['arquivo_notas_parciais']):
                # Delega a lógica de recuperação para o 'flow.py'
                run_recovery_flow(configs, now_gmt3)
                
                print("##### RECUPERAÇÃO CONCLUÍDA. CONTINUANDO PARA A COLETA DA API... #####")
                print("\n" + "="*50 + "\n")
                continue # Volta ao início do 'while' para rodar o fluxo normal
            
            # --- PASSO 2: EXECUTAR FLUXO NORMAL ---
            else:
                # Delega o fluxo normal para o 'flow.py'
                Notas_geral, Lojas_SC_geral, run_completo, indice_para_salvar = run_normal_flow(
                    configs, now_gmt3, today_gmt3
                )
            
            # --- PASSO 3: LIDAR COM O RESULTADO ---
            if run_completo:
                # Delega a lógica de sucesso
                handle_success(
                    configs['arquivo_indice'], now_gmt3, 
                    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                )
                break # Sucesso, sai do 'while True'
            else:
                # Falha parcial da API (circuit breaker), salva o índice e reinicia
                # Delega a lógica de falha de API
                handle_api_fail(indice_para_salvar)
                # O 'while True' vai rodar de novo e recomeçar do índice salvo

        except Exception as e:
            # --- PASSO 4: LIDAR COM FALHA CATASTRÓFICA ---
            # Delega o tratamento de erro e backup para o 'error_handler.py'
            handle_execution_error(
                e, Notas_geral, Lojas_SC_geral, indice_para_salvar,
                now_gmt3, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
            )

if __name__ == "__main__":
    setup_logging() # Configura o logging ANTES de tudo
    main()