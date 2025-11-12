# tests/test_api_menorpreco.py

import requests
import json
import time

# --- DADOS DE TESTE ---
# Vamos usar um GTIN comum (ex: Coca-Cola) e um Geohash de uma cidade grande
# (ex: Londrina) para ter uma alta chance de encontrar resultados.

# Geohash para o centro de Londrina, PR
GEOHASH_LONDRINA = "6gyf2d"
# GTIN (EAN) de um produto comum (ex: Coca-Cola 2L)
GTIN_COMUM = "7894900011517"
# GTIN parafuso, vai vir um json
GTIN_COMUM2 = "0000000000001"

RAIO_KM = "20"


def testar_api_menor_preco(gtin, geohash, raio):
    """
    Testa uma consulta isolada na API do Menor Preço,
    imitando a lógica do api_services.py
    """
    # URL da API usada no seu projeto
    url = "https://menorpreco.notaparana.pr.gov.br/api/v1/produtos"
    params = {
        "gtin": gtin,
        "local": geohash,
        "raio": raio
    }
    
    print(f"\n--- 1. TESTANDO CONSULTA ---")
    print(f"    GTIN: {gtin}")
    print(f"    Geohash: {geohash}")
    print(f"    Raio: {raio} km")

    try:
        # 2. Fazendo a requisição
        start_time = time.time()
        resposta = requests.get(url, params=params, timeout=20)
        duration = time.time() - start_time
        status_code = resposta.status_code 
        
        print(f"--- 2. Status Code HTTP: {status_code} --- (Duração: {duration:.2f}s)")

        # 3. Analisando a resposta (baseado na lógica do seu api_services.py)
        if status_code == 200:
            dados = resposta.json()
            produtos_encontrados = dados.get("produtos", [])
            num_produtos = len(produtos_encontrados)
            print(f"✅ SUCESSO: API respondeu e encontrou {num_produtos} notas.")
            
            # Opcional: mostrar a primeira nota para ver se os dados estão corretos
            if num_produtos > 0:
                print("--- Exemplo da primeira nota ---")
                print(json.dumps(produtos_encontrados[0], indent=2, ensure_ascii=False))

        elif status_code == 204:
            print(f"✅ SUCESSO (SEM DADOS): A API respondeu, mas não encontrou notas para esta combinação.")
        
        elif status_code in (404, 401, 403):
            print(f"❌ ERRO CRÍTICO: Status {status_code}. A URL da API pode ter mudado ou o serviço está offline.")
        
        elif 500 <= status_code < 600:
            print(f"❌ ERRO DE SERVIDOR: Status {status_code}. A API do Menor Preço está com problemas internos.")

        else:
            print(f"❌ ERRO INESPERADO: Status {status_code}")

    except requests.RequestException as e:
        print(f"❌ ERRO DE REDE (Timeout ou DNS): {e}")

# --- EXECUÇÃO DOS TESTES ---
if __name__ == "__main__":
    
    # Teste 1: Um caso que DEVE retornar dados (Status 200)
    print("Iniciando Teste 1: Produto Comum (deve achar notas)")
    testar_api_menor_preco(GTIN_COMUM, GEOHASH_LONDRINA, RAIO_KM)
    
    print("\n" + "="*50 + "\n")
    
    # Teste 2: Um caso que NÃO DEVE retornar dados (Status 204)
    print("Iniciando Teste 2: Produto Falso (deve retornar 204 - Sem Dados)")
    testar_api_menor_preco(GTIN_COMUM2, GEOHASH_LONDRINA, RAIO_KM)