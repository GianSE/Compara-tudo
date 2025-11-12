import requests
import sys
import os
import json # Para formatar o print da resposta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import GOOGLE_API_KEY

def testar_google_api(endereco):
    """
    Versão de teste isolada da sua função 'obter_lat_lon_google'.
    """
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": endereco,
        "key": GOOGLE_API_KEY,
    }
    
    print(f"\n--- 1. TESTANDO ENDEREÇO: '{endereco}' ---")

    try:
        # 2. Fazendo a requisição
        resposta = requests.get(url, params=params, timeout=20)
        status_code = resposta.status_code 
        print(f"--- 2. Status Code HTTP: {status_code} ---")

        if status_code != 200:
            print(f"❌ ERRO HTTP: {status_code}")
            return None, None

        # 3. Analisando o JSON
        dados = resposta.json()
        json_status = dados.get("status")
        print(f"--- 3. Status da API Google: {json_status} ---")

        # Descomente a linha abaixo se quiser ver o JSON inteiro
        # print(json.dumps(dados, indent=2, ensure_ascii=False))

        # 4. Extraindo os dados (exatamente como no seu script)
        if json_status == "OK" and dados.get("results"):
            latitude = dados["results"][0]["geometry"]["location"]["lat"]
            longitude = dados["results"][0]["geometry"]["location"]["lng"]

            print(f"✅ SUCESSO!")
            print(f"   Latitude: {latitude}")
            print(f"   Longitude: {longitude}")
            return latitude, longitude
        
        elif json_status == "ZERO_RESULTS":
            print(f"⚠️ AVISO: A API funcionou, mas não encontrou o endereço.")
            return None, None
        
        elif json_status == "REQUEST_DENIED":
            print(f"❌ ERRO CRÍTICO: A API Key está inválida ou bloqueada!")
            print(f"   Mensagem: {dados.get('error_message')}")
            return None, None
            
        else:
            print(f"❌ ERRO INESPERADO: Status da API foi {json_status}")
            return None, None

    except requests.RequestException as e:
        print(f"❌ ERRO DE REDE (Timeout ou DNS): {e}")
        return None, None

# --- EXECUÇÃO DOS TESTES ---
if __name__ == "__main__":
    # Teste 1: Um endereço que DEVE funcionar
    testar_google_api("Catedral de Londrina, PR, Brasil")
    
    # Teste 2: Um endereço que NÃO DEVE ser encontrado
    testar_google_api("Rua Fictícia, 1234, Nárnia")