# utils/atualizar_coords_nulas.py
import mariadb
import requests
import time
import sys
import os

# --- CONFIGURAÇÃO DE CAMINHOS ---
# Adiciona a pasta raiz ao path para conseguir importar o config.py
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

try:
    from config import DB_CONFIG
except ImportError:
    print("❌ ERRO: 'config.py' não encontrado na pasta raiz.")
    sys.exit(1)

def conectar_db():
    return mariadb.connect(**DB_CONFIG, database="dbDrogamais")

def buscar_lat_lon_nominatim(endereco):
    """
    Consulta a API do Nominatim para obter lat/lon de um endereço.
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": endereco,
        "format": "jsonv2",
        "limit": 1,
        "addressdetails": 1
    }
    # User-Agent é obrigatório
    headers = {"User-Agent": "AtualizadorCoords/1.0 (admin@drogamais.com.br)"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            dados = response.json()
            if dados:
                lat = dados[0].get("lat")
                lon = dados[0].get("lon")
                return lat, lon
    except Exception as e:
        print(f"   ⚠️ Erro na requisição para '{endereco}': {e}")
    
    return None, None

def limpar_endereco(logradouro, cidade):
    """
    Padroniza o endereço para melhor busca.
    """
    if not logradouro: logradouro = ""
    if not cidade: cidade = ""
    
    end = f"{logradouro}, {cidade}, PR, Brasil"
    
    # Limpezas básicas
    end = end.upper().replace("(MUNICÍPIO", "").replace(")", "")
    return " ".join(end.split()) # Remove espaços duplos

def main():
    print("🚀 INICIANDO ATUALIZAÇÃO MANUAL...")
    
    conn = None
    try:
        conn = conectar_db()
        cursor = conn.cursor()

        # 1. Buscar lojas com coordenadas faltando
        sql_busca = """
            SELECT id_loja, logradouro, cidade, nome_fantasia 
            FROM bronze_menorPreco_lojas 
            WHERE latitude IS NULL OR longitude IS NULL
        """
        cursor.execute(sql_busca)
        lojas_incompletas = cursor.fetchall()

        total = len(lojas_incompletas)
        print(f"📋 Total de lojas para atualizar: {total}")

        if total == 0:
            print("✅ Nenhuma loja precisa de atualização.")
            return

        # 2. Iterar e atualizar
        sql_update = """
            UPDATE bronze_menorPreco_lojas 
            SET latitude = %s, longitude = %s, data_atualizacao = NOW()
            WHERE id_loja = %s
        """

        sucessos = 0
        falhas = 0

        for i, loja in enumerate(lojas_incompletas, 1):
            id_loja = loja[0]
            logradouro = loja[1]
            cidade = loja[2]
            
            endereco_completo = limpar_endereco(logradouro, cidade)
            
            print(f"[{i}/{total}] Buscando: {endereco_completo} ...", end=" ")
            
            lat, lon = buscar_lat_lon_nominatim(endereco_completo)
            
            if lat and lon:
                cursor.execute(sql_update, (lat, lon, id_loja))
                conn.commit() 
                print(f"✅ OK! ({lat}, {lon})")
                sucessos += 1
            else:
                print("❌ Não encontrado.")
                falhas += 1
            
            # Pausa de 1.1s para não bloquear o IP no Nominatim
            time.sleep(1.1)

        print("\n" + "="*30)
        print(f"✅ Atualizados: {sucessos}")
        print(f"❌ Falhas: {falhas}")
        print("="*30)

    except mariadb.Error as e:
        print(f"\n❌ Erro de banco de dados: {e}")
    except KeyboardInterrupt:
        print("\n🛑 Interrompido pelo usuário.")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()