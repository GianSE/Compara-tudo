# db_manager.py
import mariadb as mdb
import pandas as pd
import logging
import time
from datetime import datetime, date

# --- FUNÇÃO DE CONEXÃO AUXILIAR ---
def _conectar_db(DB_CONFIG):
    """Função auxiliar interna para abrir conexão com o DB correto."""
    return mdb.connect(**DB_CONFIG)

# ============================================
# SEÇÃO DE PRODUTOS
# ============================================

def pegar_ultima_att_gtins(DB_CONFIG):
    """Busca a data de atualização mais recente da tabela de produtos."""
    print("##### COLETANDO ÚLTIMA ATUALIZAÇÃO DOS GTINS #####")
    logging.info("##### COLETANDO ÚLTIMA ATUALIZAÇÃO DOS GTINS #####")

    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    sql = "SELECT MAX(data_insercao) FROM bronze_bluesoft_produtos"
    
    cursor.execute(sql)
    resultado = cursor.fetchone()
    cursor.close()
    conn.close()

    ultima_att_gtins_raw = resultado[0] if resultado else None

    if not ultima_att_gtins_raw:
        return None
    if isinstance(ultima_att_gtins_raw, datetime):
        return ultima_att_gtins_raw.date()
    if isinstance(ultima_att_gtins_raw, date):
        return ultima_att_gtins_raw

    try:
        return datetime.strptime(str(ultima_att_gtins_raw), "%Y-%m-%d %H:%M:%S").date()
    except ValueError:
        try:
            return datetime.strptime(str(ultima_att_gtins_raw), "%Y-%m-%d").date()
        except ValueError:
            logging.error(f"Formato de data desconhecido: {ultima_att_gtins_raw}")
            return None

def insert_produtos_atualizados(DB_CONFIG, produtos_df):
    """INSERT Incremental sem normalização de zeros à esquerda."""
    if produtos_df.empty: return

    logging.info(f"4. Inserindo {len(produtos_df)} produtos (Incremental)...")
    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    sql = """
        INSERT INTO bronze_bluesoft_produtos 
        (gtin, id_produto, descricao, fabricante, apresentacao, tipo, data_insercao)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    sucessos = 0
    for _, row in produtos_df.iterrows():
        try:
            # CORREÇÃO: Adicionado vírgulas após row["GTIN"]
            cursor.execute(sql, (
                str(row["GTIN"]), # Sem zfill
                row["codigo_interno_produto"], 
                row["descricao_produto"], 
                row["nome_fantasia_fabricante"], 
                row["apresentacao_produto"],
                row.get("tipo"),
                agora
            ))
            sucessos += 1
        except Exception as e:
            logging.error(f"Erro ao inserir {row['GTIN']}: {e}")

    conn.commit()
    logging.info(f"Sucesso: {sucessos} produtos inseridos.")
    cursor.close()
    conn.close()

def coletar_produtos_no_banco(DB_CONFIG):
    """Coleta os 54 GTINs da nova tabela."""
    print("##### COLETANDO OS 54 PRODUTOS DEFINIDOS #####")
    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    sql = "SELECT DISTINCT gtin FROM bronze_bluesoft_produtos"
    cursor.execute(sql)
    gtins = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(gtins, columns=["gtin"])

def grupo_eans_selecionados(EANs, ult_gtin, arquivo_indice):
    """Retorna a lista completa (sem grupos)."""
    return EANs

# ============================================
# SEÇÃO DE LOJAS E GEOHASH
# ============================================

def pegar_geohashs_BD(DB_CONFIG):
    """Retorna Londrina e Cornélio fixos."""
    print("##### DEFININDO REGIOES DE BUSCA (Londrina e Cornélio) #####")
    lista_geohashs = ["6gge", "6ggup"]
    return pd.DataFrame(lista_geohashs, columns=["geohash"])

def inserir_lojas_sc(Lojas_SC, now_obj, DB_CONFIG):
    """Insere ou atualiza lojas novas."""
    if Lojas_SC.empty: return
    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    Lojas_SC = Lojas_SC.astype(object).where(pd.notnull(Lojas_SC), None)

    data_tuples = [
        (
            row.id_loja, row.nome_fantasia, row.razao_social, row.logradouro,
            row.Latitude, row.Longitude, row.cidade, row.geohash
        )
        for row in Lojas_SC.itertuples(index=False)
    ]

    sql = """
        INSERT INTO bronze_menorPreco_lojas
        (id_loja, nome_fantasia, razao_social, logradouro, latitude, longitude, cidade, geohash, data_atualizacao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            latitude = VALUES(latitude), longitude = VALUES(longitude),
            geohash = VALUES(geohash), data_atualizacao = NOW();
    """
    cursor.executemany(sql, data_tuples)
    conn.commit()
    cursor.close()
    conn.close()

# ============================================
# SEÇÃO DE NOTAS FISCAIS
# ============================================

def inserir_notas(Notas, now_obj, DB_CONFIG):
    """Insere notas fiscais sem normalização de GTIN."""
    if Notas.empty: return
    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    Notas = Notas.where(pd.notnull(Notas), None) 

    data_tuples = []
    for row in Notas.itertuples(index=False):
        # CORREÇÃO: Adicionado vírgula após row.gtin
        data_tuples.append((
            row.id_nota, row.datahora, row.id_loja, row.geohash, 
            str(row.gtin), # Sem zfill
            row.descricao,
            row.valor_desconto, row.valor_tabela, row.valor, 
            row.cidade
        ))

    sql = """
        INSERT INTO bronze_menorPreco_notas 
        (id_nota, date, id_loja, geohash, gtin, descricao, valor_desconto, valor_tabela, valor, cidade, data_atualizacao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE data_atualizacao = NOW();
    """
    cursor.executemany(sql, data_tuples)
    conn.commit()
    cursor.close()
    conn.close()

def insert_produtos_manuais(DB_CONFIG, produtos_list: list):
    """Setup manual inicial sem normalização."""
    if not produtos_list: return
    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    sql = """
        INSERT INTO bronze_bluesoft_produtos 
        (gtin, id_produto, descricao, fabricante, apresentacao, tipo, data_insercao)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    sucessos = 0
    for produto in produtos_list:
        try:
            # CORREÇÃO: Adicionado vírgulas faltantes
            cursor.execute(sql, (
                str(produto["gtin"]), 
                produto.get("id_produto", produto["gtin"]), 
                produto["descricao"], 
                produto.get("fabricante", "NÃO INFORMADO"), 
                produto.get("apresentacao", "NÃO INFORMADA"),
                produto.get("tipo", "NA"),
                agora
            ))
            sucessos += 1
        except Exception as e:
            logging.error(f"Erro ao inserir GTIN {produto['gtin']}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

def coletar_lojas_do_banco(DB_CONFIG):
    """
    Coleta a lista de IDs de lojas já cadastrados E que possuem coordenadas completas.
    """
    print("##### COLETANDO LOJAS CADASTRADAS NO BANCO (COM COORDENADAS) #####")
    logging.info("##### COLETANDO LOJAS CADASTRADAS NO BANCO (COM COORDENADAS) #####")

    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()

    sql = "SELECT id_loja FROM bronze_menorPreco_lojas WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    
    cursor.execute(sql)
    lista_lojas = cursor.fetchall()
    cursor.close()
    conn.close()

    Lojas = pd.DataFrame(lista_lojas, columns=["id_loja"])
    return Lojas