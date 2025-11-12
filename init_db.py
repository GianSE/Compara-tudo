# init_db.py
import mariadb as mdb
from config import DB_CONFIG # Importa suas configurações

# SQL para criar as tabelas de destino
SQL_CREATE_PRODUTOS = """
CREATE TABLE IF NOT EXISTS bronze_menorPreco_produtos (
    gtin VARCHAR(14) PRIMARY KEY,
    id_produto VARCHAR(50),
    descricao VARCHAR(255),
    fabricante VARCHAR(255),
    apresentacao VARCHAR(255),
    data_atualizacao DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
"""

SQL_CREATE_LOJAS = """
CREATE TABLE IF NOT EXISTS bronze_menorPreco_lojas (
    id_loja VARCHAR(50) PRIMARY KEY,
    nome_fantasia VARCHAR(255),
    razao_social VARCHAR(255),
    logradouro VARCHAR(255),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(10, 8),
    geohash VARCHAR(20),
    data_atualizacao DATETIME,
    bandeira VARCHAR(100) NULL DEFAULT 'OUTRAS BANDEIRAS'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
"""

SQL_CREATE_NOTAS = """
CREATE TABLE IF NOT EXISTS bronze_menorPreco_notas (
    id BIGINT(20) NOT NULL DEFAULT '0'
    id_nota VARCHAR(120)
    date DATETIME,
    id_loja VARCHAR(50),
    geohash VARCHAR(20),
    gtin VARCHAR(14),
    descricao VARCHAR(255),
    valor_desconto DECIMAL(10, 2),
    valor_tabela DECIMAL(10, 2),
    valor DECIMAL(10, 2),
    cidade VARCHAR(100),
    data_atualizacao DATETIME,
    PRIMARY KEY (`id`) USING BTREE,
	UNIQUE INDEX `idx_unique_id_nota` (`id_nota`) USING BTREE,
	INDEX `idx_notas_id_loja` (`id_loja`) USING BTREE,
	INDEX `idx_notas_gtin` (`gtin`) USING BTREE,
	INDEX `idx_notas_geohash` (`geohash`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
"""

def inicializar_banco():
    """
    Conecta ao banco de dados e garante que as tabelas de 
    destino do Menor Preço existam.
    """
    try:
        # Conecta ao banco de dados principal
        conn = mdb.connect(**DB_CONFIG, database="dbDrogamais")
        cursor = conn.cursor()
        
        print("Verificando tabela 'bronze_menorPreco_lojas'...")
        cursor.execute(SQL_CREATE_LOJAS)
        
        print("Verificando tabela 'bronze_menorPreco_produtos'...")
        cursor.execute(SQL_CREATE_PRODUTOS)
        
        print("Verificando tabela 'bronze_menorPreco_notas'...")
        cursor.execute(SQL_CREATE_NOTAS)
        
        conn.commit()
        print("✅ Verificação do esquema do banco concluída com sucesso!")
    
    except mdb.Error as e:
        print(f"❌ Erro ao inicializar o banco de dados: {e}")
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    print("Iniciando setup do banco de dados...")
    inicializar_banco()