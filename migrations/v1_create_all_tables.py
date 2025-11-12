# init_db.py

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
    id BIGINT(20) NOT NULL AUTO_INCREMENT,
    id_nota VARCHAR(120),
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

# ADICIONADO: SQL para criar a tabela Silver
SQL_CREATE_SILVER_NOTAS = """
CREATE TABLE IF NOT EXISTS `silver_menorPreco_notas` (
    `id_nota` VARCHAR(150) NOT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `date` DATETIME NULL DEFAULT NULL,
    `id_loja` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `geohash` VARCHAR(12) NULL DEFAULT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `id_produto` VARCHAR(20) NULL DEFAULT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `gtin` VARCHAR(14) NULL DEFAULT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `descricao` VARCHAR(120) NULL DEFAULT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `valor` DECIMAL(10,2) NULL DEFAULT NULL,
    `valor_desconto` DECIMAL(10,2) NULL DEFAULT NULL,
    `valor_tabela` DECIMAL(10,2) NULL DEFAULT NULL,
    `cidade` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `data_atualizacao` DATE NULL DEFAULT NULL,
    `bandeira` VARCHAR(100) NULL DEFAULT 'OUTRAS BANDEIRAS' COLLATE 'utf8mb4_uca1400_ai_ci',
    `latitude` DECIMAL(10,8) NULL DEFAULT NULL,
    `longitude` DECIMAL(11,8) NULL DEFAULT NULL,
    `nome_fantasia` VARCHAR(150) NULL DEFAULT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `razao_social` VARCHAR(150) NULL DEFAULT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `PRODUTO` VARCHAR(255) NULL DEFAULT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `microrregiao` VARCHAR(100) NULL DEFAULT NULL COLLATE 'utf8mb4_uca1400_ai_ci',
    `DateRelacionamento` DATE NULL DEFAULT NULL,
    `media_gtin` DECIMAL(10,4) NULL DEFAULT NULL,
    `media_cidade_gtin` DECIMAL(10,4) NULL DEFAULT NULL,
    `rank_cidade` INT(11) NULL DEFAULT NULL,
    `rank_microrregiao` INT(11) NULL DEFAULT NULL,
    `considerado` CHAR(1) NULL DEFAULT NULL COMMENT 'S = considerado, N = fora da média (±50%)' COLLATE 'utf8mb4_uca1400_ai_ci',
    PRIMARY KEY (`id_nota`) USING BTREE,
    INDEX `idx_silver_gtin` (`gtin`) USING BTREE,
    INDEX `idx_silver_id_loja` (`id_loja`) USING BTREE,
    INDEX `idx_silver_bandeira` (`bandeira`) USING BTREE,
    INDEX `idx_silver_cidade` (`cidade`) USING BTREE,
    INDEX `idx_silver_data` (`DateRelacionamento`) USING BTREE
)
COLLATE='utf8mb4_uca1400_ai_ci'
ENGINE=InnoDB;
"""