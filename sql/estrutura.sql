CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS prata;
CREATE SCHEMA IF NOT EXISTS ouro;
CREATE TABLE IF NOT EXISTS bronze.arquivos (
 nome_arquivo text PRIMARY KEY,
 url_origem text NOT NULL,
 caminho_local text NOT NULL,
 tamanho_bytes bigint NOT NULL,
 sha256 text NOT NULL,
 sha256_ingerido text,
 quantidade_registros bigint NOT NULL,
 quantidade_ingerida bigint,
 esquema_parquet jsonb NOT NULL,
 data_cadastro timestamptz NOT NULL DEFAULT now(),
 data_ingestao timestamptz,
 situacao text NOT NULL CHECK (situacao IN ('registrado', 'ingerido'))
);
CREATE TABLE IF NOT EXISTS prata.viagens (
 nome_arquivo text NOT NULL REFERENCES bronze.arquivos(nome_arquivo),
 numero_linha bigint NOT NULL,
 data_hora_inicio timestamp,
 data_hora_fim timestamp,
 distancia_milhas double precision,
 dados_originais jsonb NOT NULL,
 PRIMARY KEY(nome_arquivo, numero_linha)
);
CREATE TABLE IF NOT EXISTS prata.viagens_validas (
 nome_arquivo text NOT NULL REFERENCES bronze.arquivos(nome_arquivo),
 numero_linha bigint NOT NULL,
 data_hora_inicio timestamp NOT NULL,
 data_hora_fim timestamp NOT NULL,
 distancia_milhas double precision NOT NULL,
 PRIMARY KEY(nome_arquivo, numero_linha)
);
CREATE TABLE IF NOT EXISTS ouro.perguntas_respostas (
 id_pergunta integer PRIMARY KEY CHECK (id_pergunta BETWEEN 1 AND 4),
 pergunta text NOT NULL,
 resposta jsonb NOT NULL,
 consulta_sql text NOT NULL,
 data_atualizacao timestamptz NOT NULL
);
CREATE OR REPLACE VIEW prata.detalhes_viagens AS
SELECT nome_arquivo, numero_linha,
(dados_originais->>'VendorID')::bigint AS codigo_fornecedor,
data_hora_inicio, data_hora_fim,
(dados_originais->>'passenger_count')::double precision AS quantidade_passageiros,
distancia_milhas,
(dados_originais->>'RatecodeID')::double precision AS codigo_tarifa,
dados_originais->>'store_and_fwd_flag' AS indicador_armazenamento,
(dados_originais->>'PULocationID')::bigint AS codigo_local_embarque,
(dados_originais->>'DOLocationID')::bigint AS codigo_local_desembarque,
(dados_originais->>'payment_type')::bigint AS tipo_pagamento,
(dados_originais->>'fare_amount')::double precision AS valor_tarifa,
(dados_originais->>'extra')::double precision AS valor_extra,
(dados_originais->>'mta_tax')::double precision AS imposto_mta,
(dados_originais->>'tip_amount')::double precision AS valor_gorjeta,
(dados_originais->>'tolls_amount')::double precision AS valor_pedagios,
(dados_originais->>'improvement_surcharge')::double precision AS taxa_melhoria,
(dados_originais->>'total_amount')::double precision AS valor_total,
(dados_originais->>'congestion_surcharge')::double precision AS taxa_congestionamento,
(dados_originais->>'airport_fee')::double precision AS taxa_aeroporto
FROM prata.viagens;

GRANT USAGE ON SCHEMA bronze, prata, ouro TO consulta;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze, prata, ouro TO consulta;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, prata, ouro
    GRANT SELECT ON TABLES TO consulta;
