import hashlib
import json
import logging
import math
import os
from pathlib import Path
import pyarrow.parquet as parquet
from etl.banco import conectar

FILTRO_VALIDACAO = """data_hora_inicio >= timestamp '2022-01-01'
 AND data_hora_inicio < timestamp '2023-01-01'
 AND data_hora_fim >= data_hora_inicio AND data_hora_fim < timestamp '2023-01-01'
 AND distancia_milhas >= 0 AND distancia_milhas < 'Infinity'::float8"""


def ingerir_arquivo(nome_arquivo):
    caminho = Path(os.environ["DIRETORIO_DADOS"]) / nome_arquivo

    with caminho.open("rb") as arquivo:
        resumo_sha256 = hashlib.file_digest(arquivo, "sha256").hexdigest()

    arquivo_parquet = parquet.ParquetFile(caminho)

    with conectar() as conexao:
        conexao.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (nome_arquivo,))
        metadados = conexao.execute(
            "SELECT sha256,sha256_ingerido,quantidade_registros FROM bronze.arquivos WHERE nome_arquivo=%s FOR UPDATE",
            (nome_arquivo,),
        ).fetchone()

        if not metadados or metadados[0] != resumo_sha256:
            raise ValueError("Registre os metadados do arquivo atual antes da ingestão")

        if metadados[1] == resumo_sha256:
            logging.info("Arquivo já ingerido: %s", nome_arquivo)
            return

        conexao.execute(
            "DELETE FROM prata.viagens_validas WHERE nome_arquivo=%s", (nome_arquivo,)
        )

        conexao.execute(
            "DELETE FROM prata.viagens WHERE nome_arquivo=%s", (nome_arquivo,)
        )

        quantidade = 0

        with conexao.cursor().copy("COPY prata.viagens FROM STDIN") as copia:
            for lote in arquivo_parquet.iter_batches(batch_size=10000):
                for linha in lote.to_pylist():
                    quantidade += 1
                    dados_originais = {
                        chave: (
                            str(valor)
                            if isinstance(valor, float) and not math.isfinite(valor)
                            else valor
                        )
                        for chave, valor in linha.items()
                    }
                    copia.write_row(
                        (
                            nome_arquivo,
                            quantidade,
                            linha["tpep_pickup_datetime"],
                            linha["tpep_dropoff_datetime"],
                            linha["trip_distance"],
                            json.dumps(dados_originais, default=str, allow_nan=False),
                        )
                    )

        if quantidade != metadados[2]:
            raise ValueError("Quantidade ingerida divergente dos metadados")

        conexao.execute(
            """INSERT INTO prata.viagens_validas
          SELECT nome_arquivo,numero_linha,data_hora_inicio,data_hora_fim,distancia_milhas
          FROM prata.viagens WHERE nome_arquivo=%s AND """ + FILTRO_VALIDACAO,
            (nome_arquivo,),
        )

        conexao.execute(
            """UPDATE bronze.arquivos SET sha256_ingerido=%s,
          quantidade_ingerida=%s,data_ingestao=now(),situacao='ingerido'
          WHERE nome_arquivo=%s""",
            (resumo_sha256, quantidade, nome_arquivo),
        )

        logging.info("Ingeridos %d registros de %s", quantidade, nome_arquivo)


def validar_ingestao():
    with conectar() as conexao:
        quantidade_esperada, quantidade_ingerida, pendentes = conexao.execute(
            """SELECT sum(quantidade_registros),sum(quantidade_ingerida),
          count(*) FILTER (WHERE situacao <> 'ingerido') FROM bronze.arquivos"""
        ).fetchone()
        quantidade_prata = conexao.execute(
            "SELECT count(*) FROM prata.viagens"
        ).fetchone()[0]

        if (
            pendentes
            or not quantidade_ingerida
            or quantidade_ingerida != quantidade_esperada
            or quantidade_prata != quantidade_ingerida
        ):
            raise ValueError("Falha na reconciliação dos arquivos e da camada prata")

        if not conexao.execute(
            "SELECT EXISTS(SELECT 1 FROM prata.viagens_validas)"
        ).fetchone()[0]:
            raise ValueError("Não há viagens válidas para responder às perguntas")

        conexao.execute("ANALYZE prata.viagens_validas")
