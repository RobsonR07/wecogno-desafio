import hashlib
import json
import os
from pathlib import Path
from urllib.parse import quote
import pyarrow.parquet as parquet
from etl.banco import conectar


def registrar_arquivo(nome_arquivo):
    caminho = Path(os.environ["DIRETORIO_DADOS"]) / nome_arquivo

    with caminho.open("rb") as arquivo:
        resumo_sha256 = hashlib.file_digest(arquivo, "sha256").hexdigest()

    arquivo_parquet = parquet.ParquetFile(caminho)
    esquema = [
        {"nome": campo.name, "tipo": str(campo.type), "aceita_nulo": campo.nullable}
        for campo in arquivo_parquet.schema_arrow
    ]
    url_origem = os.environ.get(
        "URL_ORIGEM_DADOS",
        "https://raw.githubusercontent.com/datarisk-io/data-engineering-challenge/master/nyc-tlc-data/",
    ) + quote(nome_arquivo)

    with conectar() as conexao:
        conexao.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (nome_arquivo,))

        conexao.execute(
            """INSERT INTO bronze.arquivos
          (nome_arquivo,url_origem,caminho_local,tamanho_bytes,sha256,quantidade_registros,esquema_parquet,situacao)
          VALUES (%s,%s,%s,%s,%s,%s,%s,'registrado')
          ON CONFLICT(nome_arquivo) DO UPDATE SET url_origem=excluded.url_origem,
          caminho_local=excluded.caminho_local,tamanho_bytes=excluded.tamanho_bytes,
          sha256=excluded.sha256,quantidade_registros=excluded.quantidade_registros,
          esquema_parquet=excluded.esquema_parquet,
          situacao=CASE WHEN bronze.arquivos.sha256_ingerido=excluded.sha256
                        THEN 'ingerido' ELSE 'registrado' END""",
            (
                nome_arquivo,
                url_origem,
                str(caminho),
                caminho.stat().st_size,
                resumo_sha256,
                arquivo_parquet.metadata.num_rows,
                json.dumps(esquema),
            ),
        )


def registrar_metadados():
    for mes in range(1, 13):
        registrar_arquivo(f"yellow_tripdata_2022-{mes:02d}.parquet.gz")
