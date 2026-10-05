import os
from pathlib import Path
import psycopg

PASTA_PROJETO = Path(__file__).resolve().parents[2]


def conectar():
    return psycopg.connect(os.environ["CONEXAO_TAXI"])


def preparar_banco():
    with conectar() as conexao:
        conexao.execute((PASTA_PROJETO / "sql/estrutura.sql").read_text())
