import json
from psycopg.rows import dict_row
from etl.banco import PASTA_PROJETO, conectar

PERGUNTAS = [
    "Qual o total de registros na tabela final de viagens válidas?",
    "Qual o total de viagens iniciadas e finalizadas no dia 17 de junho de 2022?",
    "Qual foi o dia da viagem mais longa percorrida?",
    "Qual a média, o desvio padrão, o mínimo, o máximo e os quartis da distância percorrida?",
]


def gerar_respostas():
    consultas = [
        consulta.strip()
        for consulta in (PASTA_PROJETO / "sql/perguntas.sql").read_text().split(";")
        if consulta.strip()
    ]

    with conectar() as conexao:
        with conexao.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                (PASTA_PROJETO / "sql/perguntas.sql").read_text(), prepare=False
            )
            respostas = []
            while True:
                respostas.append(cursor.fetchall())

                if not cursor.nextset():
                    break

        if len(respostas) != len(PERGUNTAS):
            raise ValueError("Quantidade de respostas divergente das perguntas")

        total_arquivos, total_ingerido = conexao.execute(
            "SELECT count(*),sum(quantidade_ingerida) FROM bronze.arquivos WHERE situacao='ingerido'"
        ).fetchone()
        total_ingerido = int(total_ingerido)

        respostas[0][0]["ingestao"] = {
            "total_arquivos": total_arquivos,
            "registros_ingeridos": total_ingerido,
            "registros_excluidos": total_ingerido - respostas[0][0]["total_registros"],
        }

        conexao.execute("DELETE FROM ouro.perguntas_respostas")
        for identificador, (pergunta, consulta, resposta) in enumerate(
            zip(PERGUNTAS, consultas, respostas), 1
        ):
            conteudo = {"viagens": resposta} if identificador == 3 else resposta[0]

            conexao.execute(
                """INSERT INTO ouro.perguntas_respostas
              (id_pergunta,pergunta,resposta,consulta_sql,data_atualizacao)
              VALUES (%s,%s,%s,%s,now())""",
                (identificador, pergunta, json.dumps(conteudo, default=str), consulta),
            )
