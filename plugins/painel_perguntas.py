import os
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo
import psycopg
from psycopg.rows import dict_row
from flask import Blueprint
from flask_appbuilder import BaseView, expose
from airflow.auth.managers.models.resource_details import AccessView
from airflow.plugins_manager import AirflowPlugin
from airflow.www.auth import has_access_view

painel = Blueprint("painel_perguntas", __name__, template_folder="templates")


@painel.app_template_filter("numero_brasileiro")
def formatar_numero(valor, casas=0):
    if valor is None:
        return "—"

    return (
        f"{Decimal(str(valor)):,.{casas}f}".replace(",", "_")
        .replace(".", ",")
        .replace("_", ".")
    )


@painel.app_template_filter("data_brasileira")
def formatar_data(valor):
    if not valor:
        return "—"

    return datetime.fromisoformat(str(valor)).strftime("%d/%m/%Y")


class PainelPerguntas(BaseView):
    route_base = "/perguntas-respostas"
    default_view = "exibir"

    @expose("/")
    @has_access_view(AccessView.PLUGINS)
    def exibir(instancia):
        registros = []
        indicadores = {}
        mensagem = None
        ultima_atualizacao = None

        try:
            with psycopg.connect(
                os.environ["CONEXAO_TAXI"], connect_timeout=5, row_factory=dict_row
            ) as conexao:
                registros = conexao.execute(
                    "SELECT * FROM ouro.perguntas_respostas ORDER BY id_pergunta"
                ).fetchall()

            if registros:
                indicadores.update(registros[0]["resposta"]["ingestao"])
                indicadores["registros_validos"] = registros[0]["resposta"][
                    "total_registros"
                ]
                ultima_atualizacao = (
                    max(registro["data_atualizacao"] for registro in registros)
                    .astimezone(ZoneInfo("America/Sao_Paulo"))
                    .strftime("%d/%m/%Y às %H:%M")
                )
            else:
                mensagem = "Ainda não há respostas. Execute a DAG taxi_nova_york_2022 para atualizar o painel."

        except psycopg.Error:
            mensagem = "Não foi possível consultar os resultados. Confira a conexão com o PostgreSQL e execute a DAG."

        return instancia.render_template(
            "painel_perguntas.html",
            title="Perguntas e respostas",
            registros=registros,
            indicadores=indicadores,
            mensagem=mensagem,
            ultima_atualizacao=ultima_atualizacao,
        )


class PluginPerguntasRespostas(AirflowPlugin):
    name = "perguntas_respostas_taxi"
    flask_blueprints = [painel]
    appbuilder_views = [
        {
            "name": "Perguntas e respostas",
            "category": "WeCogno",
            "view": PainelPerguntas(),
        }
    ]
