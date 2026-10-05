from datetime import datetime, timedelta
from airflow.decorators import dag, task
from etl.banco import preparar_banco
from etl.bronze import registrar_metadados
from etl.prata import ingerir_arquivo, validar_ingestao
from etl.ouro import gerar_respostas


@dag(
    dag_id="taxi_nova_york_2022",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["wecogno", "bronze", "prata", "ouro"],
)
def processamento_taxi():
    @task
    def preparar_estrutura():
        preparar_banco()

    @task
    def registrar_bronze():
        registrar_metadados()

    @task
    def ingerir_prata(nome_arquivo):
        ingerir_arquivo(nome_arquivo)

    @task
    def validar_prata():
        validar_ingestao()

    @task
    def publicar_ouro():
        gerar_respostas()

    arquivos = [f"yellow_tripdata_2022-{mes:02d}.parquet.gz" for mes in range(1, 13)]
    (
        preparar_estrutura()
        >> registrar_bronze()
        >> ingerir_prata.expand(nome_arquivo=arquivos)
        >> validar_prata()
        >> publicar_ouro()
    )


processamento_taxi()
