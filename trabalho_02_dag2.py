"""
    Atividade Airflow

    Autor:
        Richard de Andrade
    Data:
        2022-10-09
"""
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': "Richard de Andrade",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 9)
}


@dag(default_args=default_args, schedule_interval=None, catchup=False, tags=['Titanic'], dag_id='trabalho_02_dag2')
def trabalho_airflow_dag2():

    @task
    def trata_arquivo_final():
        ARQUIVO_FINAL = "/tmp/resultados.csv"
        ARQUIVO_DAG1  = "/tmp/tabela_unica.csv"
        df = pd.read_csv(ARQUIVO_DAG1, sep=';')
        res = df.groupby(['Column']).agg({
            "Value": "mean"
        }).reset_index()
        print(res)
        res.to_csv(ARQUIVO_FINAL, index=False, sep=";")
        return ARQUIVO_FINAL

    fim = DummyOperator(task_id="fim")

    trt = trata_arquivo_final()

    trt >> fim

execucao = trabalho_airflow_dag2()
        