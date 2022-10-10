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

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Richard de Andrade",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 9)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'], dag_id='trabalho_02_dag1')
def trabalho_airflow_dag1():

    @task
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def quantidade_passageiros(nome_do_arquivo):
        NOME_DO_ARQUIVO_01 = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df["Operation"] = "Count"
        df["Column"] = "PassengerId"
        res = df.groupby(['Sex', 'Pclass', 'Column', 'Operation']).agg({
            "PassengerId": "count"
        }).reset_index()
        res = res.rename(columns={"PassengerId": "Value"})
        print(res)
        res.to_csv(NOME_DO_ARQUIVO_01, index=False, sep=";")
        return NOME_DO_ARQUIVO_01

    @task
    def media_tarifas(nome_do_arquivo):
        NOME_DO_ARQUIVO_02 = "/tmp/media_da_tarifa_por_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df["Operation"] = "Mean"
        df["Column"] = "Fare"
        res = df.groupby(['Sex', 'Pclass', 'Column', 'Operation']).agg({
            "Fare": "mean"
        }).reset_index()
        res = res.rename(columns={"Fare": "Value"})
        print(res)
        res.to_csv(NOME_DO_ARQUIVO_02, index=False, sep=";")
        return NOME_DO_ARQUIVO_02

    @task
    def soma_sibsp_parch(nome_do_arquivo):
        NOME_DO_ARQUIVO_03 = "/tmp/soma_sibsp_parch_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df["Operation"] = "Sum"
        df["Column"] = "SibSp + Parch"        
        df["SibSp_Parch"] = df["SibSp"] + df["Parch"]
        res = df.groupby(['Sex', 'Pclass', 'Column', 'Operation']).agg({
            "SibSp_Parch": "sum"
        }).reset_index()
        res = res.rename(columns={"SibSp_Parch": "Value"})
        print(res)
        res.to_csv(NOME_DO_ARQUIVO_03, index=False, sep=";")
        return NOME_DO_ARQUIVO_03  

    @task
    def junta_arquivos(nome_do_arquivo_01, nome_do_arquivo_02, nome_do_arquivo_03):
        TABELA_UNICA = "/tmp/tabela_unica.csv"
        df1 = pd.read_csv(nome_do_arquivo_01, sep=";")
        df2 = pd.read_csv(nome_do_arquivo_02, sep=";")
        df3 = pd.read_csv(nome_do_arquivo_03, sep=";")
        col_names = ["Sex", "Pclass", "Column", "Operation", "Value"]
        res = pd.DataFrame(columns = col_names)
        res = res.append([df1, df2, df3]).reset_index()
        res.to_csv(TABELA_UNICA, index=False, sep=";")
        print(res)
        return TABELA_UNICA

    triggerdag = TriggerDagRunOperator(
        task_id="trigga_parte_2",
        trigger_dag_id="trabalho_02_dag2"

    )        

    ing = ingestao()
    ind_01  = quantidade_passageiros(ing)
    ind_02  = media_tarifas(ing)
    ind_03  = soma_sibsp_parch(ing)
    ind_fnl = junta_arquivos(ind_01, ind_02, ind_03) 


    ing >> [ind_01, ind_02, ind_03] >> ind_fnl >> triggerdag

execucao = trabalho_airflow_dag1()
        