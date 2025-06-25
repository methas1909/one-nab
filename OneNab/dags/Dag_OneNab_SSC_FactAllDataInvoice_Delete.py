from asyncore import read
from airflow.models import Variable
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql import SparkSession
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'TCCDE_team',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# [Get secrets from Airflow variables]
secrets = Variable.get('TCCDE_SalesETL_SECRET', deserialize_json=True)
ONAB_MSSQL_URL = secrets['ONAB_MSSQL_URL']
ONAB_MSSQL_USER = secrets['ONAB_MSSQL_USER']
ONAB_MSSQL_PASSWORD = secrets['ONAB_MSSQL_PASSWORD']

RTM_STG_PRD_TIDB_URL = secrets['RTM_STG_PRD_TIDB_URL']
RTM_STG_PRD_TIDB_URL_LOGS = secrets['RTM_STG_PRD_TIDB_URL_LOGS']
RTM_STG_PRD_TIDB_USER = secrets['RTM_STG_PRD_TIDB_USER']
RTM_STG_PRD_TIDB_PASSWORD = secrets['RTM_STG_PRD_TIDB_PASSWORD']


sql_del_OneNab_SSC_FactAllDataInvoice_delete = f"DELETE FROM SSC_FactAllDataInvoice WHERE ETLLoadData >= DATEADD(day, -5, CAST(GETDATE() AS date))"

with DAG(
    'Dag_TCC_ONENAB_SSC_FactAllDataInvoice_delete',
    start_date=pendulum.datetime(2023, 4, 25, 3, 00, 0, tz='Asia/Bangkok'),
    schedule_interval='02 0 * * *',

    catchup=False,
    tags=['ONE NAB', 'FactAllDataInvoice delete', 'spark'],
    default_args=default_args,
    render_template_as_native_obj=True,
    description='OneNab_SSC',
    access_control={
        "TCCDE": {"can_read", "can_edit", "can_delete"}
    }
) as dag:

    base_config = {
        "executor_memory": "32g",
        "driver_memory": "5g",
        "total_executor_cores": 10,
        "executor_cores": 10,
        "env_vars": {
            "ONAB_MSSQL_URL": ONAB_MSSQL_URL,
            "ONAB_MSSQL_USER": ONAB_MSSQL_USER,
            "ONAB_MSSQL_PASSWORD": ONAB_MSSQL_PASSWORD,
            "RTM_STG_PRD_TIDB_URL": RTM_STG_PRD_TIDB_URL,
            "RTM_STG_PRD_TIDB_URL_LOGS": RTM_STG_PRD_TIDB_URL_LOGS,
            "RTM_STG_PRD_TIDB_USER": RTM_STG_PRD_TIDB_USER,
            "RTM_STG_PRD_TIDB_PASSWORD": RTM_STG_PRD_TIDB_PASSWORD
        }
    }

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # [Start Raw_Stage task group]
    with TaskGroup('SSC_FactAllDataInvoice_delete', tooltip='SSC_FactAllDataInvoice_delete') as SSC_FactAllDataInvoice_delete:
        # Start Sub ------
        OneNab_SSC_FactAllDataInvoice_delete_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAllDataInvoice_delete', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAllDataInvoice_delete)

        email_op = EmailOperator(
            task_id='Send_email_JobStatus',
            to=["phonkrit.s@tcc-technology.com"],
            subject="Success Job SSC_FactAllDataInvoice_delete",
            html_content='<strong><span style="color: #339966;">Success Job SSC_FactAllDataInvoice_delete</span></strong>',
        )
        OneNab_SSC_FactAllDataInvoice_delete_Del >> email_op

    chain(start, SSC_FactAllDataInvoice_delete, end)
