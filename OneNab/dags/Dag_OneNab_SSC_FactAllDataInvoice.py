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


# [Start assigning the date for quering]
# START_DATE = '{{ dag_run.conf.get("start_date", (execution_date - macros.timedelta(days=62)).replace(day=1)| ds ) }}'
# END_DATE = '{{ dag_run.conf.get("end_date", (((execution_date).replace(day=1) + macros.timedelta(days=32)).replace(day=1) - macros.timedelta(days=1)) | ds ) }}'

# [Start assigning the date to sql query for raw stage]
# sql_raw = f"DELETE FROM RawST_Sales_Data_SAP WHERE CAST(CAST(POSTING_DATE as char(8)) as date) BETWEEN '{START_DATE}' AND '{END_DATE}'"

sql_del_OneNab_SSC_FactAllDataInvoice = f"DELETE FROM SSC_FactAllDataInvoice WHERE ETLLoadData >= DATEADD(day, -5, CAST(GETDATE() AS date))"
# sql_del_OneNab_SSC_DimCustGrp1 = f"TRUNCATE TABLE OneNab_SSC_DimCustGrp1"
# sql_del_OneNab_SSC_DimCustGrpSap = f"TRUNCATE TABLE OneNab_SSC_DimCustGrpSap"
# sql_del_OneNab_SSC_DimCustomer = f"TRUNCATE TABLE OneNab_SSC_DimCustomer"
# sql_del_OneNab_SSC_DimLocation = f"TRUNCATE TABLE OneNab_SSC_DimLocation"
# sql_del_OneNab_SSC_DimMaterial = f"TRUNCATE TABLE OneNab_SSC_DimMaterial"
# sql_del_OneNab_SSC_DimSalesman = f"TRUNCATE TABLE OneNab_SSC_DimSalesman"
# # sql_del_OneNab_SSC_AggInvByBrandYM = f"DELETE  FROM OneNab_SSC_AggInvByBrandYM WHERE PYLoadDate >= DATE_ADD(CURDATE(), INTERVAL -3 DAY)"
# sql_del_OneNab_SSC_AggInvByBrandYM = f"TRUNCATE TABLE OneNab_SSC_AggInvByBrandYM"
# sql_del_OneNab_SSC_FactRouteSalesman = f"TRUNCATE TABLE OneNab_SSC_FactRouteSalesman"
# sql_del_OneNab_SSC_DimRoute = f"TRUNCATE TABLE OneNab_SSC_DimRoute"


# [End assigning the date to sql query for raw stage]

# [Add access_control for the team as line 53 - line 55]
with DAG(
    'Dag_TCC_ONENAB_SSC_FactAllDataInvoice',
    start_date=pendulum.datetime(2023, 4, 25, 3, 00, 0, tz='Asia/Bangkok'),
    schedule_interval='30 4 * * *',

    catchup=False,
    tags=['ONE NAB', 'FactAllDataInvoice', 'spark'],
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
    with TaskGroup('SSC_FactAllDataInvoice', tooltip='SSC_FactAllDataInvoice') as SSC_FactAllDataInvoice:
        # Start Sub ------

        OneNab_SSC_FactAllDataInvoice_Load = SparkSubmitOperator(**base_config,
                                                                 task_id="Load_OneNab_SSC_FactAllDataInvoice", name='Load_OneNab_SSC_FactAllDataInvoice', conn_id='DE_SparkConn',
                                                                 application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactAllDataInvoice.py",
                                                                 conf={
                                                                     "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                                     "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                                 }
                                                                 )

        OneNab_SSC_FactAllDataInvoice_Load
        # email_op

    chain(start, SSC_FactAllDataInvoice, end)
