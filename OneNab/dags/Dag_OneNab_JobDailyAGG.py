from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain


default_args = {
    'owner': 'TCCDE_team',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# [Get secrets from Airflow variables]
secrets = Variable.get('TCCDE_SalesETL_SECRET', deserialize_json=True)
secrets_scp = Variable.get('TCCDE_SalesETL_SECRET_SCP', deserialize_json=True)


ONAB_MSSQL_URL = secrets['ONAB_MSSQL_URL']
ONAB_MSSQL_USER = secrets['ONAB_MSSQL_USER']
ONAB_MSSQL_PASSWORD = secrets['ONAB_MSSQL_PASSWORD']

ONABSTG_MSSQL_URL = secrets['ONABSTG_MSSQL_URL']
ONABSTG_MSSQL_USER = secrets['ONABSTG_MSSQL_USER']
ONABSTG_MSSQL_PASSWORD = secrets['ONABSTG_MSSQL_PASSWORD']


RTM_STG_PRD_TIDB_URL = secrets['RTM_STG_PRD_TIDB_URL']
RTM_STG_PRD_TIDB_URL_LOGS = secrets['RTM_STG_PRD_TIDB_URL_LOGS']
RTM_STG_PRD_TIDB_USER = secrets['RTM_STG_PRD_TIDB_USER']
RTM_STG_PRD_TIDB_PASSWORD = secrets['RTM_STG_PRD_TIDB_PASSWORD']


SCP_HOST = secrets_scp['SCP_HOST']
SCP_USER = secrets_scp['SCP_USER']
SCP_PASS = secrets_scp['SCP_PASS']


sql_del_OneNab_SCC_AggInvVolByBrandYM_INC = f"""DELETE FROM SCC_AggInvVolByBrandYM 
                                                WHERE [DateKey] >= Cast (Concat(FORMAT((DATEADD(mm, DATEDIFF(mm, 0, getdate()) - 5, 0)  ),'yyyyMM'),'01') AS INT) """

sql_del_OneNab_SSC_AggInvBillBrandYM_INC = f"""DELETE FROM SSC_AggInvBillBrandYM 
                                                WHERE [DateKey] >= Cast (Concat(FORMAT((DATEADD(mm, DATEDIFF(mm, 0, getdate()) - 5, 0)  ),'yyyyMM'),'01') AS INT) """

sql_del_OneNab_SSC_AggInvByBrandYM_INC = f"""DELETE FROM SSC_AggInvByBrandYM 
                                                WHERE [DateKey] >= Cast (Concat(FORMAT((DATEADD(mm, DATEDIFF(mm, 0, getdate()) - 5, 0)  ),'yyyyMM'),'01') AS INT) """


sql_del_OneNab_SCC_AggActOutletYM = f"""delete from  SCC_AggActOutletYM 
                                        where [DateKey] >= Cast (Concat(FORMAT((DATEADD(mm, DATEDIFF(mm, 0, getdate()) - 0, 0)  ),'yyyyMM'),'01') AS INT)"""


with DAG(
    'Dag_OneNab_JobDailyAGG',
    start_date=pendulum.datetime(2022, 9, 5, 4, 50, 0, tz='Asia/Bangkok'),
    schedule_interval='30 15 * * *',
    catchup=False,
    tags=['ONE NAB', 'JobAllAOPDaily', 'spark'],
    default_args=default_args,
    render_template_as_native_obj=True,
    description='ONENAB_AllMaster',
    access_control={
        "TCCDE": {"can_read", "can_edit", "can_delete"}
    }
) as dag:

    base_config = {
        "executor_memory": "16g",
        "driver_memory": "5g",
        "total_executor_cores": 4,
        "executor_cores": 4,
        "env_vars": {
            "ONAB_MSSQL_URL": ONAB_MSSQL_URL,
            "ONAB_MSSQL_USER": ONAB_MSSQL_USER,
            "ONAB_MSSQL_PASSWORD": ONAB_MSSQL_PASSWORD,
            "ONABSTG_MSSQL_URL": ONABSTG_MSSQL_URL,
            "ONABSTG_MSSQL_USER": ONABSTG_MSSQL_USER,
            "ONABSTG_MSSQL_PASSWORD": ONABSTG_MSSQL_PASSWORD,
            "RTM_STG_PRD_TIDB_URL": RTM_STG_PRD_TIDB_URL,
            "RTM_STG_PRD_TIDB_URL_LOGS": RTM_STG_PRD_TIDB_URL_LOGS,
            "RTM_STG_PRD_TIDB_USER": RTM_STG_PRD_TIDB_USER,
            "RTM_STG_PRD_TIDB_PASSWORD": RTM_STG_PRD_TIDB_PASSWORD,
            "SCP_HOST": SCP_HOST,
            "SCP_USER": SCP_USER,
            "SCP_PASS": SCP_PASS
        }
    }

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    with TaskGroup("SCC_AggInvVolByBrandYM_INC", tooltip="SCC_AggInvVolByBrandYM_INC") as SCC_AggInvVolByBrandYM_INC:

        OneNab_SCC_AggInvVolByBrandYM_INC_Del = MySqlOperator(
            task_id='Delete_SCC_AggInvVolByBrandYM_INC', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SCC_AggInvVolByBrandYM_INC)

        OneNab_SCC_AggInvVolByBrandYM_INC_Load = SparkSubmitOperator(**base_config,
                                                                     task_id="Load_SCC_AggInvVolByBrandYM_INC", name='Load_SCC_AggInvVolByBrandYM_INC', conn_id='DE_SparkConn',
                                                                     application="./dags/onenab/TCCDE_OneNab/files/OneNab_SCC_AggInvVolByBrandYM_INC.py",
                                                                     conf={
                                                                         "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                                         "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                                     }
                                                                     )

        OneNab_SCC_AggInvVolByBrandYM_INC_Del >> OneNab_SCC_AggInvVolByBrandYM_INC_Load

    with TaskGroup("SSC_AggInvBillBrandYM_INC", tooltip="SSC_AggInvBillBrandYM_INC") as SSC_AggInvBillBrandYM_INC:

        OneNab_SSC_AggInvBillBrandYM_INC_Del = MySqlOperator(
            task_id='Delete_SSC_AggInvBillBrandYM_INC', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_AggInvBillBrandYM_INC)

        OneNab_SSC_AggInvBillBrandYM_INC_Load = SparkSubmitOperator(**base_config,
                                                                    task_id="Load_SSC_AggInvBillBrandYM_INC", name='Load_SSC_AggInvBillBrandYM_INC', conn_id='DE_SparkConn',
                                                                    application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_AggInvBillBrandYM_INC.py",
                                                                    conf={
                                                                        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                                        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                                    }
                                                                    )

        OneNab_SSC_AggInvBillBrandYM_INC_Del >> OneNab_SSC_AggInvBillBrandYM_INC_Load

    with TaskGroup("SSC_AggInvByBrandYM_INC", tooltip="SSC_AggInvByBrandYM_INC") as SSC_AggInvByBrandYM_INC:

        OneNab_SSC_AggInvByBrandYM_INC_Del = MySqlOperator(
            task_id='Delete_SSC_AggInvByBrandYM_INC', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_AggInvByBrandYM_INC)

        OneNab_SSC_AggInvByBrandYM_INC_Load = SparkSubmitOperator(**base_config,
                                                                  task_id="Load_SSC_AggInvByBrandYM_INC", name='Load_SSC_AggInvByBrandYM_INC', conn_id='DE_SparkConn',
                                                                  application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_AggInvByBrandYM_INC.py",
                                                                  conf={
                                                                      "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                                      "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                                  }
                                                                  )

        OneNab_SSC_AggInvByBrandYM_INC_Del >> OneNab_SSC_AggInvByBrandYM_INC_Load

    with TaskGroup("SCC_AggActOutletYM", tooltip="SCC_AggActOutletYM") as SCC_AggActOutletYM:

        OneNab_SCC_AggActOutletYM_Del = MySqlOperator(
            task_id='Delete_SCC_AggActOutletYM', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SCC_AggActOutletYM)

        OneNab_SCC_AggActOutletYM_Load = SparkSubmitOperator(**base_config,
                                                             task_id="Load_SCC_AggActOutletYM", name='Load_SCC_AggActOutletYM', conn_id='DE_SparkConn',
                                                             application="./dags/onenab/TCCDE_OneNab/files/OneNab_SCC_AggActOutletYM.py",
                                                             conf={
                                                                         "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                                         "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                             }
                                                             )

        OneNab_SCC_AggActOutletYM_Del >> OneNab_SCC_AggActOutletYM_Load

    chain(start,
          SCC_AggInvVolByBrandYM_INC,
          SSC_AggInvBillBrandYM_INC,
          SSC_AggInvByBrandYM_INC,
          SCC_AggActOutletYM,
          end)
