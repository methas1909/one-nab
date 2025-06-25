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


sql_del_OneNab_SSC_FactAOPDM = """ DELETE ONENAB.dbo.SSC_FactAOPDM 
                                    FROM ONENAB.dbo.SSC_FactAOPDM
                                    INNER JOIN ONENAB.dbo.SSC_FactAOPDMExcel 
                                        ON (ONENAB.dbo.SSC_FactAOPDM.YM = ONENAB.dbo.SSC_FactAOPDMExcel.YM)
                                    """

sql_del_OneNab_SSC_FactAOPRoute = """
                                        DELETE FROM SSC_FactAOPRoute
                                        WHERE EXISTS (
                                            SELECT 1 FROM SSC_FactAOPRouteExcel
                                            WHERE SSC_FactAOPRoute.YM = SSC_FactAOPRouteExcel.YM
                                        )
                                        """
sql_del_OneNab_SSC_FactAOPRouteExcel = f"TRUNCATE TABLE SSC_FactAOPRouteExcel"
sql_del_OneNab_SSC_FactAOPDMExcel = f"TRUNCATE TABLE SSC_FactAOPDMExcel"

sql_del_OneNab_SSC_FactAOPNDTDMExcel = f"TRUNCATE TABLE SSC_FactAOPNDTDM"
sql_del_OneNab_SSC_FactAOPNDTDM = f"""DELETE SSC_FactAOPNDTDM 
                                            FROM SSC_FactAOPNDTDM 
                                            INNER JOIN [SSC_FactAOPNDTDMExcel] 
                                            ON [SSC_FactAOPNDTDM].YM = FORMAT([StartDate],'yyyyMM')"""


sql_del_OneNab_SSC_FactAOP = f"""DELETE SSC_FactAOP 
                                    FROM SSC_FactAOP 
                                    INNER JOIN [SSC_FactAOPExcel] 
                                    ON (SSC_FactAOP.YM =[SSC_FactAOPExcel].YM)"""

sql_del_OneNab_SSC_FactAOPExcel = f"TRUNCATE TABLE SSC_FactAOPExcel"


sql_del_OneNab_SSC_FactAOPNDDM = """DELETE SSC_FactAOPNDDM 
                                    FROM SSC_FactAOPNDDM 
                                    INNER JOIN [SSC_FactAOPNDDMExcel] 
                                    ON (SSC_FactAOPNDDM.YM =[SSC_FactAOPNDDMExcel].YM)"""

sql_del_OneNab_SSC_FactAOPNDDMExcel = f"TRUNCATE TABLE SSC_FactAOPNDDMExcel"


sql_del_OneNab_SSC_FactAOPNDRoute = f""" DELETE SSC_FactAOPNDRoute FROM SSC_FactAOPNDRoute INNER JOIN SSC_FactAOPNDRouteExcel ON (SSC_FactAOPNDRoute.YM =SSC_FactAOPNDRouteExcel.YM) """
sql_del_OneNab_SSC_FactAOPNDRouteExcel = f"TRUNCATE TABLE SSC_FactAOPNDRouteExcel"

sql_del_OneNab_SSC_FactAOPTDMExcel = f"TRUNCATE TABLE SSC_FactAOPTDMExcel"

sql_del_OneNab_SSC_FactAOPTDM = """
                                DELETE SSC_FactAOPTDM 
                                FROM SSC_FactAOPTDM 
                                INNER JOIN [SSC_FactAOPTDMExcel] 
                                ON SSC_FactAOPTDM.YM = FORMAT(CAST([StartDate] AS datetime), 'yyyyMM') """


with DAG(
    'Dag_OneNab_JobAllAOPDaily',
    start_date=pendulum.datetime(2023, 1, 24, 4, 45, 0, tz='Asia/Bangkok'),
    schedule_interval='00 4 * * *',
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

    # [Start Raw_Stage task group]

    with TaskGroup("SSC_FactAOPDM", tooltip="SSC_FactAOPDM") as SSC_FactAOPDM:

        OneNab_SSC_FactAOPDM_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPDM', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPDM)

        OneNab_SSC_FactAOPDMExcel_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPDMExcel', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPDMExcel)

        OneNab_SSC_FactAOPDM_Load = SparkSubmitOperator(**base_config,
                                                        task_id="Load_OneNab_SSC_FactAOPDM", name='Load_OneNab_SSC_FactAOPDM', conn_id='DE_SparkConn',
                                                        application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactAOPDM.py",
                                                        conf={
                                                            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                        }
                                                        )

        OneNab_SSC_FactAOPDM_Del >> OneNab_SSC_FactAOPDMExcel_Del >> OneNab_SSC_FactAOPDM_Load

    with TaskGroup("SSC_FactAOPRoute", tooltip="SSC_FactAOPRoute") as SSC_FactAOPRoute:

        OneNab_SSC_FactAOPRoute_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPRoute', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPRoute)

        OneNab_SSC_FactAOPRouteExcel_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPRouteExcel', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPRouteExcel)

        OneNab_SSC_FactAOPRoute_Load = SparkSubmitOperator(**base_config,
                                                           task_id="Load_OneNab_SSC_FactAOPRoute", name='Load_OneNab_SSC_FactAOPRoute', conn_id='DE_SparkConn',
                                                           application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactAOPRoute.py",
                                                           conf={
                                                                   "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                                   "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                           }
                                                           )

        OneNab_SSC_FactAOPRoute_Del >> OneNab_SSC_FactAOPRouteExcel_Del >> OneNab_SSC_FactAOPRoute_Load

    with TaskGroup("SSC_FactAOP", tooltip="SSC_FactAOP") as SSC_FactAOP:

        OneNab_SSC_FactAOP_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOP', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOP)

        OneNab_SSSC_FactAOPExcel_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPExcel', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPExcel)

        OneNab_SSC_FactAOP_Load = SparkSubmitOperator(**base_config,
                                                      task_id="Load_OneNab_SSC_FactAOP", name='Load_OneNab_SSC_FactAOP', conn_id='DE_SparkConn',
                                                      application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactAOP.py",
                                                      conf={
                                                          "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                          "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                      }
                                                      )
        OneNab_SSC_FactAOP_Del >> OneNab_SSSC_FactAOPExcel_Del >> OneNab_SSC_FactAOP_Load

    with TaskGroup("SSC_FactAOPNDDM", tooltip="SSC_FactAOPNDDM") as SSC_FactAOPNDDM:

        OneNab_SSC_FactAOPNDDM_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPNDDM', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPNDDM)

        OneNab_SSSC_FactAOPNDDMExcel_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPNDDMExcel', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPNDDMExcel)

        OneNab_SSC_FactAOPNDDM_Load = SparkSubmitOperator(**base_config,
                                                          task_id="Load_OneNab_SSC_FactAOPNDDM", name='Load_OneNab_SSC_FactAOPNDDM', conn_id='DE_SparkConn',
                                                          application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactAOPNDDM.py",
                                                          conf={
                                                              "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                              "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                          }
                                                          )

        OneNab_SSC_FactAOPNDDM_Del >> OneNab_SSSC_FactAOPNDDMExcel_Del >> OneNab_SSC_FactAOPNDDM_Load

    with TaskGroup("SSC_FactAOPNDRoute", tooltip="SSC_FactAOPNDRoute") as SSC_FactAOPNDRoute:

        OneNab_SSC_FactAOPNDRoute_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPNDRoute', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPNDRoute)

        OneNab_SSSC_FactAOPNDRouteExcel_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPNDRouteExcel', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPNDRouteExcel)

        OneNab_SSC_FactAOPNDRoute_Load = SparkSubmitOperator(**base_config,
                                                             task_id="Load_OneNab_SSC_FactAOPNDRoute", name='Load_OneNab_SSC_FactAOPNDRoute', conn_id='DE_SparkConn',
                                                             application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactAOPNDRoute.py",
                                                             conf={
                                                                 "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                                 "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                             }
                                                             )

        OneNab_SSC_FactAOPNDRoute_Del >> OneNab_SSSC_FactAOPNDRouteExcel_Del >> OneNab_SSC_FactAOPNDRoute_Load

    with TaskGroup("SSC_FactAOPTDM", tooltip="SSC_FactAOPTDM") as SSC_FactAOPTDM:

        OneNab_SSC_FactAOPTDM_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPTDM', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPTDM)

        OneNab_SSSC_FactAOPTDMExcel_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPTDMExcel', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPTDMExcel)

        OneNab_SSC_FactAOPTDM_Load = SparkSubmitOperator(**base_config,
                                                         task_id="Load_OneNab_SSC_FactAOPTDM", name='Load_OneNab_SSC_FactAOPTDM', conn_id='DE_SparkConn',
                                                         application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactAOPTDM.py",
                                                         conf={
                                                             "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                             "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                         }
                                                         )

        OneNab_SSC_FactAOPTDM_Del >> OneNab_SSSC_FactAOPTDMExcel_Del >> OneNab_SSC_FactAOPTDM_Load

    with TaskGroup("SSC_FactAOPNDTDM", tooltip="SSC_FactAOPNDTDM") as SSC_FactAOPNDTDM:

        OneNab_SSC_FactAOPNDTDM_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPNDTDM', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPNDTDM)

        OneNab_SSC_FactAOPNDTDMExcel_Del = MySqlOperator(
            task_id='Delete_OneNab_SSC_FactAOPNDTDMExcel', mysql_conn_id='TCCDE_ONENABCon', sql=sql_del_OneNab_SSC_FactAOPNDTDMExcel)

        OneNab_SSC_FactAOPNDTDM_Load = SparkSubmitOperator(**base_config,
                                                           task_id="Load_OneNab_SSC_FactAOPNDTDM", name='Load_OneNab_SSC_FactAOPNDTDM', conn_id='DE_SparkConn',
                                                           application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactAOPNDTDM.py",
                                                           conf={
                                                                   "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                                                                   "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
                                                           }
                                                           )

        OneNab_SSC_FactAOPNDTDM_Del >> OneNab_SSC_FactAOPNDTDMExcel_Del >> OneNab_SSC_FactAOPNDTDM_Load

    chain(start, SSC_FactAOPDM, SSC_FactAOPRoute, SSC_FactAOPTDM, SSC_FactAOP,
          SSC_FactAOPNDDM, SSC_FactAOPNDRoute, SSC_FactAOPNDTDM, end)
