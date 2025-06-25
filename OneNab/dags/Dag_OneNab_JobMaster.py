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
    "owner": "TCCDE_team",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# [Get secrets from Airflow variables]
secrets = Variable.get("TCCDE_SalesETL_SECRET", deserialize_json=True)
secrets_scp = Variable.get("TCCDE_SalesETL_SECRET_SCP", deserialize_json=True)


ONAB_MSSQL_URL = secrets["ONAB_MSSQL_URL"]
ONAB_MSSQL_USER = secrets["ONAB_MSSQL_USER"]
ONAB_MSSQL_PASSWORD = secrets["ONAB_MSSQL_PASSWORD"]

ONABSTG_MSSQL_URL = secrets["ONABSTG_MSSQL_URL"]
ONABSTG_MSSQL_USER = secrets["ONABSTG_MSSQL_USER"]
ONABSTG_MSSQL_PASSWORD = secrets["ONABSTG_MSSQL_PASSWORD"]


RTM_STG_PRD_TIDB_URL = secrets["RTM_STG_PRD_TIDB_URL"]
RTM_STG_PRD_TIDB_URL_LOGS = secrets["RTM_STG_PRD_TIDB_URL_LOGS"]
RTM_STG_PRD_TIDB_USER = secrets["RTM_STG_PRD_TIDB_USER"]
RTM_STG_PRD_TIDB_PASSWORD = secrets["RTM_STG_PRD_TIDB_PASSWORD"]


SCP_HOST = secrets_scp["SCP_HOST"]
SCP_USER = secrets_scp["SCP_USER"]
SCP_PASS = secrets_scp["SCP_PASS"]


sql_del_OneNab_SSC_DimChannel = f"TRUNCATE TABLE SSC_DimChannel"
sql_del_OneNab_SSC_DimCustomer = f"TRUNCATE TABLE SSC_DimCustomer"
sql_del_OneNab_SSC_DimSalesman = f"TRUNCATE TABLE SSC_DimSalesman"
sql_del_OneNab_SSC_DimSalesmanExcel = f"TRUNCATE TABLE SSC_DimSalesmanExcel"
sql_del_OneNab_SSC_DimLocation = f"TRUNCATE TABLE SSC_DimLocation"
sql_del_OneNab_SSC_DimLocationExcel = f"TRUNCATE TABLE SSC_DimLocationExcel"
sql_del_OneNab_SSC_DimRoute = f"TRUNCATE TABLE SSC_DimRoute"
sql_del_OneNab_SSC_DimRouteExcel = f"TRUNCATE TABLE SSC_DimRouteExcel"
sql_del_OneNab_SSC_AggVisitCallYM_INC = f"""DELETE FROM SSC_AggVisitCallYM WHERE [DateKey] >= Cast (Concat(FORMAT((DATEADD(mm, DATEDIFF(mm, 0, getdate()) - 2, 0)  ),'yyyyMM'),'01') AS INT) """
sql_del_OneNab_SSC_AggInvAllDataYM = f"TRUNCATE TABLE SSC_AggInvAllDataYM"
sql_del_OneNab_SSC_AggDataAssetYM = f"TRUNCATE TABLE SSC_AggDataAssetYM"
sql_del_OneNab_SCC_AggAssetCoolerYM = f"TRUNCATE TABLE SCC_AggAssetCoolerYM"

sql_del_OneNab_SSC_FactVisitListINCn = f"""DELETE FROM SSC_FactVisitList WHERE ETLLoadData >= DATEADD(day, -1, CAST(GETDATE() AS date)) """
sql_del_OneNab_SSC_FactActualVisitINC = f"""DELETE FROM SSC_FactActualVisit WHERE ETLLoadData >= DATEADD(day, -1, CAST(GETDATE() AS date)) """
sql_del_OneNab_SSC_FactDataAssetINC = f"""DELETE FROM SSC_FactDataAsset WHERE ETLLoadData >= DATEADD(day, -1, CAST(GETDATE() AS date)) """


sql_del_OneNab_SSC_FactRouteSalesman = """DELETE SSC_FactRouteSalesman FROM SSC_FactRouteSalesman INNER JOIN [dbo].[SSC_DimRouteExcel] ON SSC_FactRouteSalesman.YM = [dbo].[SSC_DimRouteExcel].YM"""


with DAG(
    "Dag_ONENAB_AllMasterData",
    start_date=pendulum.datetime(2022, 9, 5, 4, 50, 0, tz='Asia/Bangkok'),
    schedule_interval="0 4 * * *",
    catchup=False,
    tags=["ONE NAB", "ONENAB_AllMaster", "spark", "1.0.0"],
    default_args=default_args,
    render_template_as_native_obj=True,
    description="ONENAB_AllMaster",
    access_control={"TCCDE": {"can_read", "can_edit", "can_delete"}},
) as dag:

    base_config = {
        "executor_memory": "16g",
        "driver_memory": "16g",
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
            "SCP_PASS": SCP_PASS,
        },
    }

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # [Start Raw_Stage task group]

    with TaskGroup("SSC_AllMasterData", tooltip="SSC_AllMasterData") as SSC_AllMaster:

        print("SCP_HOST {SCP_HOST}")
        print("SCP_USER {SCP_USER}")
        print("SCP_PASS {SCP_PASS}")

        print("base_config {base_config}")

        task1_del = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimBillingDate",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimBillingDate",
        )

        task1_de2 = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimBillingDateExcel",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimBillingDateExcel",
        )

        task1_de3 = MySqlOperator(
            task_id="Delete_OneNab_SSC_TEMPDimBillingDate",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_TEMPDimBillingDate",
        )

        task1_load_excel = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimBillingDateExcel",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimBillingDateExcel.py",
        )

        Delete_OneNab_SSC_DimChannel = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimChannel",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimChannel",
        )

        Load_OneNab_SSC_DimChannel = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimChannel",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimChannel.py",
        )

        Delete_OneNab_SSC_DimCustGrpSap = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimCustGrpSap",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimCustGrpSap",
        )

        Load_OneNab_SSC_DimCustGrpSap = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimCustGrpSap",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimCustGrpSap.py",
        )

        Delete_OneNab_SSC_DimReason = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimReason",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimReason",
        )

        Load_OneNab_SSC_DimReason = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimReason",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimReason.py",
        )

        Delete_OneNab_SSC_DimCustGrp1 = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimCustGrp1",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimCustGrp1",
        )

        Load_OneNab_SSC_DimCustGrp1 = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimCustGrp1",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimCustGrp1.py",
        )

        Delete_OneNab_SSC_DimPromotion = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimPromotion",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimPromotion",
        )

        Load_OneNab_SSC_DimPromotion = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimPromotion",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimPromotion.py",
        )

        Delete_OneNab_SSC_DimRoute = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimRoute",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimRoute",
        )

        Load_OneNab_SSC_DimRoute = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimRoute",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimRoute.py",
        )

        Delete_OneNab_SSC_DimCustomerOld = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimCustomerOld",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimCustomerOld",
        )

        Load_OneNab_SSC_DimCustomerOld = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimCustomerOld",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimCustomerOld.py",
        )

        Delete_OneNab_SSC_DimMaterial = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimMaterial",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimMaterial",
        )

        Load_OneNab_SSC_DimMaterial = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimMaterial",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimMaterial.py",
        )

        Delete_OneNab_SSC_DimMaterialOld = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimMaterialOld",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_DimMaterialOld",
        )

        Load_OneNab_SSC_DimMaterialOld = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimMaterialOld",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimMaterialOld.py",
        )

        Delete_OneNab_SSC_FactCustPerMonPlan = MySqlOperator(
            task_id="Delete_OneNab_SSC_FactCustPerMonPlan",
            mysql_conn_id="TCCDE_ONENABCon",
            sql="TRUNCATE TABLE SSC_FactCustPerMonPlan",
        )

        Load_OneNab_SSC_FactCustPerMonPlan = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_FactCustPerMonPlan",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactCustPerMonPlan.py",
        )

        task1_del >> task1_de2 >> task1_de3 >> task1_load_excel

        Delete_OneNab_SSC_DimChannel >> Load_OneNab_SSC_DimChannel
        Delete_OneNab_SSC_DimCustGrpSap >> Load_OneNab_SSC_DimCustGrpSap
        Delete_OneNab_SSC_DimReason >> Load_OneNab_SSC_DimReason
        Delete_OneNab_SSC_DimCustGrp1 >> Load_OneNab_SSC_DimCustGrp1
        Delete_OneNab_SSC_DimPromotion >> Load_OneNab_SSC_DimPromotion
        Delete_OneNab_SSC_DimRoute >> Load_OneNab_SSC_DimRoute
        Delete_OneNab_SSC_DimCustomerOld >> Load_OneNab_SSC_DimCustomerOld
        Delete_OneNab_SSC_DimMaterial >> Load_OneNab_SSC_DimMaterial
        Delete_OneNab_SSC_DimMaterialOld >> Load_OneNab_SSC_DimMaterialOld
        Delete_OneNab_SSC_FactCustPerMonPlan >> Load_OneNab_SSC_FactCustPerMonPlan

    with TaskGroup("SSC_DimCustomer", tooltip="SSC_DimCustomer") as SSC_DimCustomer:

        OneNab_SSC_DimCustomer_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimCustomer",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_DimCustomer,
        )

        OneNab_SSC_DimCustomer_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimCustomer",
            name="Load_OneNab_SSC_DimCustomer",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimCustomer.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        OneNab_SSC_DimCustomer_Del >> OneNab_SSC_DimCustomer_Load

    with TaskGroup("SSC_DimSalesman", tooltip="SSC_DimSalesman") as SSC_DimSalesman:

        OneNab_SSC_DimSalesman_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimSalesman",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_DimSalesman,
        )

        OneNab_SSC_DimSalesmanExcel_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimSalesmanExcel",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_DimSalesmanExcel,
        )

        OneNab_SSC_DimSalesman_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimSalesman",
            name="Load_OneNab_SSC_DimSalesman",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimSalesman.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        (
            OneNab_SSC_DimSalesman_Del
            >> OneNab_SSC_DimSalesmanExcel_Del
            >> OneNab_SSC_DimSalesman_Load
        )

    with TaskGroup("SSC_DimLocation", tooltip="SSC_DimLocation") as SSC_DimLocation:

        OneNab_SSC_DimLocation_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimLocation",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_DimLocation,
        )

        OneNab_SSC_DimLocationExcel_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimLocationExcel",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_DimLocationExcel,
        )

        OneNab_SSC_DimLocation_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimLocation",
            name="Load_OneNab_SSC_DimLocation",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimLocation.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        (
            OneNab_SSC_DimLocation_Del
            >> OneNab_SSC_DimLocationExcel_Del
            >> OneNab_SSC_DimLocation_Load
        )

    with TaskGroup("SSC_DimRoute", tooltip="SSC_DimRoute") as SSC_DimRoute:

        OneNab_SSC_DimRoute_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_DimRoute",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_DimRoute,
        )

        OneNab_SSC_DimRouteExcel_Del = MySqlOperator(
            task_id="Delete_OneNab_SSSC_DimRouteExcel",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_DimRouteExcel,
        )

        OneNab_SSC_FactRouteSalesman_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_FactRouteSalesman",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_FactRouteSalesman,
        )

        OneNab_SSC_DimRoute_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_DimRoute",
            name="Load_OneNab_SSC_DimRoute",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_DimRoute.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        (
            OneNab_SSC_DimRoute_Del
            >> OneNab_SSC_DimRouteExcel_Del
            >> OneNab_SSC_FactRouteSalesman_Del
            >> OneNab_SSC_DimRoute_Load
        )

    with TaskGroup(
        "SSC_FactVisitListINC", tooltip="SSC_FactVisitListINC"
    ) as SSC_FactVisitListINC:

        OneNab_SSC_FactVisitListINC_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_FactVisitListINC",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_FactVisitListINCn,
        )

        OneNab_SSC_FactVisitListINC_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_FactVisitListINC",
            name="Load_OneNab_SSC_FactVisitListINC",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactVisitListINC.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        OneNab_SSC_FactVisitListINC_Del >> OneNab_SSC_FactVisitListINC_Load

    with TaskGroup(
        "SSC_FactActualVisitINC", tooltip="SSC_FactActualVisitINC"
    ) as SSC_FactActualVisitINC:

        OneNab_SSC_FactActualVisitINC_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_FactActualVisitINC",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_FactActualVisitINC,
        )

        OneNab_SSC_SSC_FactActualVisitINC_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_FactActualVisitINC",
            name="Load_OneNab_SSC_FactActualVisitINC",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactActualVisitINC.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        OneNab_SSC_FactActualVisitINC_Del >> OneNab_SSC_SSC_FactActualVisitINC_Load

    with TaskGroup(
        "SSC_AggVisitCallYM_INC", tooltip="SSC_AggVisitCallYM_INC"
    ) as SSC_AggVisitCallYM_INC:

        OneNab_SSC_AggVisitCallYM_INC_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_AggVisitCallYM_INC",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_AggVisitCallYM_INC,
        )

        OneNab_SSC_AggVisitCallYM_INC_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_AggVisitCallYM_INC",
            name="Load_OneNab_SSC_AggVisitCallYM_INC",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_AggVisitCallYM_INC.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        OneNab_SSC_AggVisitCallYM_INC_Del >> OneNab_SSC_AggVisitCallYM_INC_Load

    with TaskGroup(
        "SSC_AggInvAllDataYM", tooltip="SSC_AggInvAllDataYM"
    ) as SSC_AggInvAllDataYM:

        OneNab_SSC_AggInvAllDataYM_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_AggInvAllDataYM",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_AggInvAllDataYM,
        )

        OneNab_SSC_AggInvAllDataYM_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_AggInvAllDataYM",
            name="Load_OneNab_SSC_AggInvAllDataYM",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_AggInvAllDataYM.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        OneNab_SSC_AggInvAllDataYM_Del >> OneNab_SSC_AggInvAllDataYM_Load

    with TaskGroup(
        "SSC_FactDataAssetINC", tooltip="SSC_FactDataAssetINC"
    ) as SSC_FactDataAssetINC:

        OneNab_SSC_FactDataAssetINC_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_FactDataAssetINC",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_FactDataAssetINC,
        )

        OneNab_SSC_FactDataAssetINC_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_FactDataAssetINC",
            name="Load_OneNab_SSC_FactDataAssetINC",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_FactDataAssetINC.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        OneNab_SSC_FactDataAssetINC_Del >> OneNab_SSC_FactDataAssetINC_Load

    with TaskGroup(
        "SSC_AggDataAssetYM", tooltip="SSC_AggDataAssetYM"
    ) as SSC_AggDataAssetYM:

        OneNab_SSC_AggDataAssetYM_Del = MySqlOperator(
            task_id="Delete_OneNab_SSC_AggDataAssetYM",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SSC_AggDataAssetYM,
        )

        OneNab_SSC_AggDataAssetYM_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SSC_AggDataAssetYM",
            name="Load_OneNab_SSC_AggDataAssetYM",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SSC_AggDataAssetYM.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        OneNab_SSC_AggDataAssetYM_Del >> OneNab_SSC_AggDataAssetYM_Load

    with TaskGroup(
        "SCC_AggAssetCoolerYM", tooltip="SCC_AggAssetCoolerYM"
    ) as SCC_AggAssetCoolerYM:

        OneNab_SCC_AggAssetCoolerYM_Del = MySqlOperator(
            task_id="Delete_OneNab_SCC_AggAssetCoolerYM",
            mysql_conn_id="TCCDE_ONENABCon",
            sql=sql_del_OneNab_SCC_AggAssetCoolerYM,
        )

        OneNab_SCC_AggAssetCoolerYM_Load = SparkSubmitOperator(
            **base_config,
            task_id="Load_OneNab_SCC_AggAssetCoolerYM",
            name="Load_OneNab_SCC_AggAssetCoolerYM",
            conn_id="DE_SparkConn",
            application="./dags/onenab/TCCDE_OneNab/files/OneNab_SCC_AggAssetCoolerYM.py",
            conf={
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            },
        )

        OneNab_SCC_AggAssetCoolerYM_Del >> OneNab_SCC_AggAssetCoolerYM_Load

    chain(
        start,
        SSC_AllMaster,
        SSC_DimCustomer,
        SSC_DimSalesman,
        SSC_DimLocation,
        SSC_DimRoute,
        SSC_FactVisitListINC,
        SSC_FactActualVisitINC,
        SSC_AggVisitCallYM_INC,
        SSC_AggInvAllDataYM,
        SSC_FactDataAssetINC,
        SSC_AggDataAssetYM,
        SCC_AggAssetCoolerYM,
        end,
    )
