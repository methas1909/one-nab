# from asyncore import read
# from airflow.models import Variable
# import pendulum
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from pyspark.sql import SparkSession
# from airflow.operators.mysql_operator import MySqlOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.utils.task_group import TaskGroup
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.operators.python import PythonOperator
# from airflow.models.baseoperator import chain
# from airflow.utils.dates import days_ago
# from airflow import DAG
# from airflow.operators.email_operator import EmailOperator
# from datetime import datetime, timedelta

# default_args = {
#     'owner' : 'TCCDE_team',
#     'retries': 5, 
#     'retry_delay': timedelta(minutes=5),
# }

# # [Get secrets from Airflow variables]
# secrets = Variable.get('TCCDE_SalesETL_SECRET', deserialize_json=True)
# ONAB_MSSQL_URL = secrets['ONAB_MSSQL_URL']
# ONAB_MSSQL_USER = secrets['ONAB_MSSQL_USER']
# ONAB_MSSQL_PASSWORD = secrets['ONAB_MSSQL_PASSWORD']

# RTM_STG_PRD_TIDB_URL = secrets['RTM_STG_PRD_TIDB_URL']
# RTM_STG_PRD_TIDB_URL_LOGS = secrets['RTM_STG_PRD_TIDB_URL_LOGS']
# RTM_STG_PRD_TIDB_USER = secrets['RTM_STG_PRD_TIDB_USER']
# RTM_STG_PRD_TIDB_PASSWORD = secrets['RTM_STG_PRD_TIDB_PASSWORD']


# # [Start assigning the date for quering]
# # START_DATE = '{{ dag_run.conf.get("start_date", (execution_date - macros.timedelta(days=62)).replace(day=1)| ds ) }}'
# # END_DATE = '{{ dag_run.conf.get("end_date", (((execution_date).replace(day=1) + macros.timedelta(days=32)).replace(day=1) - macros.timedelta(days=1)) | ds ) }}'

# # [Start assigning the date to sql query for raw stage]
# # sql_raw = f"DELETE FROM RawST_Sales_Data_SAP WHERE CAST(CAST(POSTING_DATE as char(8)) as date) BETWEEN '{START_DATE}' AND '{END_DATE}'"

# sql_del_Sales_ONENAB_SSC_DimChannel = f"TRUNCATE TABLE Sales_ONENAB_SSC_DimChannel"
# sql_del_Sales_ONENAB_SSC_DimCustGrp1 = f"TRUNCATE TABLE Sales_ONENAB_SSC_DimCustGrp1"
# sql_del_Sales_ONENAB_SSC_DimCustGrpSap = f"TRUNCATE TABLE Sales_ONENAB_SSC_DimCustGrpSap"
# sql_del_Sales_ONENAB_SSC_DimCustomer = f"TRUNCATE TABLE Sales_ONENAB_SSC_DimCustomer"
# sql_del_Sales_ONENAB_SSC_DimLocation = f"TRUNCATE TABLE Sales_ONENAB_SSC_DimLocation"
# sql_del_Sales_ONENAB_SSC_DimMaterial = f"TRUNCATE TABLE Sales_ONENAB_SSC_DimMaterial"
# sql_del_Sales_ONENAB_SSC_DimSalesman = f"TRUNCATE TABLE Sales_ONENAB_SSC_DimSalesman"
# # sql_del_Sales_ONENAB_SSC_AggInvByBrandYM = f"DELETE  FROM Sales_ONENAB_SSC_AggInvByBrandYM WHERE PYLoadDate >= DATE_ADD(CURDATE(), INTERVAL -3 DAY)"
# sql_del_Sales_ONENAB_SSC_AggInvByBrandYM = f"TRUNCATE TABLE Sales_ONENAB_SSC_AggInvByBrandYM"
# sql_del_Sales_ONENAB_SSC_FactRouteSalesman = f"TRUNCATE TABLE Sales_ONENAB_SSC_FactRouteSalesman"
# sql_del_Sales_ONENAB_SSC_DimRoute = f"TRUNCATE TABLE Sales_ONENAB_SSC_DimRoute"


# # [End assigning the date to sql query for raw stage]

# # [Add access_control for the team as line 53 - line 55]
# with DAG(
#     'Dag_RTMSTG_OneNab_Master_PRD_26',
#     start_date= pendulum.datetime(2023, 4, 25, 3, 00, 0, tz='Asia/Bangkok'),
#     schedule_interval='00 6 * * *',
#     catchup=False,
#     tags=['RTMSTG','SIN AllMaster', 'spark'],
#     default_args=default_args,
#     render_template_as_native_obj=True,
#     description='SIN All Master loading from SIN to RTM_STAGING',
#     access_control={
#         "TCCDE":{"can_read", "can_edit", "can_delete"}
#     }
# ) as dag:

#    
#     base_config = {
#         "executor_memory": "8g",
#         "driver_memory": "4g",
#         "total_executor_cores": 5,
#         "executor_cores": 5,
#         "env_vars": {
#             "ONAB_MSSQL_URL": ONAB_MSSQL_URL,
#             "ONAB_MSSQL_USER": ONAB_MSSQL_USER,
#             "ONAB_MSSQL_PASSWORD": ONAB_MSSQL_PASSWORD,
#             "RTM_STG_PRD_TIDB_URL": RTM_STG_PRD_TIDB_URL,
#             "RTM_STG_PRD_TIDB_URL_LOGS": RTM_STG_PRD_TIDB_URL_LOGS,
#             "RTM_STG_PRD_TIDB_USER": RTM_STG_PRD_TIDB_USER,
#             "RTM_STG_PRD_TIDB_PASSWORD": RTM_STG_PRD_TIDB_PASSWORD
#         }
#     }

#     start = EmptyOperator(task_id='start')
#     end = EmptyOperator(task_id='end')

#     # [Start Raw_Stage task group]
#     with TaskGroup('RTM_Staging_OneNab_Master', tooltip='Task for RTM_Staging_OneNab_Master') as RTM_Staging_OneNab_Master:
#         # Start Sub ------
#         Sales_ONENAB_SSC_DimChannel_Del = MySqlOperator(
#             task_id='Delete_ONENAB_SSC_DimChannel', mysql_conn_id='TCCDE_RTMStagePRDCon', sql=sql_del_Sales_ONENAB_SSC_DimChannel)

#         Sales_ONENAB_SSC_DimChannel_Load = SparkSubmitOperator(**base_config,
#                                                   task_id="Load_Sales_ONENAB_SSC_DimChannel", name='Load_Sales_ONENAB_SSC_DimChannel', conn_id='DE_SparkConn',
#                                                   application="./dags/onenab/TCCDE_OneNab/files/RTMSTG_Sales_ONENAB_SSC_DimChannel.py",
#                                                   conf={
#                                                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#                                                       "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
#                                                   }
#                                                   )
#         # End Sub ------  


#         # Start Sub ------
#         Sales_ONENAB_SSC_DimCustGrp1_Del = MySqlOperator(
#             task_id='Delete_Sales_ONENAB_SSC_DimCustGrp1', mysql_conn_id='TCCDE_RTMStagePRDCon', sql=sql_del_Sales_ONENAB_SSC_DimCustGrp1)

#         Sales_ONENAB_SSC_DimCustGrp1_Load = SparkSubmitOperator(**base_config,
#                                                   task_id="Load_Sales_ONENAB_SSC_DimCustGrp1", name='Load_Sales_ONENAB_SSC_DimCustGrp1', conn_id='DE_SparkConn',
#                                                   application="./dags/onenab/TCCDE_OneNab/files/RTMSTG_Sales_ONENAB_SSC_DimCustGrp1.py",
#                                                   conf={
#                                                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#                                                       "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
#                                                   }
#                                                   )
#         # End Sub ------  

#         # Start Sub ------
#         Sales_ONENAB_SSC_DimCustGrpSap_Del = MySqlOperator(
#             task_id='Delete_Sales_ONENAB_SSC_DimCustGrpSap', mysql_conn_id='TCCDE_RTMStagePRDCon', sql=sql_del_Sales_ONENAB_SSC_DimCustGrpSap)

#         Sales_ONENAB_SSC_DimCustGrpSap_Load = SparkSubmitOperator(**base_config,
#                                                   task_id="Load_Sales_ONENAB_SSC_DimCustGrpSap", name='Load_Sales_ONENAB_SSC_DimCustGrpSap', conn_id='DE_SparkConn',
#                                                   application="./dags/onenab/TCCDE_OneNab/files/RTMSTG_Sales_ONENAB_SSC_DimCustGrpSap.py",
#                                                   conf={
#                                                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#                                                       "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
#                                                   }
#                                                   )
#         # End Sub ------  

#         # Start Sub ------
#         Sales_ONENAB_SSC_DimCustomer_Del = MySqlOperator(
#             task_id='Delete_Sales_ONENAB_SSC_DimCustomer', mysql_conn_id='TCCDE_RTMStagePRDCon', sql=sql_del_Sales_ONENAB_SSC_DimCustomer)

#         Sales_ONENAB_SSC_DimCustomer_Load = SparkSubmitOperator(**base_config,
#                                                   task_id="Load_Sales_ONENAB_SSC_DimCustomer", name='Load_Sales_ONENAB_SSC_DimCustomer', conn_id='DE_SparkConn',
#                                                   application="./dags/onenab/TCCDE_OneNab/files/RTMSTG_Sales_ONENAB_SSC_DimCustomer.py",
#                                                   conf={
#                                                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#                                                       "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
#                                                   }
#                                                   )
#         # End Sub ------  

#         # Start Sub ------
#         Sales_ONENAB_SSC_DimLocation_Del = MySqlOperator(
#             task_id='Delete_Sales_ONENAB_SSC_DimLocation', mysql_conn_id='TCCDE_RTMStagePRDCon', sql=sql_del_Sales_ONENAB_SSC_DimLocation)

#         Sales_ONENAB_SSC_DimLocation_Load = SparkSubmitOperator(**base_config,
#                                                   task_id="Load_Sales_ONENAB_SSC_DimLocation", name='Load_Sales_ONENAB_SSC_DimLocation', conn_id='DE_SparkConn',
#                                                   application="./dags/onenab/TCCDE_OneNab/files/RTMSTG_Sales_ONENAB_SSC_DimLocation.py",
#                                                   conf={
#                                                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#                                                       "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
#                                                   }
#                                                   )
#         # End Sub ------  

#         # Start Sub ------
#         Sales_ONENAB_SSC_DimMaterial_Del = MySqlOperator(
#             task_id='Delete_Sales_ONENAB_SSC_DimMaterial', mysql_conn_id='TCCDE_RTMStagePRDCon', sql=sql_del_Sales_ONENAB_SSC_DimMaterial)

#         Sales_ONENAB_SSC_DimMaterial_Load = SparkSubmitOperator(**base_config,
#                                                   task_id="Load_Sales_ONENAB_SSC_DimMaterial", name='Load_Sales_ONENAB_SSC_DimMaterial', conn_id='DE_SparkConn',
#                                                   application="./dags/onenab/TCCDE_OneNab/files/RTMSTG_Sales_ONENAB_SSC_DimMaterial.py",
#                                                   conf={
#                                                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#                                                       "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
#                                                   }
#                                                   )
#         # End Sub ------  

#         # Start Sub ------
#         Sales_ONENAB_SSC_DimSalesman_Del = MySqlOperator(
#             task_id='Delete_Sales_ONENAB_SSC_DimSalesman', mysql_conn_id='TCCDE_RTMStagePRDCon', sql=sql_del_Sales_ONENAB_SSC_DimSalesman)

#         Sales_ONENAB_SSC_DimSalesman_Load = SparkSubmitOperator(**base_config,
#                                                   task_id="Load_Sales_ONENAB_SSC_DimSalesman", name='Load_Sales_ONENAB_SSC_DimSalesman', conn_id='DE_SparkConn',
#                                                   application="./dags/onenab/TCCDE_OneNab/files/RTMSTG_Sales_ONENAB_SSC_DimSalesman.py",
#                                                   conf={
#                                                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#                                                       "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
#                                                   }
#                                                   )
#         # End Sub ------  

#         # Start Sub ------
#         Sales_ONENAB_SSC_AggInvByBrandYM_Del = MySqlOperator(
#             task_id='Delete_Sales_ONENAB_SSC_AggInvByBrandYM', mysql_conn_id='TCCDE_RTMStagePRDCon', sql=sql_del_Sales_ONENAB_SSC_AggInvByBrandYM)

#         Sales_ONENAB_SSC_AggInvByBrandYM_Load = SparkSubmitOperator(**base_config,
#                                                   task_id="Load_Sales_ONENAB_SSC_AggInvByBrandYM", name='Load_Sales_ONENAB_SSC_AggInvByBrandYM', conn_id='DE_SparkConn',
#                                                   application="./dags/onenab/TCCDE_OneNab/files/RTMSTG_Sales_ONENAB_SSC_AggInvByBrandYM.py",
#                                                   conf={
#                                                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#                                                       "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
#                                                   }
#                                                   )
#         # End Sub ------  

#         # Start Sub ------
#         Sales_ONENAB_SSC_DimRoute_Del = MySqlOperator(
#             task_id='Delete_Sales_ONENAB_SSC_DimRoute', mysql_conn_id='TCCDE_RTMStagePRDCon', sql=sql_del_Sales_ONENAB_SSC_DimRoute)

#         Sales_ONENAB_SSC_DimRoute_Load = SparkSubmitOperator(**base_config,
#                                                   task_id="Load_Sales_ONENAB_SSC_DimRoute", name='Load_Sales_ONENAB_SSC_DimRoute', conn_id='DE_SparkConn',
#                                                   application="./dags/onenab/TCCDE_OneNab/files/RTMSTG_Sales_ONENAB_SSC_DimRoute.py",
#                                                   conf={
#                                                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#                                                       "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
#                                                   }
#                                                   )
#         # End Sub ------  

#         # Start Sub ------
#         Sales_ONENAB_SSC_FactRouteSalesman_Del = MySqlOperator(
#             task_id='Delete_Sales_ONENAB_SSC_FactRouteSalesman', mysql_conn_id='TCCDE_RTMStagePRDCon', sql=sql_del_Sales_ONENAB_SSC_FactRouteSalesman)

#         Sales_ONENAB_SSC_FactRouteSalesman_Load = SparkSubmitOperator(**base_config,
#                                                   task_id="Load_Sales_ONENAB_SSC_FactRouteSalesman", name='Load_Sales_ONENAB_SSC_FactRouteSalesman', conn_id='DE_SparkConn',
#                                                   application="./dags/onenab/TCCDE_OneNab/files/RTMSTG_Sales_ONENAB_SSC_FactRouteSalesman.py",
#                                                   conf={
#                                                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#                                                       "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
#                                                   }
#                                                   )
#         # End Sub ------  


#            ########### Send Email ##############
        
#         email_op = EmailOperator(
#                 task_id='Send_email_JobStatus',
#                 # to=["phonkrit.s@tcc-technology.com"],
#                 to=["phonkrit.s@tcc-technology.com"],
#                 #cc='phonkrit.s@tcc-technology.com,thanaporn.s@tcc-technology.com,theerapat.c@tcc-technology.com,wareethip.m@tcc-technology.com', 
#                 subject="Success Job OneNAB All Master Data to .26",
#                 # html_content="Survey data (สำรวจยอดซื้อและสต๊อก) are in attachment.",
#                 html_content='<strong><span style="color: #339966;">Success Job OneNAB All Master Data to .26</span></strong>',
#                 #files=[SV_SalesStock_BrandSKU_Data.name+'.csv',SV_SalesStock_Image_Data.name+'.csv',SV_SalesStock_Source_Data.name+'.csv'],
#                 # files=[SV_SalesStock_All.name+'.csv'],
#             )

        

        
        

#         Sales_ONENAB_SSC_DimChannel_Del >> Sales_ONENAB_SSC_DimChannel_Load >> \
#         Sales_ONENAB_SSC_DimCustGrp1_Del >> Sales_ONENAB_SSC_DimCustGrp1_Load >> \
#         Sales_ONENAB_SSC_DimCustGrpSap_Del >> Sales_ONENAB_SSC_DimCustGrpSap_Load >> \
#         Sales_ONENAB_SSC_DimCustomer_Del >> Sales_ONENAB_SSC_DimCustomer_Load >> \
#         Sales_ONENAB_SSC_DimLocation_Del >> Sales_ONENAB_SSC_DimLocation_Load >> \
#         Sales_ONENAB_SSC_DimMaterial_Del >> Sales_ONENAB_SSC_DimMaterial_Load >> \
#         Sales_ONENAB_SSC_DimSalesman_Del >> Sales_ONENAB_SSC_DimSalesman_Load >> \
#         Sales_ONENAB_SSC_AggInvByBrandYM_Del >> Sales_ONENAB_SSC_AggInvByBrandYM_Load >>\
#         Sales_ONENAB_SSC_DimRoute_Del >> Sales_ONENAB_SSC_DimRoute_Load >>\
#         Sales_ONENAB_SSC_FactRouteSalesman_Del >> Sales_ONENAB_SSC_FactRouteSalesman_Load >>\
#         email_op

       
#     chain(start, RTM_Staging_OneNab_Master, end)
