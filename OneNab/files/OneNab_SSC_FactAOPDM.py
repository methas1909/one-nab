import os
import paramiko
import pandas as pd
from io import BytesIO
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.config(
    "spark.sql.execution.arrow.enabled", "true").getOrCreate()

#  ENV Variables
url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]

scp_host = os.environ["SCP_HOST"]
scp_user = os.environ["SCP_USER"]
scp_pass = os.environ["SCP_PASS"]
remote_file_path = "/data/NAB/Excel/Prod/SSC_FactAOPDM.xlsx"


def read_sql_table(query: str):
    return (spark.read
            .format("jdbc")
            .option("url", url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load())


def write_to_sql(df: DataFrame, table_name: str, url: str, user: str, password: str, mode="append"):
    (df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .mode(mode)
        .save())
    print(f' Data inserted into {table_name} ({df.count()} rows)')


def SSC_FactAOPDMExcel():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(scp_host, username=scp_user, password=scp_pass)

        with ssh.open_sftp() as sftp:
            print(" SCP Connection Successful!")

            with sftp.open(remote_file_path, "rb") as remote_file:
                file_buffer = BytesIO(remote_file.read())
    except Exception as e:
        print(f" SCP Error: {e}")
        return None

    try:
        df_pandas = pd.read_excel(file_buffer, engine="openpyxl")
        if df_pandas.empty:
            print(" No data found in the Excel file.")
            return None

        df_spark = spark.createDataFrame(df_pandas).withColumn(
            "PYLoadDate", current_timestamp())

        write_to_sql(df_spark, "SSC_FactAOPDMExcel",
                     url, user, password, mode="append")
        print(f" Processed {df_spark.count()} records into SSC_FactAOPDMExcel")
        return df_spark

    except Exception as e:
        print(f" Error reading Excel file: {e}")
        return None


query = """
SELECT [YM]
  ,[Branch]
  ,[SubBranch]
  ,[BranchDesc]
  ,[Brand]
  ,[ShopType]
  ,[AOPLY]
  ,[AOP]
  ,GETDATE() AS PYLoadDate
FROM [SSC_FactAOPDMExcel]
"""

df_excel_spark = SSC_FactAOPDMExcel()
df = read_sql_table(query)
write_to_sql(df, "SSC_FactAOPDM", url, user, password, mode="append")
