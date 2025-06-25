# การนำเข้าไลบรารีที่จำเป็น
import os
from io import BytesIO
import paramiko
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp

#  สร้าง SparkSession
spark = (SparkSession.builder
         .config("spark.sql.execution.arrow.enabled", "true")
         .getOrCreate())

#  ดึงข้อมูลการเชื่อมต่อจาก Environment Variables
url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]

scp_host = os.environ["SCP_HOST"]
scp_user = os.environ["SCP_USER"]
scp_pass = os.environ["SCP_PASS"]
remote_file_path = "/data/NAB/Excel/Prod/SSC_FactAOPNDDM.xlsx"


def read_sql_table(query: str) -> DataFrame:
    """ อ่านข้อมูลจาก SQL Server เป็น PySpark DataFrame """
    return (spark.read
            .format("jdbc")
            .option("url", url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load())


def write_to_sql(df: DataFrame, table_name: str, url: str, user: str, password: str, mode="append"):
    """Writes a PySpark DataFrame to SQL Server."""
    (df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .mode(mode)
        .save())
    print(f' Data inserted into {table_name} ({df.count()} rows)')


def SSC_FactAOPNDDMExcel():
    """Downloads the SSC_FactAOPNDDM Excel file via SCP and loads into SQL Server."""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(scp_host, username=scp_user, password=scp_pass)

        with ssh.open_sftp() as sftp:
            print(" SCP Connection Successful!")
            with sftp.open(remote_file_path, "rb") as remote_file:
                excel_buffer = BytesIO(remote_file.read())  #  โหลดเข้า memory

    except Exception as e:
        print(f" SCP Error: {e}")
        return None

    try:
        df_pandas = pd.read_excel(excel_buffer, engine="openpyxl")
        if df_pandas.empty:
            print(" No data found in the Excel file.")
            return None

        # Convert to PySpark DataFrame
        df_spark = spark.createDataFrame(df_pandas).withColumn(
            "PYLoadDate", current_timestamp())

        # Write to SQL Server
        write_to_sql(df_spark, "SSC_FactAOPNDDMExcel",
                     url, user, password, mode="append")
        print(
            f" Processed {df_spark.count()} records into SSC_FactAOPNDDMExcel")
        return df_spark
    except Exception as e:
        print(f" Error reading Excel file: {e}")
        return None


df_excel_spark = SSC_FactAOPNDDMExcel()


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
    FROM [SSC_FactAOPNDDMExcel]
"""

df = read_sql_table(query)
write_to_sql(df, "SSC_FactAOPNDDM", url, user, password, mode="append")
