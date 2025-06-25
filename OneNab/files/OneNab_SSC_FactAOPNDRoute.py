

# การนำเข้าไลบรารีที่จำเป็น
import os

# ไลบรารีสำหรับการทำงานกับ PySpark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, col, concat, lit
from pyspark.sql import DataFrame

# ไลบรารีสำหรับการทำงานกับไฟล์และข้อมูลภายนอก
import paramiko
import pandas as pd
from io import BytesIO
#    สร้าง SparkSession
spark = (SparkSession.builder.config(
    "spark.sql.execution.arrow.enabled", "true").getOrCreate())

#    ดึงข้อมูลการเชื่อมต่อจาก Environment Variables
url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]


scp_host = os.environ["SCP_HOST"]
scp_user = os.environ["SCP_USER"]
scp_pass = os.environ["SCP_PASS"]


def read_sql_table(query: str):
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


remote_file_path = "/data/NAB/Excel/Prod/SSC_FactAOPNDRoute.xlsx"
local_file_path = "/tmp/SSC_FactAOPNDRoute.xlsx"


def SSC_FactAOPNDRouteExcel():
    """Downloads the SSC_FactAOPNDRoute Excel file via SCP into memory and writes it to SQL Server."""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(scp_host, username=scp_user, password=scp_pass)

        with ssh.open_sftp() as sftp:
            print(" SCP Connection Successful!")
            with sftp.open(remote_file_path, "rb") as remote_file:
                file_buffer = BytesIO(remote_file.read())
                print(" File loaded into memory.")
    except Exception as e:
        print(f" SCP Error: {e}")
        return None

    try:
        df_pandas = pd.read_excel(file_buffer, engine="openpyxl")
        if df_pandas.empty:
            print(" No data found in the Excel file.")
            return None

        # Convert to PySpark DataFrame
        df_spark = spark.createDataFrame(df_pandas).withColumn(
            "PYLoadDate", current_timestamp())

        # Write to SQL Server
        write_to_sql(df_spark, "SSC_FactAOPNDRouteExcel",
                     url, user, password, mode="append")
        print(
            f" Processed {df_spark.count()} records into SSC_FactAOPNDRouteExcel")
        return df_spark
    except Exception as e:
        print(f" Error reading Excel file: {e}")
        return None

df_excel_spark = SSC_FactAOPNDRouteExcel()
query = """
    SELECT [YM]
      ,[Route]
      ,[Brand]
      ,[ShopType]
      ,[AOPLYRoute]
      ,[AOPRoute]
    ,GETDATE() AS PYLoadDate
    FROM [SSC_FactAOPNDRouteExcel]
"""

df = read_sql_table(query)

write_to_sql(df, "SSC_FactAOPNDRoute", url, user, password, mode="append")
