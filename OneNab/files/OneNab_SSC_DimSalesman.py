import os
import sys
import paramiko
import pandas as pd
from io import BytesIO
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, col, coalesce
from pyspark.sql.types import StructType, StructField, StringType

#   สร้าง SparkSession
spark = SparkSession.builder.appName("PySparkSQLServer").getOrCreate()

#   ตั้งค่าข้อมูลเครื่องเซิร์ฟเวอร์
scp_host = os.environ["SCP_HOST"]
scp_user = os.environ["SCP_USER"]
scp_pass = os.environ["SCP_PASS"]

#   กำหนดค่าการเชื่อมต่อ SQL Server
url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]


read_url = os.environ["ONABSTG_MSSQL_URL"]
read_user = os.environ["ONABSTG_MSSQL_USER"]
read_password = os.environ["ONABSTG_MSSQL_PASSWORD"]


def read_sql_table(query: str, url: str, user: str, password: str):
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


def SSC_DimSalesmanExcel():
    remote_file_path = "/data/NAB/Excel/Prod/SSC_DimSalesman.xlsx"

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(scp_host, username=scp_user, password=scp_pass)

        with ssh.open_sftp() as sftp:
            print(" SCP Connection Successful!")
            with sftp.open(remote_file_path, "rb") as remote_file:
                file_buffer = BytesIO(remote_file.read())
            print(" File loaded into memory successfully.")
    except Exception as e:
        print(f" SCP Error: {e}")
        return None

    try:
        df_pandas = pd.read_excel(file_buffer, engine="openpyxl")

        if df_pandas.empty:
            print(" No data found in the Excel file.")
            return None

        #  แปลงเป็น Spark DF
        df_spark = spark.createDataFrame(df_pandas)

        # ➕ Add timestamp
        df_spark = df_spark.withColumn("PYLoadDate", current_timestamp())

        #  Write to SQL Server
        write_to_sql(df=df_spark, table_name="SSC_DimSalesmanExcel",
                     url=url, user=user, password=password, mode="append")

        print(
            f" Processed {df_spark.count()} records into SSC_DimRouteExcel")
        return df_spark

    except Exception as e:
        print(f" Error reading Excel file from memory: {e}")
        return None


def SSC_DimSalesmanSTG():
    query = """
    SELECT [Salesman]
          ,[SalesmanDesc]
          ,[SalesmanDescription]
          ,[ETLLoadData]
      FROM [SSC_DimSalesman]
    """

    df = read_sql_table(query=query, url=read_url,
                        user=read_user, password=read_password)
    return df

#   รวมข้อมูลจาก Excel และ Temp Table


def merge_SSC_DimSalesman():
    df_excel = SSC_DimSalesmanExcel()
    df = SSC_DimSalesmanSTG()

    df_merged = df_excel.alias("ex").join(
        df.alias("stg"), "Salesman", "outer"
    ).select(
        col("Salesman"),
        coalesce(col("ex.SalesmanDesc"), col(
            "stg.SalesmanDesc")).alias("SalesmanDesc"),
        coalesce(col("ex.SalesmanDescription"), col(
            "stg.SalesmanDescription")).alias("SalesmanDescription"),
        current_timestamp().alias("PYLoadDate")
    )

    write_to_sql(df=df_merged, table_name="SSC_DimSalesman",
                 user=user, password=password, url=url, mode="append")
    print(f"  Merged {df_merged.count()} records into SSC_DimSalesman")


#   รันทุกฟังก์ชัน
merge_SSC_DimSalesman()
