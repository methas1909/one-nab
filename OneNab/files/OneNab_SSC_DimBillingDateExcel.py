
import os
import sys
import paramiko
import pandas as pd
from io import BytesIO
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("PySparkSQLServer").getOrCreate()

# 🔹 กำหนดค่าการเชื่อมต่อ SQL Server
url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]


read_url = os.environ["ONABSTG_MSSQL_URL"]
read_user = os.environ["ONABSTG_MSSQL_USER"]
read_password = os.environ["ONABSTG_MSSQL_PASSWORD"]


#   ตั้งค่าข้อมูลเครื่องเซิร์ฟเวอร์
scp_host = os.environ["SCP_HOST"]
scp_user = os.environ["SCP_USER"]
scp_pass = os.environ["SCP_PASS"]
remote_file_path = "/data/NAB/Excel/Prod/SSC_DimBillingDate.xlsx"


def read_excel_from_scp() -> pd.DataFrame:
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(scp_host, username=scp_user, password=scp_pass)

        with ssh.open_sftp() as sftp:
            print("  SCP Connection Successful!")
            with sftp.open(remote_file_path, "rb") as remote_file:
                file_buffer = BytesIO(remote_file.read())
                pandas_df = pd.read_excel(file_buffer, engine="openpyxl")
                print("  Excel file loaded into memory.")
                return pandas_df

    except Exception as e:
        print(f"   SCP or Excel read error: {e}")
        sys.exit(1)


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
    (df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .mode(mode)
        .save())
    print(f' Data inserted into {table_name} ({df.count()} rows)')


def SSC_DimBillingDateExcel():
    pandas_df = read_excel_from_scp()

    if pandas_df.empty:
        print("   No data found in the Excel file.")
        return None

    df = spark.createDataFrame(pandas_df).withColumn(
        "PYLoadDate", current_timestamp())
    write_to_sql(df=df, table_name="SSC_DimBillingDateExcel",
                 url=url, user=user, password=password)
    return df


def SSC_DimBillingDateExcelRead():
    query = "SELECT [DATE] AS DateField, IS_WORKDAY FROM SSC_DimBillingDateExcel"
    df = read_sql_table(query=query, url=url, user=user, password=password)
    return df.withColumn("PYLoadDate", current_timestamp())


def SSC_TEMPDimBillingDate():
    query = "SELECT * FROM SSC_TEMPDimBillingDate"
    df = read_sql_table(query=query, user=user, url=url, password=password)
    return df.withColumn("PYLoadDate", current_timestamp())


def SSC_STGBillingDate():
    query = """SELECT [DateID], [YearID], [QuarterID], [QuarterDesc], [S_QuarterID], [S_QuarterDesc],
                      [MonthID], [MonthDesc], [S_MonthID], [S_MonthDesc], [DayID], [DateRef], [Date],
                      [WeekOfYearID], [WeekOfYearDesc], [DayOfWeekID], [DayOfWeekDesc], [DaysInMonth],
                      [FsYearID], [FsQuarterID], [FsQuarterDesc], [FsS_QuarterID], [FsS_QuarterDesc],
                      [FsMonthID], [FsS_MonthID], [FsWeekOfYearID], [FsWeekOfYearDesc], [ETLLoadData]
               FROM [STAGE].[dbo].[SSC_DimBillingDate]"""
    return read_sql_table(query=query, user=read_user, url=read_url, password=read_password)


def SSC_DimBillingDate():
    df_excel = SSC_DimBillingDateExcelRead()
    df_temp = SSC_TEMPDimBillingDate()
    df_stg = SSC_STGBillingDate()

    df_merged = (
        df_stg.join(df_excel, df_stg["DateRef"] ==
                    df_excel["DateField"], "left")
        .drop("DateField")
        .withColumnRenamed("DateID", "DateKey")
        .withColumnRenamed("IS_WORKDAY", "WorkDayFlag")
        .withColumn("PYLoadDate", current_timestamp())
    )

    write_to_sql(df=df_merged, table_name="SSC_DimBillingDate",
                 url=url, user=user, password=password)


# เรียกใช้งาน
SSC_DimBillingDateExcel()
SSC_DimBillingDate()
