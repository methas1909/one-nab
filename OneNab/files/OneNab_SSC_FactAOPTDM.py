

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


remote_file_path = "/data/NAB/Excel/Prod/SSC_FactAOPTDM.xlsx"
local_file_path = "/tmp/SSC_FactAOPTDM.xlsx"


def SSC_DimTargetExcelGeneric(remote_file_path, sheet_name, tbname):
    """
    Downloads an Excel sheet via SCP into memory and writes it to SQL Server.
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(scp_host, username=scp_user, password=scp_pass)

        with ssh.open_sftp() as sftp:
            print(" SCP Connection Successful!")

            # 🔁 อ่านไฟล์เข้า memory โดยตรง
            with sftp.open(remote_file_path, "rb") as remote_file:
                excel_buffer = BytesIO(remote_file.read())

    except Exception as e:
        print(f" SCP Error: {e}")
        return None

    try:
        df_pandas = pd.read_excel(
            excel_buffer, sheet_name=sheet_name, engine="openpyxl")
        if df_pandas.empty:
            print(f" No data in sheet: {sheet_name}")
            return None

        df_pandas = df_pandas.astype(str)

        df_spark = spark.createDataFrame(df_pandas).withColumn(
            "PYLoadDate", current_timestamp()
        )

        write_to_sql(df_spark, tbname, url, user, password, mode="append")
        print(f" Processed {df_spark.count()} records into {tbname}")
        return df_spark

    except Exception as e:
        print(f" Error processing Excel sheet '{sheet_name}': {e}")
        return None


df_est = SSC_DimTargetExcelGeneric(
    remote_file_path=remote_file_path,
    sheet_name="Est",
    tbname="SSC_FactAOPTDMExcel"
)

df_crystal = SSC_DimTargetExcelGeneric(
    remote_file_path=remote_file_path,
    sheet_name="Crystal",
    tbname="SSC_FactAOPTDMExcel"
)

df_oishi = SSC_DimTargetExcelGeneric(
    remote_file_path=remote_file_path,
    sheet_name="Oishi",
    tbname="SSC_FactAOPTDMExcel"
)

df_other_brand = SSC_DimTargetExcelGeneric(
    remote_file_path=remote_file_path,
    sheet_name="Other Brand",
    tbname="SSC_FactAOPTDMExcel"
)

df_total_brand = SSC_DimTargetExcelGeneric(
    remote_file_path=remote_file_path,
    sheet_name="Total Brand",
    tbname="SSC_FactAOPTDMExcel"
)

df_fsr_all = SSC_DimTargetExcelGeneric(
    remote_file_path=remote_file_path,
    sheet_name="FSR All Brand",
    tbname="SSC_FactAOPTDMExcel"
)

df_wrangyer = SSC_DimTargetExcelGeneric(
    remote_file_path=remote_file_path,
    sheet_name="Wrangyer",
    tbname="SSC_FactAOPTDMExcel"
)


query = """
    SELECT [TDMName]
          ,[TDMID]
          ,[ShopType]
          ,FORMAT([StartDate],'yyyyMM') AS YM
          ,[Brand]
          ,[TargetType]
          ,[LY]
          ,[AOP]
      FROM [SSC_FactAOPTDMExcel]
"""

df = read_sql_table(query)
write_to_sql(df, "SSC_FactAOPTDM", url, user, password, mode="append")
