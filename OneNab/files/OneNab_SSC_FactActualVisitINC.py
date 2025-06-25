import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp

#   สร้าง SparkSession และตั้งค่าเชื่อมต่อ SQL Server
spark = SparkSession.builder.getOrCreate()

#   ตั้งค่าการเชื่อมต่อ SQL Server
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


def SSC_FactActualVisit():
    query = """
    SELECT Branch, Route, Salesman, Customer, VisitDate, ETLLoadData, GETDATE() AS PYLoadDate
    FROM STAGE.dbo.SSC_DataActualVisit
    WHERE FORMAT(VisitDate, 'yyyyMM') >= '202110'
    """
    df = read_sql_table(query=query, url=read_url,
                        user=read_user, password=read_password)
    df = df.withColumn("PYLoadDate", current_timestamp())  # เพิ่ม timestamp
    write_to_sql(df, "SSC_FactActualVisit", mode="append")
    return df


def SSC_FactActualVisitINC():
    query = """
    SELECT Branch, Route, Salesman, Customer, VisitDate, ETLLoadData, GETDATE() AS PYLoadDate
    FROM STAGE.dbo.SSC_DataActualVisit
    WHERE ETLLoadData >= DATEADD(day, -1, CAST(GETDATE() AS date))
    """
    df = read_sql_table(query=query, url=read_url,
                        user=read_user, password=read_password)
    df = df.withColumn("PYLoadDate", current_timestamp())  # เพิ่ม timestamp
    write_to_sql(df=df, table_name="SSC_FactActualVisit",
                 url=url, user=user, password=password, mode="append")
    return df


df_result = SSC_FactActualVisitINC()
print("  Data Processing Completed.")
