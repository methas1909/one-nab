import os
import sys
import paramiko
import pandas as pd
from io import BytesIO
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, when, lit
from pyspark.sql.types import LongType, IntegerType


spark = SparkSession.builder.config(
    "spark.sql.execution.arrow.enabled", "true").getOrCreate()

scp_host = os.environ["SCP_HOST"]
scp_user = os.environ["SCP_USER"]
scp_pass = os.environ["SCP_PASS"]


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


def download_scp(remote_path, local_path):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(scp_host, username=scp_user, password=scp_pass)

        with ssh.open_sftp() as sftp:
            print("  SCP Connection Successful!")
            sftp.get(remote_path, local_path)
            print(f"  File Downloaded: {local_path}")

    except Exception as e:
        print(f"   SCP Error: {e}")
        sys.exit(1)


def SSC_DimRouteExcel():
    remote_file_path = "/data/NAB/Excel/Prod/SSC_DimRoute.xlsx"

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

        for col in df_pandas.columns:
            if col not in ["SubBranch", "YM"]:
                df_pandas[col] = df_pandas[col].astype(str)

        df_spark = spark.createDataFrame(df_pandas)

        if "SubBranch" in df_spark.columns:
            df_spark = df_spark.withColumn(
                "SubBranch", df_spark["SubBranch"].cast(LongType()))
        if "YM" in df_spark.columns:
            df_spark = df_spark.withColumn(
                "YM", df_spark["YM"].cast(IntegerType()))

        df_spark = df_spark.withColumn("PYLoadDate", current_timestamp())

        write_to_sql(df=df_spark, table_name="SSC_DimRouteExcel",
                     url=url, user=user, password=password, mode="append")

        print(
            f" Processed {df_spark.count()} records into SSC_DimRouteExcel")
        return df_spark

    except Exception as e:
        print(f" Error reading Excel file from memory: {e}")
        return None


def SSC_DimRouteRead():
    query = """
    SELECT Route, RouteDesc, RouteDescription, Sector, ETLLoadData, GETDATE() AS PYLoadDate
    FROM SSC_DimRoute
    """
    df = read_sql_table(query=query, url=read_url,
                        user=read_user, password=read_password)
    return df


def SSC_DimRouteExcelRead():
    query = """
    SELECT DISTINCT Route, TerritoryDistrict, SalesType1, SalesType2, BranchVL,
           RegionSSC, SubBranch, SalesMan, TDM, DM
    FROM SSC_DimRouteExcel
    WHERE YM = (SELECT MAX(YM) FROM SSC_DimRouteExcel)
    """
    df = read_sql_table(query=query, url=url, user=user, password=password)
    return df


def SSC_DimRouteExcelReadForFact():
    query = """
    SELECT Route, TerritoryDistrict, SalesType1, SalesType2, BranchVL,
           RegionSSC, SubBranch, SalesMan, TDM, DM, YM
    FROM SSC_DimRouteExcel
    """
    df = read_sql_table(query=query, url=url, user=user, password=password)
    return df


def merge_SSC_DimRoute():
    df_route = SSC_DimRouteRead()
    df_excel = SSC_DimRouteExcelRead()

    df_left = df_excel.withColumn("_merge_flag_left", lit(1))
    df_right = df_route.withColumn("_merge_flag_right", lit(1))

    df_merged = df_left.join(df_right, on="Route", how="left")

    df_merged = df_merged.withColumn(
        "_merge",
        when(col("_merge_flag_left").isNotNull() & col(
            "_merge_flag_right").isNotNull(), "both")
        .when(col("_merge_flag_left").isNotNull() & col("_merge_flag_right").isNull(), "left_only")
        .when(col("_merge_flag_left").isNull() & col("_merge_flag_right").isNotNull(), "right_only")
    )

    df_merged = df_merged.drop(
        "SalesMan", "TDM", "DM", "_merge", "_merge_flag_left", "_merge_flag_right")

    df_merged = df_merged.dropDuplicates()

    write_to_sql(df=df_merged, table_name="SSC_DimRoute",
                 url=url, password=password, user=user, mode="append")

    print(f"  Merged {df_merged.count()} records into SSC_DimRoute")

    return df_merged


def merge_SSC_FactRouteSalesman():
    df_fact = SSC_DimRouteExcelReadForFact()
    df_route = SSC_DimRouteRead()

    df_merged = (df_fact.join(df_route, "Route", "left")
                 .drop("TerritoryDistrict", "SalesType1", "SalesType2", "RegionSSC",
                       "SubBranch", "RouteDesc", "RouteDescription", "Sector")
                 .dropDuplicates()
                 .withColumn("PYLoadDate", current_timestamp()))

    write_to_sql(df=df_merged, table_name="SSC_FactRouteSalesman",
                 url=url, user=user, password=password, mode="append")

    print(
        f"  Merged {df_merged.count()} records into SSC_FactRouteSalesman")
    return df_merged


SSC_DimRouteExcel()
merge_SSC_DimRoute()
merge_SSC_FactRouteSalesman()
