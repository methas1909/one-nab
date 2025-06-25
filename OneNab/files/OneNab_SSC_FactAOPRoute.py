from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import os
import paramiko
import pandas as pd
from io import BytesIO


spark = SparkSession.builder.getOrCreate()


db_url_onenab = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]


scp_host = os.environ["SCP_HOST"]
scp_user = os.environ["SCP_USER"]
scp_pass = os.environ["SCP_PASS"]
remote_excel_path = "/data/NAB/Excel/Prod/SSC_FactAOPRoute.xlsx"


db_properties = {
    "user": user,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


def read_table(db_url, query):
    return spark.read.format("jdbc").options(
        url=db_url,
        query=query,
        **db_properties
    ).load()


def write_table(df, table_name, db_url, mode="append", truncate=False):
    (df.write.format("jdbc")
        .option("url", db_url)
        .option("dbtable", table_name)
        .option("batchsize", "10000")
        .mode(mode)
        .options(**db_properties)
        .save())

    print(f" Inserted data into {table_name}")


print(" Reading SSC_FactAOPRouteExcel from remote SCP...")

try:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(scp_host, username=scp_user, password=scp_pass)

    with ssh.open_sftp() as sftp:
        with sftp.open(remote_excel_path, "rb") as remote_file:
            excel_data = remote_file.read()
            excel_buffer = BytesIO(excel_data)

    pandas_df = pd.read_excel(excel_buffer, engine="openpyxl")
    if not pandas_df.empty:
        df_excel = spark.createDataFrame(pandas_df).withColumn(
            "PYLoadDate", current_timestamp())

        print(f"df_excel : {df_excel.count()}")
        write_table(df_excel, "SSC_FactAOPRouteExcel",
                    db_url_onenab, mode="append")
        print(" Excel data loaded and written to SSC_FactAOPRouteExcel")
    else:
        print("  Excel file is empty.")

except Exception as e:
    print(f" Failed to load Excel via SCP: {e}")


query_route = """
        SELECT YM, Route, Brand, ShopType, AOPLYRoute, AOPRoute, GETDATE() AS PYLoadDate
        FROM SSC_FactAOPRouteExcel
    """

df_route = read_table(db_url_onenab, query_route).withColumn(
    "PYLoadDate", current_timestamp())
write_table(df_route, "SSC_FactAOPRoute", db_url_onenab,
            mode="append", truncate=False)


print("ETL Processing Completed.")
