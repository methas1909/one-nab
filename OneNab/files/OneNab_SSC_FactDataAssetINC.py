import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp

#   สร้าง SparkSession และตั้งค่า JDBC สำหรับเชื่อมต่อ SQL Server
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


tbname = "SSC_FactDataAsset"


def SSC_FactDataAsset():
    query = """
    SELECT DA.[YM]
        , DA.[BranchByAsset]
        , VP.Branch AS BranchByVP
        , VP.Route AS VPRoute
        , DA.[Customer]
        , DA.[AssetType]
        , DA.[AssetSubType]
        , DA.[AssetStatus]
        , DA.[ActiveStatus]
        , DA.[AssetQRCode]
        , DA.[AssetCode]
        , DA.[AssetDesc]
        , DA.[AssetEquipmentNo]
        , DA.[AssetSerialNo]
        , DA.ETLLoadData
        , GETDATE() AS PYLoadDate
    FROM [STAGE].[dbo].[SSC_DataAsset] DA
        LEFT JOIN [STAGE].[dbo].[SSC_SumCustomerPerMonth_Plan] VP 
        ON VP.YM = DA.YM AND VP.Customer = DA.Customer
    """
    df = read_sql_table(query=query, url=read_url,
                        user=read_user, password=read_password)
    df = df.withColumn("PYLoadDate", current_timestamp())  # เพิ่ม timestamp
    write_to_sql(df=df, table_name=tbname, url=url,
                 password=password, user=user, mode="append")
    return df


def SSC_FactDataAssetINC():
    query = """
    SELECT DA.[YM]
        , DA.[BranchByAsset]
        , VP.Branch AS BranchByVP
        , VP.Route AS VPRoute
        , DA.[Customer]
        , DA.[AssetType]
        , DA.[AssetSubType]
        , DA.[AssetStatus]
        , DA.[ActiveStatus]
        , DA.[AssetQRCode]
        , DA.[AssetCode]
        , DA.[AssetDesc]
        , DA.[AssetEquipmentNo]
        , DA.[AssetSerialNo]
        , DA.ETLLoadData
        , GETDATE() AS PYLoadDate
    FROM [STAGE].[dbo].[SSC_DataAsset] DA
        LEFT JOIN [STAGE].[dbo].[SSC_SumCustomerPerMonth_Plan] VP 
        ON VP.YM = DA.YM AND VP.Customer = DA.Customer
    WHERE DA.ETLLoadData >= DATEADD(day, -1, CAST(GETDATE() AS date))
    """
    df = read_sql_table(query=query, url=read_url,
                        user=read_user, password=read_password)
    df = df.withColumn("PYLoadDate", current_timestamp())  # เพิ่ม timestamp
    write_to_sql(df=df, table_name=tbname, url=url,
                 password=password, user=user, mode="append")
    return df


#   ประมวลผลข้อมูล
df_result = SSC_FactDataAssetINC()
print("  Data Processing Completed.")
