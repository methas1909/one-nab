

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp

#   สร้าง SparkSession
spark = SparkSession.builder.appName("PySparkSQLServer").getOrCreate()

# 🔹 กำหนดค่าการเชื่อมต่อ SQL Server
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


def SSC_DimMaterialOld(tbname):
    """ อ่านข้อมูลจาก SSC_DimMaterialOld และบันทึกลง SQL """

    query = """
    SELECT  [Material]
          ,[MaterialDesc1]
          ,[MaterialDesc2]
          ,[MaterialDesc1Thai]
          ,[MaterialDesc2Thai]
          ,[MatGrp]
          ,[MatGrpDesc1]
          ,[MatGrpDesc2]
          ,[MatType1Id]
          ,[MatType1]
          ,[MatType2]
          ,[MatType3]
          ,[MatType4]
          ,[Brand1]
          ,[Brand2]
          ,[Brand3]
          ,[PackageSize]
          ,[PackageQty]
          ,[Millilitre]
          ,[Multiply]
          ,[KPIweight]
          ,[MaterialGrp1Desc]
          ,[MaterialGrp2Desc]
          ,[MaterialGrp3Desc]
          ,[MaterialGrp4Desc]
          ,[MaterialGrp5Desc]
          ,[BOM_Case]
          ,[BOM_Bottle]
          ,[ETLLoadData]
          ,GETDATE() AS PYLoadDate
      FROM [SSC_DimMaterialOld]
    """

 
    df = read_sql_table(query=query, user=read_user,
                        url=read_url, password=read_password)

   
    df = df.withColumn("PYLoadDate", current_timestamp())

    
    write_to_sql(df=df, table_name=tbname, url=url,
                 user=user, password=password, mode="append")

    print(f'------------ Read/Write DataFrame {tbname} Completed ------------')

    return df




DF_ReadResultC = SSC_DimMaterialOld(tbname='SSC_DimMaterialOld')
