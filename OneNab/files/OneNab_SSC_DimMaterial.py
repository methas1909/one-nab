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


def SSC_DimMaterial(tbname):
    """ อ่านข้อมูลจาก SSC_DimMaterial และบันทึกลง SQL """

    query = """
    SELECT [UseCompany]
          ,[Material]
          ,[OldMaterial]
          ,[MaterialDescThai]
          ,[MaterialDescriptionThai]
          ,[MaterialDescEng]
          ,[MaterialDescriptionEng]
          ,[MaterialType]
          ,[MaterialTypeDesc]
          ,[MaterialTypeDescription]
          ,[Flaver]
          ,[FlaverDesc]
          ,[FlaverDescription]
          ,[PackagingType]
          ,[Packaging]
          ,[PackagingDesc]
          ,[PackagingDescription]
          ,[NABPackSize]
          ,[NABPackSizeDesc]
          ,[NABPackSizeDescription]
          ,[Sizing]
          ,[SizingDesc]
          ,[SizingDescription]
          ,[BasicMaterial]
          ,[BrandCode]
          ,[BrandDesc]
          ,[BrandDescription]
          ,[GroupPostMix]
          ,[CubeUnit]
          ,[CubeUnitDesc]
          ,[CubeUnitDescription]
          ,[CubeConvert]
          ,[MatType1Id]
          ,[MatType1]
          ,[Millilitre]
          ,[KPIweight]
          ,[xCase]
          ,[BOM_Case]
          ,[BOM_Bottle]
          ,[ETLLoadData]
          ,GETDATE() AS PYLoadDate
      FROM [SSC_DimMaterial]
    """

 
    df = read_sql_table(query=query, url=read_url,
                        user=read_user, password=read_password)

   
    df = df.withColumn("PYLoadDate", current_timestamp())

    
    write_to_sql(df=df, table_name=tbname, url=url,
                 user=user, password=password, mode="append")

    print(f'------------ Read/Write DataFrame {tbname} Completed ------------')

    return df




DF_ReadResultC = SSC_DimMaterial(tbname='SSC_DimMaterial')
