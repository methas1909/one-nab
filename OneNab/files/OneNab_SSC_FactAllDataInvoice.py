from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os

spark = SparkSession.builder.getOrCreate()


url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]


query = """
SELECT 1 AS HANAFlag
        ,[Branch]
        ,[YM]
        ,FORMAT([BillingDate],'yyyyMMdd') AS DateKey
        ,[BillingDate]
        ,[Route]
        ,[Channel]
        ,[ChannelVSMS]
        ,[ChannelMarge]
        ,[SalesOffice]
        ,[BType]
        ,[Salesman]
        ,[Customer]
        ,[Documents]
        ,NEW.[Material]
        ,SoldBaseQty/CubeConvert AS SoldCase
        ,[SoldSingle]
        ,[SoldBaseQty]
        ,FreeBaseQty/CubeConvert AS FreeCase
        ,[FreeSingle]
        ,[FreeBaseQty]
        ,[NetItemAmt]
        ,[VatAmt]
        ,[TotalAmt]
        ,[Discount]
        ,[SoldTo]
        ,[BillTo]
        ,[Payer]
        ,[ShipTo]
        ,[DataSource]
        ,[PromotionCode]
        ,[ReasonCode]
        ,[DOCUMENT_NUMBER]
        ,[VL]
        ,NEW.ETLLoadData
    FROM [STAGE].[dbo].[SSC_DataInvoice] NEW
        LEFT JOIN SSC_DimMaterial MAT ON NEW.Material = MAT.Material COLLATE Thai_CI_AS
      where 
      NEW.ETLLoadData >= DATEADD(day, -5, CAST(GETDATE() AS date))
    """


read_df = (spark.read.format("jdbc")
           .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
           .option("url", url)
           .option("query", query)
           .option("user", user)
           .option("password", password)
           .load())




# [Start Secret Variable]
url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]
# [END Secret Variable]
tidb_df = read_df
dbtable = 'SSC_FactAllDataInvoice'


tidb_df = tidb_df.withColumn("PYLoadDate", current_timestamp())


(tidb_df.repartition(10).write.format("jdbc")
    .option("url", url)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("dbtable", dbtable)
    .option("batchsize", 5000)
    .option("user", user)
    .option("password", password).mode('append')
    .option("isolationLevel", "NONE").save())

spark.stop()
