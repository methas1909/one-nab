from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, year, month, date_format
from pyspark.sql.types import IntegerType, StringType, LongType
import pyspark.sql.functions as F
import hvac
import os

spark = SparkSession.builder.getOrCreate()


url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]
# table = os.environ["MSSQL_TABLE"]

query = """
    SELECT [CustGrp]
        ,[CustGrpDesc]
        ,[CustGrpDescription]
        ,[ShopType]
        ,[ETLLoadData]
        ,[PYLoadDate]
    FROM [SSC_DimCustGrp1]
    """

 # [END Secret Variable]

read_df = spark.read.format("jdbc") \
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("url", url) \
    .option("query", query) \
    .option("user", user) \
    .option("password", password) \
    .load()

# delete later, used for counting


# [Start Secret Variable]    
url = os.environ["RTM_STG_PRD_TIDB_URL"]
user = os.environ["RTM_STG_PRD_TIDB_USER"]
password = os.environ["RTM_STG_PRD_TIDB_PASSWORD"]
# [END Secret Variable]
tidb_df = read_df 
dbtable = 'Sales_ONENAB_SSC_DimCustGrp1'


tidb_df = tidb_df.withColumn("PYLoadDate",current_timestamp())


tidb_df.repartition(10).write.format("jdbc") \
    .option("url", url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", dbtable) \
    .option("batchsize", 150000) \
    .option("user", user) \
    .option("password", password).mode('append') \
    .option("isolationLevel","NONE").save()

spark.stop()
