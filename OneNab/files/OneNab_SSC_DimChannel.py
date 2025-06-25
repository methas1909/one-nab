#  Standard library
import os

#  PySpark Core
from pyspark.sql import SparkSession, DataFrame

#  PySpark Functions (จัดกลุ่มให้เรียบร้อย)
from pyspark.sql.functions import current_timestamp


spark = SparkSession.builder.getOrCreate()


url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]


read_url = os.environ["ONABSTG_MSSQL_URL"]
read_user = os.environ["ONABSTG_MSSQL_USER"]
read_password = os.environ["ONABSTG_MSSQL_PASSWORD"]


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


query = """
    SELECT [ChannelMarge]
          ,[Channel]
          ,[ChannelDesc]
          ,[SalesOffice]
          ,[SalesOfficeDesc]
          ,[ChannelVSMS]
          ,[ChannelVSMSDesc]
          ,[ETLLoadData]
        ,GETDATE() AS [PYLoadDate]
      FROM [SSC_DimChannel]
    """


read_df = (spark.read.format("jdbc")
           .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
           .option("url", read_url)
           .option("query", query)
           .option("user", read_user)
           .option("password", read_password)
           .load())


tidb_df = read_df
dbtable = 'SSC_DimChannel'


tidb_df = tidb_df.withColumn("PYLoadDate", current_timestamp())


write_to_sql(df=tidb_df, table_name=dbtable, url=url,
             password=password, user=user, mode="append")

spark.stop()
