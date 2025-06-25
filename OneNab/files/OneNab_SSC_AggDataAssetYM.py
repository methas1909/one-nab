import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, concat, lit

#   สร้าง SparkSession และตั้งค่า JDBC
spark = SparkSession.builder.getOrCreate()

#   ตั้งค่าการเชื่อมต่อ SQL Server
url = os.environ["ONAB_MSSQL_URL"]
mssql_user = os.environ["ONAB_MSSQL_USER"]
sql_password = os.environ["ONAB_MSSQL_PASSWORD"]


def read_sql_table(query):
    """ อ่านข้อมูลจาก SQL Server เป็น PySpark DataFrame """
    return (spark.read
            .format("jdbc")
            .option("url", url)
            .option("query", query)
            .option("user", mssql_user)
            .option("password", sql_password)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load())


def write_to_sql(df, table_name, mode="append"):
    """ บันทึกข้อมูลลง SQL Server """
    (df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table_name)
        .option("user", mssql_user)
        .option("password", sql_password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .mode(mode)
        .save())
    print(f'  Inserted {df.count()} rows into {table_name}')


def SSC_AggDataAssetYM():
    query = """
    SELECT YM, BranchByVP, VPRoute, ActiveStatus, AssetStatus, AssetType, AssetCode
    FROM SSC_FactDataAsset
    """
    df = read_sql_table(query)

    #   Aggregate Data
    df_agg = (df.groupBy(
        concat(col("YM"), lit("01")).alias("DateKey"),
        col("BranchByVP").alias("BranchCode"),
        col("VPRoute").alias("RouteCode"),
        col("ActiveStatus"),
        col("AssetStatus"),
        col("AssetType")
    ).count().withColumnRenamed("count", "NoofCooler")
     .withColumn("PYLoadDate", current_timestamp()))

    #   บันทึกข้อมูลลง SQL Server
    write_to_sql(df_agg, "SSC_AggDataAssetYM", mode="append")

    return df_agg


#   ประมวลผลข้อมูล
df_result = SSC_AggDataAssetYM()
print("  Data Processing Completed.")
