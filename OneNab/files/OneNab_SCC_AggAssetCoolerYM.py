import os
from pyspark.sql import SparkSession

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


def SCC_AggAssetCoolerYM():

    query_invoice = """
        SELECT  
                CONCAT(TAA.YM,'01') AS DateKey,
                TAA.BranchCode,
                ISNULL(TAA.RouteCode, '') AS RouteCode,
                TAA.CustomerCode,
                AST.AssetSubType,
                AST.NoOfCooler,
                TAA.AmountPerCooler * AST.NoOfCooler AS AmountByAsset,
                GETDATE() AS PYLoadDate
            FROM 
            (
                SELECT  
                    DA.YM,
                    ISNULL(DA.BranchByVP, DA.BranchByAsset) AS BranchCode,
                    DA.VPRoute AS RouteCode,
                    DA.Customer AS CustomerCode,
                    COUNT(*) AS TotalCooler,
                    MAX(INV.TotalAmount) AS TotalAmount,
                    MAX(INV.TotalAmount) / COUNT(*) AS AmountPerCooler
                FROM 
                    SSC_FactDataAsset DA
                    LEFT JOIN (
                        SELECT  
                            DateKey,
                            CustomerCode,
                            SUM(TotalAmount) AS TotalAmount
                        FROM 
                            [dbo].[SCC_AggInvVolByBrandYM]
                        WHERE 
                            ISNULL(BrandDesc, '0') <> 'F&N'
                        GROUP BY 
                            DateKey, CustomerCode
                    ) INV 
                    ON DA.YM = LEFT(INV.DateKey, 6) AND DA.Customer = INV.CustomerCode 
                WHERE 
                    DA.AssetType = 'ตู้แช่'
                GROUP BY 
                    DA.YM,
                    ISNULL(DA.BranchByVP, DA.BranchByAsset),
                    DA.VPRoute,
                    DA.Customer
            ) TAA
            LEFT JOIN 
            (
                SELECT 
                    YM,
                    Customer,
                    AssetSubType,
                    COUNT(*) AS NoOfCooler
                FROM 
                    SSC_FactDataAsset
                WHERE 
                    AssetType = 'ตู้แช่'
                GROUP BY 
                    YM, Customer, AssetSubType
            ) AST 
            ON TAA.YM = AST.YM AND TAA.CustomerCode = AST.Customer

    """
    df_final = read_sql_table(query_invoice)

    write_to_sql(df_final, "SCC_AggAssetCoolerYM", mode="append")

    return df_final


df_result = SCC_AggAssetCoolerYM()
print("  Data Processing Completed.")
