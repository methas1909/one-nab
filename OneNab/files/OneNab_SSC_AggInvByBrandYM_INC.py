
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import hvac
import os

spark = SparkSession.builder.getOrCreate()

#    อ่านค่าจาก Environment Variables
read_url = os.environ["ONABSTG_MSSQL_URL"]
read_user = os.environ["ONABSTG_MSSQL_USER"]
read_password = os.environ["ONABSTG_MSSQL_PASSWORD"]

write_url = os.environ["ONAB_MSSQL_URL"]
write_user = os.environ["ONAB_MSSQL_USER"]
write_password = os.environ["ONAB_MSSQL_PASSWORD"]

#    กำหนด Database Properties (แก้ไข user/password)
read_db_properties = {
    "user": read_user,
    "password": read_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

write_db_properties = {
    "user": write_user,
    "password": write_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def read_sql_table(query):
    return spark.read \
        .format("jdbc") \
        .option("url", read_url) \
        .option("dbtable", f"({query}) as subquery") \
        .option("user", read_user) \
        .option("password", read_password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()


def write_to_sql(df, table_name, mode="append"):
    df.write \
        .format("jdbc") \
        .option("url", write_url) \
        .option("dbtable", table_name) \
        .option("user", write_user) \
        .option("password", write_password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode(mode) \
        .save()


def process():
    query = """
                  SELECT  
                      INV.HANAFlag,
                      CONCAT(YM,'01') AS DateKey,
                      Branch AS BranchCode,
                      Route AS RouteCode,
                      Salesman,
                      ChannelMarge AS ChannelSalesOffice,
                      INV.Customer AS CustomerCode,
                      DataSource AS DataSource,
                      BType AS BillType,
                      BrandDescription,
                      CASE 
                          WHEN BrandDesc = 'Crystal' THEN 'Crystal'
                          WHEN BrandDesc = 'Est' THEN 'Est'
                          WHEN BrandDesc = 'Oishi' THEN 'Oishi'
                          WHEN BrandDesc = 'Oishi Chakulza' THEN 'Oishi Chakulza'
                          WHEN BrandDesc = 'Oishi Green Tea' THEN 'Oishi Green Tea'
                          WHEN BrandDesc = 'Wrangyer' THEN 'Wrangyer' 
                          ELSE 'Other Brand' 
                      END AS Brand,
                      CASE 
                          WHEN BType = 'TaxInv' THEN COUNT(DISTINCT Documents) 
                          ELSE 0 
                      END AS NoOfBrandBill,
                      CASE 
                          WHEN BType <> 'TaxInv' THEN COUNT(DISTINCT Documents) 
                          ELSE 0 
                      END AS NoOfBrandBillCN,
                      SUM(SoldSingle) AS SoldSingle,
                      SUM(SoldCase) AS SoldCase,
                      SUM(SoldBaseQty) AS SoldBaseQty,
                      SUM((SoldBaseQty * Millilitre) / 1000) AS SoldLitre,
                      SUM(FreeCase) AS FreeCase,
                      SUM(FreeBaseQty) AS FreeBaseQty,
                      SUM((FreeBaseQty * Millilitre) / 1000) AS FreeLitre,
                      SUM(NetItemAmt) AS NetItemAmt,
                      SUM(VatAmt) AS VatAmt,
                      SUM(TotalAmt) AS TotalAmount,
                      SUM(Discount) AS Discount,
                      GETDATE() AS PYLoadDate
                  FROM ONENAB.dbo.SSC_FactAllDataInvoice INV
                  INNER JOIN (
                      SELECT ReasonCode 
                      FROM ONENAB.dbo.SSC_DimReason 
                      WHERE LEFT(ReasonCode,1) <> '6'
                  ) AS CR ON INV.ReasonCode = CR.ReasonCode
                  INNER JOIN (
                      SELECT * 
                      FROM ONENAB.dbo.SSC_DimCustomer 
                      WHERE CustGrp1 NOT IN (-2,131,132,134,139,340,345)
                  ) AS CU ON INV.HANAFlag = CU.HANAFlag AND INV.Customer = CU.Customer
                  LEFT JOIN ONENAB.dbo.SSC_DimMaterial MT ON INV.Material = MT.Material
                  WHERE MaterialType = 'Z31' 
                  AND MatType1Id IN (1,8) 
                  AND PackagingType IN ('NR','RB') 
                  AND GroupPostMix <> 'PostMix'
                  AND YM >= CAST(FORMAT((DATEADD(mm, DATEDIFF(mm, 0, GETDATE()) - 5, 0)),'yyyyMM') AS INT) 
                  GROUP BY  
                      INV.HANAFlag,  
                      CONCAT(YM,'01'), 
                      Branch, 
                      Route, 
                      Salesman, 
                      INV.Customer, 
                      ChannelMarge, 
                      DataSource, 
                      BType, 
                      BrandDescription,
                      CASE 
                          WHEN BrandDesc = 'Crystal' THEN 'Crystal'
                          WHEN BrandDesc = 'Est' THEN 'Est'
                          WHEN BrandDesc = 'Oishi' THEN 'Oishi'
                          WHEN BrandDesc = 'Oishi Chakulza' THEN 'Oishi Chakulza'
                          WHEN BrandDesc = 'Oishi Green Tea' THEN 'Oishi Green Tea'
                          WHEN BrandDesc = 'Wrangyer' THEN 'Wrangyer' 
                          ELSE 'Other Brand' 
                      END

    
   """
    df = read_sql_table(query)
    write_to_sql(df, "SSC_AggInvByBrandYM", "append")

    return df


# Run the processing function
df_result = process()

