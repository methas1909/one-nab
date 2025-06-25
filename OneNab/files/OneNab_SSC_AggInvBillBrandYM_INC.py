import os

# ไลบรารีสำหรับการทำงานกับ PySpark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrame

# ไลบรารีสำหรับการทำงานกับไฟล์และข้อมูลภายนอก
import paramiko
import pandas as pd

#    สร้าง SparkSession
spark = (SparkSession.builder.config(
    "spark.sql.execution.arrow.enabled", "true").getOrCreate())

#    อ่านค่าจาก Environment Variables

url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]

#    กำหนด Database Properties (แก้ไข user/password)
def read_sql_table(query):
    """ อ่านข้อมูลจาก SQL Server เป็น PySpark DataFrame """
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
                      CASE 
                          WHEN BType = 'TaxInv' THEN COUNT(DISTINCT Documents) 
                          ELSE 0 
                      END AS NoOfBill,
                      CASE 
                          WHEN BType <> 'TaxInv' THEN COUNT(DISTINCT Documents) 
                          ELSE 0 
                      END AS NoOfBillCN,
                      CASE 
                          WHEN BType = 'TaxInv' THEN COUNT(DISTINCT CONCAT(BasicMaterial,Documents)) 
                          ELSE 0 
                      END AS NoOfBrand,
                      CASE 
                          WHEN BType <> 'TaxInv' THEN COUNT(DISTINCT CONCAT(BasicMaterial,Documents)) 
                          ELSE 0 
                      END AS NoOfBrandCN,
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
                      BType

"""

df = read_sql_table(query)

write_to_sql(df=df, table_name="SSC_AggInvBillBrandYM",
             url=url, user=user, password=password, mode="append")
