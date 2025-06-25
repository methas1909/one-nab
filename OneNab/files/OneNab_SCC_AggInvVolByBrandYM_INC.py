import pandas as pd
import os
import sqlalchemy
from sqlalchemy import event
from pyspark.sql import SparkSession

# สร้าง SparkSession
spark = SparkSession.builder.getOrCreate()

# Environment Variables
url = os.environ["ONAB_MSSQL_URL"]
mssql_user = os.environ["ONAB_MSSQL_USER"]
sql_password = os.environ["ONAB_MSSQL_PASSWORD"]

# Connection String
conn_str = f"mssql+pymssql://{mssql_user}:{sql_password}@10.7.54.203:1433/ONENAB"
engine = sqlalchemy.create_engine(conn_str, encoding="utf-8")

# Function to write chunk to SQL Server
def write_V1(df_Write, tbname, batch_no, rowcount):
    print(
        f"write_to {tbname} - Batch {batch_no}: {len(df_Write)} rows, Total: {rowcount + len(df_Write)} rows -- ")
    df_Write.to_sql(tbname, engine, index=False, if_exists="append")
    print(
        f'-- Inserted into {tbname} - Batch {batch_no}: {len(df_Write)} rows, Total: {rowcount + len(df_Write)} rows --')

# Main batch processing function
def SCC_AggInvVolByBrandYM(tbname):
    dflist = []
    conn = engine.connect().execution_options(stream_results=True)

    SQL = """
			WITH 
				CF_ReasonCode AS
				(
				SELECT ReasonCode
				FROM SSC_DimReason
				WHERE LEFT(ReasonCode,1) <> '6' 
				),
				CF_Customer AS
				(
				SELECT *
				FROM SSC_DimCustomer
				WHERE CustGrp1 NOT IN (-2,131,132,134,139,340,345)
				),
				LastDayOfMonth AS
				(
				SELECT  MAX(DateKey) AS LastDayOfMonth
				FROM SSC_FactAllDataInvoice
				GROUP BY YM
				)
				SELECT  INV.HANAFlag
					, CONCAT(INV.YM,'01') AS DateKey
					, INV.Branch AS BranchCode
					, INV.Route AS RouteCode
					, Salesman
					, ChannelMarge AS ChannelSalesOffice
					, INV.Customer AS CustomerCode
					, DataSource AS DataSource 
					, BasicMaterial  AS BasicMaterial 
					, BrandDesc
					, CASE WHEN BrandDesc = 'Crystal' THEN  'Crystal'
							WHEN BrandDesc = 'Est' THEN  'Est'
							WHEN BrandDesc = 'Oishi' THEN  'Oishi'
							WHEN BrandDesc = 'Oishi Chakulza' THEN  'Oishi Chakulza'
							WHEN BrandDesc = 'Oishi Green Tea' THEN  'Oishi Green Tea'
							WHEN BrandDesc = 'Wrangyer' THEN  'Wrangyer' 
							ELSE 'Other Brand' END AS Brand
					, MatType1
					, BType AS BillType
					, SUM(SoldSingle) AS SoldSingle
					, SUM(SoldCase) AS SoldCase
					, SUM(CASE WHEN LD.LastDayOfMonth IS NOT NULL THEN SoldCase ELSE 0 END) AS SoldCaseLastDay
					, SUM(SoldBaseQty) AS SoldBaseQty
					, SUM((SoldBaseQty * Millilitre)/1000) AS SoldLitre
					, SUM(FreeSingle) AS FreeSingle
					, SUM(FreeCase) AS FreeCase
					, SUM(CASE WHEN LD.LastDayOfMonth IS NOT NULL THEN FreeCase ELSE 0 END) AS FreeCaseLastDay
					, SUM(FreeBaseQty) AS FreeBaseQty
					, SUM((FreeBaseQty * Millilitre)/1000) AS FreeLitre
					, SUM(NetItemAmt) AS NetItemAmt
					, SUM(VatAmt) AS VatAmt
					, SUM(TotalAmt) AS TotalAmount
					, SUM(Discount) AS Discount
					, GETDATE() AS PYLoadDate
				FROM SSC_FactAllDataInvoice INV
					INNER JOIN CF_ReasonCode CR ON INV.ReasonCode = CR.ReasonCode    
					INNER JOIN CF_Customer CU ON INV.HANAFlag = CU.HANAFlag AND INV.Customer = CU.Customer
					LEFT JOIN SSC_DimMaterial MT ON INV.Material = MT.Material
					LEFT JOIN LastDayOfMonth LD ON INV.DateKey = LD.LastDayOfMonth
				WHERE MaterialType = 'Z31' 
				AND MatType1Id IN (1,8) 
				AND PackagingType IN ('NR','RB') 
				AND GroupPostMix <> 'PostMix'
				AND YM >= Cast (FORMAT((DATEADD(mm, DATEDIFF(mm, 0, getdate()) - 5, 0)  ),'yyyyMM') AS INT)
				GROUP BY INV.HANAFlag, CONCAT(YM,'01'), Branch, Route, Salesman, INV.Customer, ChannelMarge, DataSource, BType, BasicMaterial, MatType1,BrandDesc
					, CASE WHEN BrandDesc = 'Crystal' THEN  'Crystal'
							WHEN BrandDesc = 'Est' THEN  'Est'
							WHEN BrandDesc = 'Oishi' THEN  'Oishi'
							WHEN BrandDesc = 'Oishi Chakulza' THEN  'Oishi Chakulza'
							WHEN BrandDesc = 'Oishi Green Tea' THEN  'Oishi Green Tea'
							WHEN BrandDesc = 'Wrangyer' THEN  'Wrangyer' 
							ELSE 'Other Brand' END 
				ORDER BY CONCAT(YM,'01'), Branch, Route, Salesman, INV.Customer, ChannelMarge, DataSource, BasicMaterial
    """

    batch_no = 1
    rowcount = 0
    csize = 10000

    print(f'-- Completed inserting into {tbname}, Total Rows: {rowcount} --')

    for chunk in pd.read_sql_query(SQL, conn, chunksize=csize):
        print(f'Processing batch: {batch_no}')
        write_V1(chunk, tbname, batch_no, rowcount)
        dflist.append(chunk)
        batch_no += 1
        rowcount += len(chunk)

    final_df = pd.concat(dflist, ignore_index=True)
    print(f'-- Completed inserting into {tbname}, Total Rows: {rowcount} --')
    return final_df


# Execution
if __name__ == "__main__":
    tbnameA = 'SCC_AggInvVolByBrandYM'
    DF_ReadResultA = SCC_AggInvVolByBrandYM(tbnameA)
