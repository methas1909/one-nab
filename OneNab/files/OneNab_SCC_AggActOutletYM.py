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
def SCC_AggActOutletYM(tbname):
    dflist = []
    conn = engine.connect().execution_options(stream_results=True)

    SQL = """
          WITH 
      CF_Channel AS
      (
      SELECT ChannelMarge,Value2 AS PostMixFlag
      FROM SSC_DimChannel CN
          INNER JOIN TEST_CONFIGTABLE CF ON CN.SalesOfficeDesc = CF.Value1
      WHERE SalesOfficeDesc IN ('Direct Sales','Presales','Online','Post-Mix','Vending')
      ),
      CF_ReasonCode AS
      (
      SELECT ReasonCode
      FROM SSC_DimReason
      WHERE ReasonCode = '' 
      ),

      TotalOutlet AS
      (
      SELECT FORMAT(DATEADD(MONTH,+1,(CONVERT(date, CONCAT(YM,'01')))),'yyyyMM') AS YM, Branch, Route,ShopType
        , COUNT(DISTINCT VP.CUSTOMER)  AS TotalOutlet
      FROM SSC_FactCustPerMonPlan VP
          LEFT JOIN SSC_DimCustomer CU ON HANAFlag = 1 AND VP.Customer = CU.Customer
          LEFT JOIN SSC_DimCustGrp1 CG ON CU.CustGrp1 = CG.CustGrp
      GROUP BY FORMAT(DATEADD(MONTH,+1,(CONVERT(date, CONCAT(YM,'01')))),'yyyyMM'),Branch,Route,ShopType
      ),

      ActiveCustomer AS
      (
      SELECT  YM,INV.Customer,ShopType, PackagingType,Branch
            , CASE WHEN BrandDesc = 'Crystal' THEN  'Crystal'
                WHEN BrandDesc = 'Est' THEN  'Est'
            WHEN BrandDesc = 'Oishi' THEN  'Oishi'
            WHEN BrandDesc = 'Oishi Chakulza' THEN  'Oishi Chakulza'
            WHEN BrandDesc = 'Oishi Green Tea' THEN  'Oishi Green Tea'
            WHEN BrandDesc = 'Wrangyer' THEN  'Wrangyer' 
            ELSE 'Other Brand' END AS Brand  
          
      FROM SSC_FactAllDataInvoice INV 
          INNER JOIN CF_ReasonCode CR ON INV.ReasonCode = CR.ReasonCode
        INNER JOIN CF_Channel CC ON INV.ChannelMarge = CC.ChannelMarge
        LEFT JOIN SSC_DimLocation LO ON INV.Branch = LO.BranchCode
        LEFT JOIN SSC_DimMaterial MT ON INV.Material = MT.Material 
        LEFT JOIN SSC_DimCustomer CU ON INV.HANAFlag = CU.HANAFlag AND INV.Customer = CU.Customer
        LEFT JOIN SSC_DimCustGrp1 CG ON CU.CustGrp1 = CG.CustGrp
        
      WHERE BType = 'TaxInv'
            AND MaterialType = 'Z31' AND MatType1Id IN (1,8) AND PackagingType IN ('NR','RB') AND GroupPostMix <> 'PostMix'
          AND ShopType NOT IN ('CVM','Employee') AND CU.SpecialGrp1 IN ('','Other') 
          AND LEFT(INV.Customer,1) <> 'E' AND CustGrp NOT IN (-2,131,132,134,139,340,345)
          AND NOT (ISNULL(LO.PxIncludeFlag,0) = 0 AND PostMixFlag = 'Post-Mix') 

      GROUP BY YM,INV.Customer,ShopType,PackagingType,Branch
            ,CASE WHEN BrandDesc = 'Crystal' THEN  'Crystal'
                WHEN BrandDesc = 'Est' THEN  'Est'
            WHEN BrandDesc = 'Oishi' THEN  'Oishi'
            WHEN BrandDesc = 'Oishi Chakulza' THEN  'Oishi Chakulza'
            WHEN BrandDesc = 'Oishi Green Tea' THEN  'Oishi Green Tea'
            WHEN BrandDesc = 'Wrangyer' THEN  'Wrangyer' 
            ELSE 'Other Brand' END  
      ),

      BrandOutlet AS
      (
      SELECT  CONCAT(YM,'01') AS DateKey
            , ISNULL(CAST(RO.BranchVL AS varchar),'') AS BranchCode
          , ISNULL(INV.VL,'') AS RouteCode
          , ShopType
          , COUNT(DISTINCT (CASE WHEN BrandDesc = 'Est' THEN INV.Customer END)) AS EstActiveOutletVL
          , COUNT(DISTINCT (CASE WHEN BrandDesc = 'Crystal' THEN INV.Customer END))  AS CrystalActiveOutletVL
          , COUNT(DISTINCT (CASE WHEN BrandDesc LIKE 'Oishi%' THEN INV.Customer END)) AS OishiActiveOutletVL
          , COUNT(DISTINCT (CASE WHEN BrandDesc LIKE 'Oishi Green Tea' THEN INV.Customer END)) AS OishiGreenTeaActiveOutletVL
          , COUNT(DISTINCT (CASE WHEN BrandDesc LIKE 'Oishi Chakulza' THEN INV.Customer END)) AS OishiChakulzaActiveOutletVL 
          , COUNT(DISTINCT (CASE WHEN BrandDesc = 'Wrangyer' THEN INV.Customer END)) AS WrangyerActiveOutletVL
          , COUNT(DISTINCT (CASE WHEN CG.ShopType = 'FSR' AND PackagingType = 'RB' THEN INV.Customer END)) AS FSRRBActiveOutletVL
          , COUNT(DISTINCT INV.Customer) AS ActiveBrandOutletVL
          , COUNT(DISTINCT CONCAT(INV.Customer,BrandDesc)) AS NoOfBrandOutletVL
      , COUNT(DISTINCT (CASE WHEN LEFT(BrandDesc,5) <> 'Oishi' THEN INV.Customer END)) ActiveBrandOutletVLExOishi
        
      FROM SSC_FactAllDataInvoice INV  
          INNER JOIN CF_ReasonCode CR ON INV.ReasonCode = CR.ReasonCode
        INNER JOIN CF_Channel CC ON INV.ChannelMarge = CC.ChannelMarge
        LEFT JOIN SSC_DimRoute RO ON INV.VL = RO.Route
        LEFT JOIN SSC_DimLocation LO ON RO.BranchVL = LO.BranchCode
        LEFT JOIN SSC_DimMaterial MT ON INV.Material = MT.Material 
        LEFT JOIN SSC_DimChannel CH ON INV.ChannelMarge = CH.ChannelMarge
        LEFT JOIN SSC_DimCustomer CU ON INV.HANAFlag = CU.HANAFlag AND INV.Customer = CU.Customer
        LEFT JOIN SSC_DimCustGrp1 CG ON CU.CustGrp1 = CG.CustGrp
        
      WHERE BType = 'TaxInv'
            AND MaterialType = 'Z31' AND MatType1Id IN (1,8) AND PackagingType IN ('NR','RB') AND GroupPostMix <> 'PostMix'
          AND ShopType NOT IN ('CVM','Employee') AND CU.SpecialGrp1 IN ('','Other') 
          AND LEFT(INV.Customer,1) <> 'E' AND CustGrp NOT IN (-2,131,132,134,139,340,345)
          AND NOT(ISNULL(LO.PxIncludeFlag,0) = 0 AND PostMixFlag = 'Post-Mix')
          
      GROUP BY YM, ISNULL(CAST(RO.BranchVL AS varchar),''), ISNULL(INV.VL,''),ShopType
      ),

      ActiveOutlet AS
      (
      SELECT  CONCAT(COALESCE(VP.YM,OL.YM,INV.YM),'01') AS DateKey
            , COALESCE(VP.Branch,OL.Branch,INV.Branch) AS BranchCode
          , CASE WHEN VP.Customer IS NOT NULL THEN ISNULL(VP.Route,OL.Route) ELSE '' END AS RouteCode, INV.ShopType
          , MAX(TotalOutlet) AS TotalOutletVP  
          , COUNT(DISTINCT INV.Customer) AS ActiveOutletVP
      FROM SSC_FactCustPerMonPlan VP    
          LEFT JOIN SSC_DimCustomer CU ON HANAFlag = 1 AND VP.Customer = CU.Customer
        FULL JOIN ActiveCustomer INV ON VP.YM = INV.YM AND INV.Customer = VP.Customer  
        FULL JOIN TotalOutlet OL ON VP.YM = OL.YM AND VP.Branch = OL.Branch AND VP.Route = OL.Route AND INV.ShopType = OL.ShopType
      WHERE INV.ShopType IS NOT NULL OR OL.ShopType IS NOT NULL
      GROUP BY CONCAT(COALESCE(VP.YM,OL.YM,INV.YM),'01')
            , COALESCE(VP.Branch,OL.Branch,INV.Branch)
          , CASE WHEN VP.Customer IS NOT NULL THEN ISNULL(VP.Route,OL.Route) ELSE '' END, INV.ShopType
      ),
      MergeDimension AS
      (
      SELECT DateKey,BranchCode,RouteCode,ShopType FROM ActiveOutlet
      UNION
      SELECT DateKey,BranchCode,RouteCode,ShopType FROM BrandOutlet
      )
      SELECT MAIN.DateKey, MAIN.BranchCode, MAIN.RouteCode,MAIN.ShopType
          , SUM(TotalOutletVP) AS TotalOutletVP
        , SUM(ActiveOutletVP) AS ActiveOutletVP
        , SUM(EstActiveOutletVL) AS EstActiveOutletVL
        , SUM(CrystalActiveOutletVL) AS CrystalActiveOutletVL
        , SUM(OishiActiveOutletVL) AS OishiActiveOutletVL
        , SUM(OishiGreenTeaActiveOutletVL) AS OishiGreenTeaActiveOutletVL
        , SUM(OishiChakulzaActiveOutletVL) AS OishiChakulzaActiveOutletVL
        , SUM(WrangyerActiveOutletVL) AS WrangyerActiveOutletVL
        , SUM(FSRRBActiveOutletVL) AS FSRRBActiveOutletVL
        , SUM(ActiveBrandOutletVL) AS ActiveBrandOutletVL	  
        , SUM(NoOfBrandOutletVL) AS NoOfBrandOutletVL 
        , GETDATE() AS PYLoadDate
      , SUM(ActiveBrandOutletVLExOishi) AS ActiveBrandOutletVLExOishi
      FROM MergeDimension MAIN
          LEFT JOIN ActiveOutlet AO ON MAIN.DateKey = AO.DateKey AND MAIN.BranchCode = AO.BranchCode AND MAIN.RouteCode = AO.RouteCode AND MAIN.ShopType = AO.ShopType
        LEFT JOIN BrandOutlet BO  ON MAIN.DateKey = BO.DateKey AND MAIN.BranchCode = BO.BranchCode AND MAIN.RouteCode = BO.RouteCode AND MAIN.ShopType = BO.ShopType
        WHERE MAIN.DateKey >= Cast (Concat(FORMAT((DATEADD(mm, DATEDIFF(mm, 0, getdate()) - 0, 0)  ),'yyyyMM'),'01') AS INT)
      GROUP BY MAIN.DateKey, MAIN.BranchCode, MAIN.RouteCode,MAIN.ShopType
      ORDER BY MAIN.DateKey, MAIN.BranchCode, MAIN.RouteCode,MAIN.ShopType
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
    tbnameA = 'SCC_AggActOutletYM'
    DF_ReadResultA = SCC_AggActOutletYM(tbnameA)


