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
def SC_AggVisitCallYM(tbname):
    dflist = []
    conn = engine.connect().execution_options(stream_results=True)

    SQL = """
            WITH
                    PlanVisit AS
                    (
                    SELECT  CASE WHEN SalesType1 = 'PS' THEN FORMAT(DATEADD(DAY,1,AssignedDate),'yyyyMM')
                                ELSE FORMAT(AssignedDate,'yyyyMM')
                                END AS DateKey
                        , Branch, VP.Route, VP.Salesman, Customer
                        , COUNT(*) AS NoOfVisit

                    FROM SSC_FactVisitList VP
                        LEFT JOIN SSC_DimRoute RO ON VP.Route = RO.Route
                    WHERE CASE WHEN SalesType1 = 'PS' THEN FORMAT(DATEADD(DAY,1,AssignedDate),'yyyyMM')
                                            ELSE FORMAT(AssignedDate,'yyyyMM')
                                            END >=  FORMAT((DATEADD(mm, DATEDIFF(mm, 0, getdate()) - 2, 0)  ),'yyyyMM')

                    GROUP BY CASE WHEN SalesType1 = 'PS' THEN FORMAT(DATEADD(DAY,1,AssignedDate),'yyyyMM')
                                ELSE FORMAT(AssignedDate,'yyyyMM') END 
                        , Branch, VP.Route, VP.Salesman, Customer
                    ),

                    ActualVisit AS
                    (
                    SELECT  CASE WHEN SalesType1 = 'PS' THEN FORMAT(DATEADD(DAY,1,VisitDate),'yyyyMM')
                                ELSE FORMAT(VisitDate,'yyyyMM')
                                END AS DateKey
                        , Branch, AV.Route, AV.Salesman, Customer
                        , COUNT(*) AS NoOfVisit

                    FROM SSC_FactActualVisit AV
                        LEFT JOIN SSC_DimRoute RO ON AV.Route = RO.Route
                                WHERE CASE WHEN SalesType1 = 'PS' THEN FORMAT(DATEADD(DAY,1,VisitDate),'yyyyMM')
                                            ELSE FORMAT(VisitDate,'yyyyMM')
                                            END >=  FORMAT((DATEADD(mm, DATEDIFF(mm, 0, getdate()) - 2, 0)  ),'yyyyMM')

                    GROUP BY CASE WHEN SalesType1 = 'PS' THEN FORMAT(DATEADD(DAY,1,VisitDate),'yyyyMM')
                                ELSE FORMAT(VisitDate,'yyyyMM') END 
                        , Branch, AV.Route, AV.Salesman, Customer
                    ),

                    AllList AS
                    ( SELECT Branch, Route, Salesman, Customer, DateKey FROM ActualVisit 
                    UNION 
                    SELECT Branch, Route, Salesman, Customer, DateKey FROM PlanVisit )

                    SELECT AL.Branch AS BranchCode, AL.Route AS RouteCode, AL.Salesman, CONCAT(AL.DateKey,'01') AS DateKey
                        , COUNT(DISTINCT PV.Customer) AS CustomerPlanVisit
                        , COUNT(DISTINCT AV.Customer) AS CustomerActualVisit
                        , COUNT(DISTINCT CASE WHEN PV.Customer IS NOT NULL AND AV.Customer IS NOT NULL THEN AL.Customer END) CustomerVisitOnPlan
                        , SUM(ISNULL(PV.NoOfVisit,0)) AS NoOfPlanVisit
                        , SUM(ISNULL(AV.NoOfVisit,0)) AS NoOfActualVisit
                        , GETDATE() AS PYLoadDate
                    FROM AllList AL
                        LEFT JOIN PlanVisit PV ON AL.DateKey=PV.DateKey AND AL.Branch=PV.Branch AND AL.Customer=PV.Customer AND AL.Route=PV.Route AND AL.Salesman=PV.Salesman
                        LEFT JOIN ActualVisit AV ON AL.DateKey=AV.DateKey AND AL.Branch=AV.Branch AND AL.Customer=AV.Customer AND AL.Route=AV.Route AND AL.Salesman=AV.Salesman
                    GROUP BY AL.Branch, AL.Route, AL.Salesman ,AL.DateKey
                    ORDER BY AL.Branch, AL.Route, AL.Salesman ,AL.DateKey
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
    tbnameA = 'SSC_AggVisitCallYM'
    DF_ReadResultA = SC_AggVisitCallYM(tbnameA)


