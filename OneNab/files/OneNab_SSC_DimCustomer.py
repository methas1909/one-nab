import os
import paramiko
import geopandas as gpd
import pandas as pd
import tempfile
import os
from pyspark.sql import SparkSession
from shapely.geometry import Point
from pyspark.sql import functions as F


spark = SparkSession.builder.config(
    "spark.sql.execution.arrow.enabled", "true").getOrCreate()


scp_host = os.environ["SCP_HOST"]
scp_user = os.environ["SCP_USER"]
scp_pass = os.environ["SCP_PASS"]


url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]


read_url = os.environ["ONABSTG_MSSQL_URL"]
read_user = os.environ["ONABSTG_MSSQL_USER"]
read_password = os.environ["ONABSTG_MSSQL_PASSWORD"]


dbtable = "SSC_DimCustomer"
BATCH_SIZE = 20000


def read_sql_table(query: str, url: str, user: str, password: str):
    return (spark.read
            .format("jdbc")
            .option("url", url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load())


def download_shapefile_via_scp():
    remote_folder = "/home/tsradmin/tsrproject/tsrapp/lib/python3.9/site-packages/airflow/NAB/Shapefiles/th_boundary_v2/"
    shapefile_parts = [
        "th_tambon_boundary_v2.shp",
        "th_tambon_boundary_v2.shx",
        "th_tambon_boundary_v2.dbf",
        "th_tambon_boundary_v2.prj"
    ]

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(scp_host, username=scp_user, password=scp_pass)

        with ssh.open_sftp() as sftp, tempfile.TemporaryDirectory() as tmpdir:
            print(" SCP Connection Successful!")

            for file_name in shapefile_parts:
                remote_path = os.path.join(remote_folder, file_name)
                local_path = os.path.join(tmpdir, file_name)
                with sftp.open(remote_path, "rb") as remote_file:
                    with open(local_path, "wb") as local_file:
                        local_file.write(remote_file.read())

            shp_path = os.path.join(tmpdir, "th_tambon_boundary_v2.shp")
            gdf = gpd.read_file(shp_path, encoding='utf-8')
            gdf = gdf.to_crs(epsg=32647)
            return gdf

    except Exception as e:
        print(f" Error: {e}")
        return None


th_boundary = download_shapefile_via_scp()

query_count = """
    SELECT COUNT(*) AS count FROM (
        SELECT Customer FROM [STAGE].[dbo].[SSC_DimCustomerOld]
        UNION ALL
        SELECT Customer FROM [STAGE].[dbo].[SSC_DimCustomer]
    ) AS SUBQUERY
"""


df_count = read_sql_table(
    query=query_count, user=read_user, url=read_url, password=read_password)


TOTAL_ROWS = df_count.collect()[0][0] if df_count.count() > 0 else 0
print(f" TOTAL_ROWS: {TOTAL_ROWS}")


if TOTAL_ROWS == 0:
    print(" No data available, exiting process.")
    spark.stop()
    exit(0)


num_batches = (TOTAL_ROWS + BATCH_SIZE - 1) // BATCH_SIZE


for batch_num in range(num_batches):
    offset = batch_num * BATCH_SIZE
    print(
        f"Processing batch {batch_num + 1}, Offset: {offset} to {offset + BATCH_SIZE}")

    query = f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (ORDER BY Customer, HANAFlag) AS rn FROM (
                SELECT 0 AS HANAFlag, '' AS OldCustomer,
                       [Customer], [CustomerDesc],
                       CONCAT([Customer],' - ', [CustomerDesc]) AS [CustomerDescription],
                       CAST([CustType] AS varchar) AS [CustType],
                       [Consent], [ConsentDate], NULL AS [VP], [CustAdj],
                       NULL AS [CustAdjDesc], NULL AS [CustAdjDescription],
                       [SpecialGrp1], [SpecialGrp2],
                       [BillSubDistrict], [BillDistrict], [BillProvince], [BillZipCode],
                       [WorkSubDistrict], [WorkDistrict], [WorkProvince], [WorkZipCode],
                       [LONGITUDE], [LATITUDE], [CustGrp1],
                       NULL AS [CustGroup], [ThaiBevGroup], [TaxNo],
                       NULL AS [FirstBillingDate], NULL AS [FirstBillingMonth], NULL AS [FirstBillingYear],
                       [ETLLoadData]
                FROM [STAGE].[dbo].[SSC_DimCustomerOld]

                UNION ALL

                SELECT 1 AS HANAFlag, [OldCustomer],
                       [Customer], [CustomerDesc], [CustomerDescription], [CustType],
                       [Consent], [ConsentDate], [VP], [CustAdj],
                       [CustAdjDesc], [CustAdjDescription],
                       [SpecialGrp1], [SpecialGrp2],
                       [BillSubDistrict], [BillDistrict], [BillProvince], [BillZipCode],
                       [WorkSubDistrict], [WorkDistrict], [WorkProvince], [WorkZipCode],
                       [LONGITUDE], [LATITUDE], [CustGrp1],
                       [CustGroup], [ThaiBevGroup], [TaxNo],
                       [FirstBillingDate], [FirstBillingMonth], [FirstBillingYear],
                       [ETLLoadData]
                FROM [STAGE].[dbo].[SSC_DimCustomer]
            ) AS unioned
        ) AS numbered
        WHERE rn BETWEEN {offset + 1} AND {offset + BATCH_SIZE}
    """

    branch_data = read_sql_table(
        query=query, user=read_user, url=read_url, password=read_password)
    print(f"Read rows: {branch_data.count()}")

    branch_data = branch_data.drop("rn")
    branch_data_pd = branch_data.toPandas()

    for col in ["LONGITUDE", "LATITUDE"]:
        branch_data_pd[col] = pd.to_numeric(
            branch_data_pd[col], errors="coerce")

    dt_geo = [Point(xy) for xy in zip(
        branch_data_pd["LONGITUDE"], branch_data_pd["LATITUDE"])]
    dt_point = gpd.GeoDataFrame(
        branch_data_pd, geometry=dt_geo, crs="EPSG:4326").to_crs(epsg=32647)

    dt_point = dt_point[dt_point.geometry.notnull()]
    dt_point = dt_point[dt_point.geometry.apply(lambda g: g.is_valid)]

    dt_point2 = gpd.sjoin(th_boundary, dt_point, how="right")

    drop_cols = ["index_left", "id", "prov_idn",
                 "amphoe_idn", "tambon_idn", "geometry"]
    dt_point2.drop(
        columns=[col for col in drop_cols if col in dt_point2.columns], inplace=True)

    dt_point2.rename(columns={
        "p_name_t": "PointProvinceTH", "p_name_e": "PointProvinceEN",
        "s_region": "PointRegion", "a_name_t": "PointAmphoeTH", "a_name_e": "PointAmphoeEN",
        "t_name_t": "PointTambonTH", "t_name_e": "PointTambonEN"
    }, inplace=True)

    DF_ReadResultA_spark = spark.createDataFrame(dt_point2)

    columns = [
        "HANAFlag", "OldCustomer", "Customer", "CustomerDesc", "CustomerDescription",
        "CustType", "Consent", "ConsentDate", "VP", "CustAdj", "CustAdjDesc",
        "CustAdjDescription", "SpecialGrp1", "SpecialGrp2", "BillSubDistrict", "BillDistrict",
        "BillProvince", "BillZipCode", "WorkSubDistrict", "WorkDistrict", "WorkProvince",
        "WorkZipCode", "LONGITUDE", "LATITUDE", "PointRegion", "PointProvinceTH", "PointProvinceEN",
        "PointAmphoeTH", "PointAmphoeEN", "PointTambonTH", "PointTambonEN", "CustGrp1", "CustGroup",
        "ThaiBevGroup", "TaxNo", "FirstBillingDate", "FirstBillingMonth", "FirstBillingYear",
        "ETLLoadData"
    ]

    column_types = {
        "FirstBillingDate": "date",
        "FirstBillingYear": "integer",
        "FirstBillingMonth": "string",
        "LONGITUDE": "double",
        "LATITUDE": "double",
    }

    for col_name in columns:
        cast_type = column_types.get(col_name, "string")
        DF_ReadResultA_spark = DF_ReadResultA_spark.withColumn(
            col_name, F.coalesce(F.col(col_name), F.lit(None)).cast(cast_type)
        )

    (DF_ReadResultA_spark.repartition(200).write.format("jdbc")
        .option("url", url)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("dbtable", "SSC_DimCustomer")
        .option("batchsize", BATCH_SIZE)
        .option("user", user)
        .option("password", password)
        .mode("append")
        .option("isolationLevel", "NONE")
        .save())

    print(f"Successfully written batch {batch_num + 1} to SQL Server.")
