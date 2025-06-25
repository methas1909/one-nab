
import os
import paramiko
import pandas as pd
import geopandas as gpd
import fsspec
import zipfile
import geopandas as gpd
import tempfile
import os
from io import BytesIO
from shapely.geometry import Point
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, col, concat, lit, trim
import sys
from io import BytesIO

# Initialize SparkSession
spark = SparkSession.builder.config(
    "spark.sql.execution.arrow.enabled", "true").getOrCreate()

# Environment variables for SCP connection
scp_host = os.environ["SCP_HOST"]
scp_user = os.environ["SCP_USER"]
scp_pass = os.environ["SCP_PASS"]

# SQL Server connection details
url = os.environ["ONAB_MSSQL_URL"]
user = os.environ["ONAB_MSSQL_USER"]
password = os.environ["ONAB_MSSQL_PASSWORD"]
url_stg = os.environ["ONABSTG_MSSQL_URL"]
user_stg = os.environ["ONABSTG_MSSQL_USER"]
password_stg = os.environ["ONABSTG_MSSQL_PASSWORD"]

# File paths
remote_file_path = "/data/NAB/Excel/Prod/Postmix Master(DimLocation).xlsx"


# Function: Read Data from SQL Server using PySpark JDBC
def read_sql_table(query: str, url: str, user: str, password: str):
    """Reads a table from SQL Server as a PySpark DataFrame."""
    return (spark.read.format("jdbc")
            .option("url", url)
            .option("query", query)
            .option("user", user)
            .option("password", password)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load())

# Function: Write Data to SQL Server


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
    print(f'  Data inserted into {table_name} ({df.count()} rows)')

# Function: Download SSC_DimLocation Excel File via SCP


def SSC_DimLocationExcel():
    """Downloads SSC_DimLocation Excel via SCP directly to memory and saves to SQL Server."""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(scp_host, username=scp_user, password=scp_pass)

        with ssh.open_sftp() as sftp:
            print("  SCP Connection Successful!")
            with sftp.open(remote_file_path, "rb") as remote_file:
                file_buffer = BytesIO(remote_file.read())  # โหลดเข้า memory
            print("  File loaded into memory successfully.")
    except Exception as e:
        print(f"   SCP Error: {e}")
        return None

    try:
        df_pandas = pd.read_excel(file_buffer, engine="openpyxl")
        if df_pandas.empty:
            print("   No data found in the Excel file.")
            return None

        # Convert to PySpark DataFrame
        df_spark = spark.createDataFrame(df_pandas).withColumn(
            "PYLoadDate", current_timestamp())

        # Write to SQL Server
        write_to_sql(df=df_spark, table_name="SSC_DimLocationExcel",
                     url=url, user=user, password=password, mode="append")
        print(
            f"  Processed {df_spark.count()} records into SSC_DimLocationExcel")
        return df_spark
    except Exception as e:
        print(f"   Error reading Excel file from memory: {e}")
        return None

# Function: Download Shapefile via SCP


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

            # Load shapefile from extracted files
            shp_path = os.path.join(tmpdir, "th_tambon_boundary_v2.shp")
            gdf = gpd.read_file(shp_path,encoding='utf-8')
            gdf = gdf.to_crs(epsg=32647)
            return gdf

    except Exception as e:
        print(f" Error: {e}")
        return None

# Function to process geospatial data
def process_geospatial_data(df_location):
    """Processes geospatial data and performs a spatial join."""
    th_boundary = download_shapefile_via_scp()

    # Convert Latitude and Longitude to geometry
    df_location_pd = df_location.toPandas()
    df_location_pd["geometry"] = [Point(xy) for xy in zip(
        df_location_pd["Longitude"], df_location_pd["Latitude"])]

    # Create GeoDataFrame
    dt_point = gpd.GeoDataFrame(
        df_location_pd, geometry="geometry", crs="EPSG:4326")
    dt_point = dt_point.to_crs(epsg=32647)

    # Perform spatial join
    gdf_merged = gpd.sjoin(th_boundary, dt_point, how="right")

    # Clean up unnecessary columns
    gdf_merged = gdf_merged.drop(
        columns=["index_left", "id", "prov_idn", "amphoe_idn", "tambon_idn", "geometry"])

    # Rename columns
    gdf_merged.rename(columns={
        "p_name_t": "ProvinceTH",
        "p_name_e": "ProvinceEN",
        "s_region": "SRegion",
        "a_name_t": "AmphoeTH",
        "a_name_e": "AmphoeEN",
        "t_name_t": "TumbonTH",
        "t_name_e": "TumbonEN"
    }, inplace=True)

    return gdf_merged


query_location = """
SELECT [BranchCode], [OldBranchCode], [BranchDescThai], [BranchDescriptionThai],
       [BranchDescEng], [BranchDescriptionEng], [PlantCode], [PlantDescThai],
       [PlantDescriptionThai], [PlantDescEng], [PlantDescriptionEng], [CompanyCode],
       [CompanyDescThai], [CompanyDescriptionThai], [CompanyDescEng], [CompanyDescriptionEng],
       [RegionThaiBev], [RegionTBL], [RegionSSC], [SubRegionSSC], [Latitude], [Longitude],
       [ETLLoadData], GETDATE() AS PYLoadDate
FROM [SSC_DimLocation]
"""

query_excel = """
SELECT [League], [BranchCode], [Warehouse], [PxIncludeFlag], [RegionSale_Label]
FROM [SSC_DimLocationExcel]
"""
# Main script to read, process, and write data
df_excel_spark = SSC_DimLocationExcel()

df_location = read_sql_table(
    query=query_location, url=url_stg, user=user_stg, password=password_stg)

df_excel = read_sql_table(query=query_excel, url=url,
                          user=user, password=password)

gdf_merged = process_geospatial_data(df_location)

df_final = spark.createDataFrame(gdf_merged)


df_final = df_final.withColumn("BranchCode", col("BranchCode").cast("string"))
df_final = df_final.withColumn(
    "OldBranchCode", col("OldBranchCode").cast("string"))

df_excel = df_excel.withColumn(
    "BranchCode", trim(col("BranchCode").cast("string")))


join_condition = (
    (df_excel["BranchCode"] == df_final["BranchCode"]) |
    (df_excel["BranchCode"] == df_final["OldBranchCode"])
)

df_result = df_final.join(df_excel, join_condition, "left").select(
    df_final["*"],
    df_excel["Warehouse"],
    df_excel["RegionSale_Label"],
    df_excel["League"],
    df_excel["PxIncludeFlag"]
)


df_result = (df_result.withColumnRenamed("SRegion", "PointRegion")
             .withColumnRenamed("ProvinceTH", "PointProvinceTH")
             .withColumnRenamed("ProvinceEN", "PointProvinceEN")
             .withColumnRenamed("AmphoeTH", "PointAmphoeTH")
             .withColumnRenamed("AmphoeEN", "PointAmphoeEN")
             .withColumnRenamed("TumbonTH", "PointTambonTH")
             .withColumnRenamed("TumbonEN", "PointTambonEN"))

df_result = df_result.fillna({"Warehouse": "", "RegionSale_Label": ""})
df_result = df_result.withColumn(
    "RegionSale_Label", concat(lit("0"), col("RegionSale_Label")))

write_to_sql(df=df_result, table_name="SSC_DimLocation",
             url=url, user=user, password=password, mode="append")

print("  Data Processing Completed!")
