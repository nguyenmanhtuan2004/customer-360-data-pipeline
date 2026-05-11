from datetime import datetime, timedelta
import os
import sys
import argparse

# --- ĐỒNG BỘ PHIÊN BẢN PYTHON CHO SPARK ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType

# --- CONSTANTS / CONFIGURATIONS ---
SEARCH_LOG_PATH = "final_output_logsearch.parquet"
CONTENT_LOG_PATH = "final_output_logcontent.parquet"

OUTPUT_FACT = "Fact_Customer_360.parquet"
OUTPUT_DIM_USER = "Dim_User.parquet"
OUTPUT_DIM_CUST = "Dim_Cust.parquet"
OUTPUT_DIM_SERVICE = "Dim_Service.parquet"
OUTPUT_DIM_DATE = "Dim_Date.parquet"

def get_spark_session():
    """Khởi tạo Spark Session với RAM 8GB/4GB."""
    return SparkSession.builder \
        .appName("Optimized_Star_Schema_Modeling") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def create_obt(spark):
    """Tạo One Big Table (OBT) bằng cách gộp search và content logs."""
    print("Đang đọc và chuẩn hóa dữ liệu OBT...")
    df_search = spark.read.parquet(SEARCH_LOG_PATH) \
        .withColumnRenamed("user_id", "Profile_ID") \
        .withColumn("has_search", F.lit(True))

    df_content = spark.read.parquet(CONTENT_LOG_PATH) \
        .withColumnRenamed("Contract", "Profile_ID") \
        .withColumn("has_content", F.lit(True))

    df_obt = df_search.join(df_content, on="Profile_ID", how="outer")

    df_obt = df_obt.withColumn(
        "data_source",
        F.when(F.col("has_search").isNotNull() & F.col("has_content").isNotNull(), "both_search_and_content")
         .when(F.col("has_search").isNotNull(), "log_search")
         .when(F.col("has_content").isNotNull(), "log_content")
         .otherwise("unknown")
    ).drop("has_search", "has_content")

    return df_obt, df_content

def create_dim_user(spark, df_obt):
    """Tạo bảng Dim_User trực tiếp từ dữ liệu đã xử lý."""
    print("Đang bóc tách Dim_User...")
    return df_obt.select("Profile_ID").distinct()

def create_dim_cust(spark, df_content):
    """Tạo bảng Dim_Cust từ thông tin khách hàng trong content logs."""
    print("Đang bóc tách Dim_Cust...")
    return df_content.select("Profile_ID", "Active", "Taste").distinct()

def create_dim_service(spark):
    """Tạo bảng Dim_Service từ mapping cố định."""
    print("Đang tạo Dim_Service...")
    service_mapping = [
        (1, "CHANNEL", "Truyen Hinh", "Live TV"),
        (2, "RELAX",   "Giai Tri",   "Entertainment"),
        (3, "CHILD",   "Thieu Nhi",  "Kids"),
        (4, "FIMS",    "Phim Truyen", "Movies"),
        (5, "VOD",     "Phim Truyen", "Movies"),
        (6, "KPLUS",   "The Thao",   "Sports"),
        (7, "SPORT",   "The Thao",   "Sports"),
    ]
    return spark.createDataFrame(service_mapping, 
        ["service_key", "AppName", "Type", "category_group"])

def create_dim_date(spark, start="2022-06-01", end="2022-07-31"):
    """Tạo bảng Dim_Date (Chiều thời gian)."""
    print(f"Đang tạo Dim_Date từ {start} đến {end}...")
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    
    date_list = []
    curr = start_date
    while curr <= end_date:
        date_list.append((
            int(curr.strftime("%Y%m%d")),
            curr.strftime("%Y-%m-%d"),
            curr.year,
            curr.month,
            curr.day,
            curr.strftime("%B"),
            f"Q{(curr.month-1)//3 + 1}",
            curr.strftime("%A")
        ))
        curr += timedelta(days=1)
    
    return spark.createDataFrame(date_list, [
        "date_key", "full_date", "year", "month", "day", 
        "month_name", "quarter", "day_of_week"
    ])

def create_fact_customer_360(df_obt, dim_service, snapshot_date_key=20220731):
    """Tạo Fact_Customer_360 từ OBT và mapping service."""
    print(f"Đang tạo Fact_Customer_360 với date_key={snapshot_date_key}...")
    # Chuẩn hóa tên cột nếu có typo từ Step 1
    if "MostWacth" in df_obt.columns:
        df_obt = df_obt.withColumnRenamed("MostWacth", "MostWatch")
    
    if "MostWatch" not in df_obt.columns:
        print("⚠️ Cảnh báo: Không tìm thấy cột thông tin xem phim. Tạo cột trống.")
        df_obt = df_obt.withColumn("MostWatch", F.lit(None))

    service_lookup = dim_service.groupBy("Type").agg(F.first("service_key").alias("service_key"))
    
    return df_obt.join(F.broadcast(service_lookup), df_obt["MostWatch"] == service_lookup["Type"], "left") \
                 .drop("Type") \
                 .withColumn("date_key", F.lit(snapshot_date_key))

def save_star_schema(fact, d_user, d_cust, d_service, d_date):
    """Lưu tất cả các bảng ra định dạng Parquet (Parallel Write)."""
    print("Đang lưu dữ liệu Star Schema...")
    fact.write.mode("overwrite").parquet(OUTPUT_FACT)
    d_user.write.mode("overwrite").parquet(OUTPUT_DIM_USER)
    d_cust.write.mode("overwrite").parquet(OUTPUT_DIM_CUST)
    d_service.write.mode("overwrite").parquet(OUTPUT_DIM_SERVICE)
    d_date.write.mode("overwrite").parquet(OUTPUT_DIM_DATE)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end", type=str, default="20220714", help="Snapshot date key (YYYYMMDD)")
    args = parser.parse_args()
    
    snapshot_date_key = int(args.end)

    spark = get_spark_session()

    # ETL Pipeline
    df_obt, df_content = create_obt(spark)
    df_obt.cache()
    
    dim_user = create_dim_user(spark, df_obt)
    dim_cust = create_dim_cust(spark, df_content)
    dim_service = create_dim_service(spark)
    dim_date = create_dim_date(spark)
    
    fact_customer_360 = create_fact_customer_360(df_obt, dim_service, snapshot_date_key)
    
    # Validation & Save
    if fact_customer_360.count() > 0:
        print("==== Mẫu dữ liệu Fact_Customer_360 ====")
        fact_customer_360.show(5)
    else:
        print("⚠️ Cảnh báo: Fact Table không có dữ liệu để hiển thị.")
    
    save_star_schema(fact_customer_360, dim_user, dim_cust, dim_service, dim_date)
    print("-> XONG! Hệ thống Star Schema đã sẵn sàng.")

if __name__ == "__main__":
    main()