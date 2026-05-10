import os
import findspark
findspark.init()

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

import sys
# --- ĐỒNG BỘ PHIÊN BẢN PYTHON CHO SPARK ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 1. Khởi tạo Spark Session
spark = (
    SparkSession.builder
    .config("spark.driver.memory", "8g")
    .config("spark.executor.cores", 8)
    .getOrCreate()
)

def get_latest_date_in_path(path):
    """Tìm file .json có tên ngày (YYYYMMDD) lớn nhất trong đường dẫn."""
    try:
        files = [f.replace(".json", "") for f in os.listdir(path) if f.endswith(".json") and f.replace(".json", "").isdigit()]
        valid_dates = [f for f in files if len(f) == 8]
        if not valid_dates:
            return None
        return max(valid_dates)
    except Exception:
        return None

def category_AppName(df):
    """Phân loại AppName thành các Type tương ứng."""
    df = df.withColumn(
        "Type",
        F.when(F.col("AppName") == "CHANNEL", "Truyen Hinh")
        .when(F.col("AppName") == "RELAX", "Giai Tri")
        .when(F.col("AppName") == "CHILD", "Thieu Nhi")
        .when(F.col("AppName").isin("FIMS", "VOD"), "Phim Truyen")
        .when(F.col("AppName").isin("KPLUS", "SPORT"), "The Thao")
    )
    df = df.select('Contract', 'Type', 'TotalDuration')
    
    # Loại bỏ dữ liệu rác
    df = df.filter((F.col("Contract") != '0') & (F.col("Type").isNotNull()))
    return df

def most_watch(df):
    """Tìm thể loại có thời lượng xem nhiều nhất."""
    # Lấy giá trị lớn nhất trong các cột thể loại
    df = df.withColumn(
        "Max_Duration",
        F.greatest(
            F.coalesce(F.col("Giai Tri"), F.lit(0)),
            F.coalesce(F.col("Phim Truyen"), F.lit(0)),
            F.coalesce(F.col("The Thao"), F.lit(0)),
            F.coalesce(F.col("Thieu Nhi"), F.lit(0)),
            F.coalesce(F.col("Truyen Hinh"), F.lit(0))
        )
    )
    
    # Map giá trị lớn nhất ngược lại tên thể loại
    df = df.withColumn(
        "MostWacth",
        F.when(F.col("Max_Duration") == F.col("Truyen Hinh"), "Truyen Hinh")
        .when(F.col("Max_Duration") == F.col("Phim Truyen"), "Phim Truyen")
        .when(F.col("Max_Duration") == F.col("The Thao"), "The Thao")
        .when(F.col("Max_Duration") == F.col("Thieu Nhi"), "Thieu Nhi")
        .when(F.col("Max_Duration") == F.col("Giai Tri"), "Giai Tri")
    ).drop("Max_Duration")
    
    return df

def customer_taste(df):
    """Gom các thể loại khách hàng đã xem thành một chuỗi."""
    df = df.withColumn(
        "Taste",
        F.concat_ws("-",
            F.when(F.col("Giai Tri").isNotNull(), F.lit("Giai Tri")),
            F.when(F.col("Phim Truyen").isNotNull(), F.lit("Phim Truyen")),
            F.when(F.col("The Thao").isNotNull(), F.lit("The Thao")),
            F.when(F.col("Thieu Nhi").isNotNull(), F.lit("Thieu Nhi")),
            F.when(F.col("Truyen Hinh").isNotNull(), F.lit("Truyen Hinh"))
        )
    )
    return df

def generate_range_date(start_date, end_date):
    """Tạo danh sách các ngày dưới dạng list string từ start_date đến end_date."""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    
    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
        
    return date_list

def find_active(df):
    """Gom nhóm theo Contract và phân loại mức độ Active."""
    windowspec = Window.partitionBy("Contract")
    df = df.withColumn("Active_Count", F.count("Date").over(windowspec))
    df = df.withColumn("Active", F.when(F.col("Active_Count") > 5, "High").otherwise("Low"))
    
    # Tính tổng lại duration của từng ngày thành bảng report cuối cùng
    df_agg = df.groupBy("Contract").agg(
        F.sum("Giai Tri").alias("Total_Giai_Tri"),
        F.sum("Phim Truyen").alias("Total_Phim_Truyen"),
        F.sum("The Thao").alias("Total_The_Thao"),
        F.sum("Thieu Nhi").alias("Total_Thieu_Nhi"),
        F.sum("Truyen Hinh").alias("Total_Truyen_Hinh"),
        F.first("MostWacth").alias("MostWacth"),
        F.first("Taste").alias("Taste"),
        F.first("Active").alias("Active")
    )
    return df_agg

def ETL_1_DAY(path, date_str):
    """Luồng ETL xử lý cho 1 ngày."""
    print(f'-- Đang xử lý file: {date_str}.json')
    filepath = os.path.join(path, f"{date_str}.json")
    
    # Bỏ qua nếu file không tồn tại
    if not os.path.exists(filepath):
         print(f'-- File {filepath} không tồn tại, bỏ qua.')
         return None
         
    df = spark.read.json(filepath)
    
    # Lấy field _source (nếu cấu trúc file dạng elk/elasticsearch log)
    if "_source" in df.columns:
        df = df.select("_source.*")
        
    df = category_AppName(df)
    
    # Pivot dữ liệu: Contract làm index, Type làm cols
    df = df.groupBy("Contract").pivot("Type").sum("TotalDuration")
    
    df = most_watch(df)
    df = customer_taste(df)
    df = df.withColumn("Date", F.to_date(F.lit(date_str), "yyyyMMdd"))
    
    return df

def save_to_parquet(df, output_path):
    """Ghi dữ liệu kết quả ra định dạng Parquet - Format ưu tiên của BigData"""
    print(f'-- Lưu dữ liệu tại: {output_path}')
    df.write.mode("overwrite").parquet(output_path)

def maintask(path, output_path, start_date, end_date):
    """Luồng chính ETL Log Content: Load → Clean → Analyze → Save."""
    print("------------- Bắt đầu luồng ETL LOG CONTENT --------------")
    
    # 1. Tự động phát hiện ngày mới nhất từ dữ liệu thực tế
    latest_data_date = get_latest_date_in_path(path)
    
    if latest_data_date:
        # Nếu tìm thấy dữ liệu, ta ghi đè dải ngày để đảm bảo lấy đủ 2 tháng (phục vụ Active/Loyalty)
        dt_end = datetime.strptime(latest_data_date, "%Y%m%d")
        dt_start = (dt_end.replace(day=1) - timedelta(days=1)).replace(day=1)
        
        start_date = dt_start.strftime("%Y%m%d")
        end_date = latest_data_date
        print(f"📅 Chế độ tự động: Phát hiện dữ liệu từ {start_date} đến {end_date}")
    else:
        print(f"📅 Chế độ thủ công: Xử lý từ {start_date} đến {end_date}")

    date_list = generate_range_date(start_date, end_date)
    final_df = None
    
    for date_str in date_list:
        df_day = ETL_1_DAY(path, date_str)
        if df_day is not None:
             if final_df is None:
                 final_df = df_day
             else:
                 # Union dữ liệu các ngày lại
                 final_df = final_df.unionByName(df_day, allowMissingColumns=True)

    if final_df is not None:
        print('-----------------------------')
        print('Tổng hợp mức độ Active và Tách dòng theo Contract...')
        final_result = find_active(final_df)
        
        print('-----------------------------')
        print('SHOWING PREVIEW BẢNG LOG_CONTENT')
        final_result.show(truncate=False)
        
        # Lưu kết quả
        save_to_parquet(final_result, output_path)
        print("TẤT CẢ QUÁ TRÌNH ETL_CONTENT ĐÃ HOÀN TẤT!")
        return final_result
    else:
        print("Không có dữ liệu để xử lý trong giai đoạn này.")
        return None

import argparse
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Log Content Pipeline")
    parser.add_argument("--start", help="Start date (YYYYMMDD)")
    parser.add_argument("--end", help="End date (YYYYMMDD)")
    args = parser.parse_args()

    # Mặc định là ngày hôm qua nếu không truyền tham số
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    
    BASE_PATH = "log_content"  
    OUTPUT_PATH = "final_output_logcontent.parquet"
    START_DATE = args.start if args.start else yesterday
    END_DATE = args.end if args.end else START_DATE
    
    print(f"📅 Chế độ tự động: Xử lý dữ liệu từ {START_DATE} đến {END_DATE}")
    df_result = maintask(BASE_PATH, OUTPUT_PATH, START_DATE, END_DATE)