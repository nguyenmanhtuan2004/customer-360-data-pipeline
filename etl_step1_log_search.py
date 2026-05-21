import os
import json
import time
import pandas as pd
from datetime import datetime, timedelta

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
import vertexai
from vertexai.generative_models import GenerativeModel

# --- CONSTANTS ---
GOOGLE_CREDENTIALS = r"bigdata-mapping-b6ba7074c7d7.json"
VERTEX_PROJECT = "bigdata-mapping"
VERTEX_LOCATION = "us-central1"
CLASSIFICATION_BATCH_SIZE = 100
API_COOLDOWN_SECONDS = 2
MAPPING_FILE = "mapping.csv"
MIN_KEYWORD_LENGTH = 3

import sys
# Đồng bộ phiên bản Python cho Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --- INITIALIZE SERVICES OPTIONALLY ---
if os.path.exists(GOOGLE_CREDENTIALS):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS
    try:
        vertexai.init(project=VERTEX_PROJECT, location=VERTEX_LOCATION)
        HAS_AI = True
        print("✅ Đã kết nối Vertex AI.")
    except Exception as e:
        print(f"⚠️ Không thể khởi tạo Vertex AI: {e}")
        HAS_AI = False
else:
    print(f"ℹ️ Không tìm thấy file {GOOGLE_CREDENTIALS}. Chạy chế độ offline (chỉ dùng mapping.csv).")
    HAS_AI = False

# 1. Khởi tạo Spark Session
spark = (
    SparkSession.builder
    .config("spark.driver.memory", "8g")
    .config("spark.executor.cores", 8)
    .getOrCreate()
)

def get_latest_date_in_path(path):
    """Tìm folder có tên ngày (YYYYMMDD) lớn nhất trong đường dẫn."""
    try:
        subdirs = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d)) and d.isdigit() and len(d) == 8]
        if not subdirs:
            return None
        return max(subdirs)
    except Exception:
        return None

def get_valid_paths(base_path, start_date, to_date):
    """Lọc các folder log theo ngày tồn tại trong khoảng [start_date, to_date]."""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(to_date, "%Y%m%d")
    delta_days = (end - start).days
    
    valid_paths = []
    for i in range(delta_days + 1):
        current_date_str = (start + timedelta(days=i)).strftime("%Y%m%d")
        folder_path = os.path.join(base_path, current_date_str)
        if os.path.exists(folder_path):
            valid_paths.append(folder_path)
    return valid_paths

def classify_batch(movie_list):
    """Phân loại danh sách keyword thành thể loại phim bằng Gemini AI, chia theo lô."""
    if not movie_list:
        return {}

    if HAS_AI:
        model = GenerativeModel("gemini-2.5-flash")
    results = {}

    for i in range(0, len(movie_list), CLASSIFICATION_BATCH_SIZE):
        batch = movie_list[i:i+CLASSIFICATION_BATCH_SIZE]
        total_batches = (len(movie_list) + CLASSIFICATION_BATCH_SIZE - 1) // CLASSIFICATION_BATCH_SIZE
        print(f"Đang phân loại lô {i//CLASSIFICATION_BATCH_SIZE + 1}/{total_batches} ({len(batch)} từ)...")
        
        prompt = f"""
    Bạn là một chuyên gia phân loại nội dung phim, chương trình truyền hình và các loại nội dung giải trí.  
    Bạn sẽ nhận một danh sách tên có thể viết sai, viết liền không dấu, viết tắt, hoặc chỉ là cụm từ liên quan đến nội dung.

    ⚠️ Nguyên tắc quan trọng:
    - Không được trả về "Other" nếu có thể đoán được dù chỉ một phần ý nghĩa.  
    - Luôn cố gắng sửa lỗi, nhận diện tên gần đúng hoặc đoán thể loại gần đúng.  
    - Nếu không chắc → chọn thể loại gần nhất (VD: từ mô tả tình cảm → Romance, tên địa danh thể thao → Sports, chương trình giải trí → Reality Show, v.v.)

    Nhiệm vụ của bạn:
    1. **Chuẩn hoá tên**: thêm dấu tiếng Việt nếu cần, tách từ, chỉnh chính tả (vd: "thuyếtminh" → "Thuyết minh", "tramnamu" → "Trăm năm hữu duyên", "capdoi" → "Cặp đôi").
    2. **Nhận diện tên hoặc ý nghĩa gốc gần đúng nhất**. Bao gồm:
    - Tên phim, series, show, chương trình
    - Quốc gia / đội tuyển (→ "Sports" hoặc "News")
    - Từ khoá mô tả nội dung (→ phân loại theo ý nghĩa, ví dụ "thuyếtminh" → "Other" hoặc "bigfoot" → "Horror")
    3. **Gán thể loại phù hợp nhất** trong các nhóm sau:  
    - Action  
    - Romance  
    - Comedy  
    - Horror  
    - Animation  
    - Drama  
    - C Drama  
    - K Drama  
    - Sports  
    - Music  
    - Reality Show  
    - TV Channel  
    - News  
    - Other

    Một số quy tắc gợi ý nhanh:
    - Có từ “VTV”, “HTV”, “Channel” → TV Channel  
    - Có “running”, “master key”, “reality” → Reality Show  
    - Quốc gia, CLB bóng đá, sự kiện thể thao → Sports hoặc News  
    - “sex”, “romantic”, “love” → Romance  
    - “potter”, “hogwarts” → Drama / Fantasy  
    - Tên phim Việt/Trung/Hàn → ưu tiên Drama / C Drama / K Drama

    Chỉ trả về **1 JSON object**.  
    Key = tên gốc trong danh sách.  
    Value = thể loại đã phân loại.

    Danh sách:
    {json.dumps(batch, ensure_ascii=False)} """ 

        if not HAS_AI:
            print("  -> Bỏ qua gọi AI (Offline mode).")
            for m in batch:
                results[m] = "Other"
            continue

        try:
            response = model.generate_content(prompt)
            text = response.text.strip()

            # Lấy JSON
            start_index, end_index = text.find("{"), text.rfind("}")
            if start_index == -1 or end_index == -1:
                print(f"  -> Lỗi: Không tìm thấy JSON hợp lệ trong lô này.")
                for m in batch:
                    results[m] = "Other"
                continue

            parsed = json.loads(text[start_index:end_index+1])

            parsed_lower_keys = {str(k).lower(): v for k, v in parsed.items()}
            for title in batch:
                results[title] = parsed_lower_keys.get(str(title).lower(), "Other")

        except Exception as e:
            print(f"  -> Lỗi ở lô {i//CLASSIFICATION_BATCH_SIZE + 1}:", e)
            for m in batch:
                results[m] = "Other"      
        time.sleep(API_COOLDOWN_SECONDS)
    return results

def clean_and_filter(df, month_prev, month_curr):
    """Làm sạch timestamp, chuẩn hóa keyword, lọc rác, và tách theo tháng so sánh."""
    df = (
        df.withColumn(
            "datetime_clean",
            F.trim(F.regexp_replace(F.col("datetime"), r"\s+(CH|SA)$", ""))
        )
        .withColumn(
            "month",
            F.month(
                F.coalesce(
                    F.try_to_timestamp(F.col("datetime_clean"), F.lit("yyyy-MM-dd HH:mm:ss.SSS")),
                    F.try_to_timestamp(F.col("datetime_clean"), F.lit("yyyy-MM-dd H:mm:ss.SSS")),
                    F.try_to_timestamp(F.col("datetime_clean"), F.lit("yyyy-MM-dd HH:mm:ss")),
                    F.try_to_timestamp(F.col("datetime_clean"), F.lit("yyyy-MM-dd H:mm:ss"))
                )
            )
        )
    )
    
    df = (
        df.filter(F.col("user_id").isNotNull() & F.col("keyword").isNotNull())
          .withColumn(
              "keyword",
              F.trim(F.regexp_replace(F.lower(F.col("keyword")), r"[^\p{L}\p{N}\s]", ""))
          )
          .filter(F.length(F.col("keyword")) >= MIN_KEYWORD_LENGTH)
    )
    
    df_prev = df.filter(F.col("month") == month_prev)
    df_curr = df.filter(F.col("month") == month_curr)
    return df_prev, df_curr

def get_top_keywords(df_prev, df_curr):
    """Lấy keyword được tìm kiếm nhiều nhất của mỗi user theo từng tháng, rồi join lại."""
    window_spec_prev = Window.partitionBy("user_id").orderBy(F.desc("count_prev"))
    window_spec_curr = Window.partitionBy("user_id").orderBy(F.desc("count_curr"))
    
    df_prev_top = (
        df_prev.groupBy("user_id", F.col("keyword").alias("most_searched_prev"))
        .agg(F.count("*").alias("count_prev"))
        .withColumn("rank", F.row_number().over(window_spec_prev))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    df_curr_top = (
        df_curr.groupBy("user_id", F.col("keyword").alias("most_searched_curr"))
        .agg(F.count("*").alias("count_curr"))
        .withColumn("rank", F.row_number().over(window_spec_curr))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    df = (
        df_prev_top.join(df_curr_top, on="user_id", how="full_outer")
        .fillna(0, subset=["count_prev", "count_curr"])
        .where(F.col("most_searched_prev").isNotNull() & F.col("most_searched_curr").isNotNull())
        .orderBy(F.desc("count_prev")) 
    )
    return df

def classify_keywords_from_df(unique_keywords_df, output_file=MAPPING_FILE):
    """Collect keyword từ DataFrame, gọi AI classify và lưu kết quả vào CSV."""
    if hasattr(unique_keywords_df, 'collect'):  # Spark DataFrame
        movie_list = [row["keyword"] for row in unique_keywords_df.collect()]
    else:  # pandas DataFrame hoặc list-like
        movie_list = unique_keywords_df["keyword"].tolist()

    print(f"Đang tiến hành phân loại cho {len(movie_list)} keyword...")
    classification = classify_batch(movie_list)
    df_classification = pd.DataFrame(list(classification.items()), columns=["keyword", "category"])
    df_classification.to_csv(output_file, index=False)

    print(f"Hoàn tất! File {output_file} đã được tạo.")
    return df_classification

def extract_unique_keywords(result_df, 
                            col_prev="most_searched_prev", 
                            col_count_prev="count_prev",
                            col_curr="most_searched_curr", 
                            limit_prev=10000,
                            limit_curr=10000,
                            offset_prev=3000,
                            offset_curr=3000):
    """Trích xuất keyword: Lấy các dòng tiếp theo (bỏ qua offset) bằng cách dùng Window function."""
    
    # Tháng trước: tính tổng, sort theo popularity (giảm dần) và lấy theo offset & limit
    window_prev = Window.orderBy(F.desc("total"))
    top_prev = (result_df.select(col_prev, col_count_prev)
                      .groupBy(col_prev)
                      .agg(F.sum(col_count_prev).alias("total"))
                      .withColumn("rn", F.row_number().over(window_prev))
                      .filter((F.col("rn") > offset_prev) & (F.col("rn") <= offset_prev + limit_prev))
                      .select(F.col(col_prev).alias("keyword")))
    
    # Tháng sau: giữ nguyên thứ tự tự nhiên (dùng monotonically_increasing_id)
    window_curr = Window.orderBy(F.monotonically_increasing_id())
    top_curr = (result_df.select(col_curr)
                      .withColumn("rn", F.row_number().over(window_curr))
                      .filter((F.col("rn") > offset_curr) & (F.col("rn") <= offset_curr + limit_curr))
                      .select(F.col(col_curr).alias("keyword")))
    
    return top_prev.union(top_curr).distinct()

def map_categories_to_result(df, 
                              col_prev="most_searched_prev",
                              col_curr="most_searched_curr",
                              col_classification_keyword="keyword",
                              col_category="category",
                              suffix_prev="_prev",
                              suffix_curr="_curr",
                              order_by_col="count_prev",
                              ascending=False):
    
    spark_classification = spark.read.csv(MAPPING_FILE, header=True)
   
    category_prev_name = f"{col_category}{suffix_prev}"
    category_curr_name = f"{col_category}{suffix_curr}"
    
    mapped_prev = df.join(
        spark_classification.select(
            F.col(col_classification_keyword).alias("kw_prev"),
            F.col(col_category).alias(category_prev_name)
        ),
        F.col(col_prev) == F.col("kw_prev"),
        "left"
    ).drop("kw_prev")
    
    final_result = mapped_prev.join(
        spark_classification.select(
            F.col(col_classification_keyword).alias("kw_curr"),
            F.col(col_category).alias(category_curr_name)
        ),
        F.col(col_curr) == F.col("kw_curr"),
        "left"
    ).drop("kw_curr")
    
    final_result = final_result.orderBy(F.desc(order_by_col) if not ascending else F.asc(order_by_col))
    
    return final_result

def classify_category_shift(df):
    """Phân loại hành vi dịch chuyển thể loại phim giữa tháng trước và tháng sau."""
    df = df.withColumn(
        "Trending_Type",
        F.when(
            F.col("category_prev") == F.col("category_curr"), "Unchanged").otherwise("Changed")
        ).withColumn(
            "Previous",
            F.when(F.col("category_prev") == F.col("category_curr"), "Unchanged").otherwise(
                F.concat_ws("-",F.col("category_prev"), F.col("category_curr"))
            )
        )
    return df

def save_data_parquet(result, save_path):
    """Ghi DataFrame ra file Parquet (overwrite)."""
    print(f"-- Lưu dữ liệu tại: {save_path}")
    result.write.mode("overwrite").parquet(save_path)


def load_parquet_logs(spark, paths):
    """Đọc và hợp nhất tất cả các file Parquet từ danh sách đường dẫn."""
    if not paths:
        return None
        
    print(f"Đọc file đầu tiên: {paths[0]}")
    try:
        df_main = spark.read.parquet(paths[0])
    except AnalysisException as e:
        print(f"Lỗi khi đọc file Parquet đầu tiên: {e}")
        return None

    for file_path in paths[1:]:
        print(f"ETL_TASK đang nạp file: {file_path}")
        try:
            df_new = spark.read.parquet(file_path)
            df_main = df_main.union(df_new)
        except AnalysisException as e:
            print(f"Bỏ qua lỗi đọc file {file_path}: {e}")
            
    return df_main

def maintask(path, start_date, to_date, output_path):
    """Luồng chính ETL Log Search: Load → Clean → Analyze → Map → Save."""
    print("------------- Bắt đầu luồng ETL HÀNH VI TÌM KIẾM --------------")
    
    # 1. Tự động phát hiện ngày mới nhất từ dữ liệu thực tế
    latest_data_date = get_latest_date_in_path(path)
    
    if latest_data_date:
        # Nếu tìm thấy dữ liệu, ta ghi đè dải ngày để đảm bảo lấy đủ 2 tháng so sánh
        # END = ngày mới nhất (VD: 20220714)
        dt_end = datetime.strptime(latest_data_date, "%Y%m%d")
        # START = ngày 1 của tháng trước (VD: 20220601)
        dt_start = (dt_end.replace(day=1) - timedelta(days=1)).replace(day=1)
        
        start_date = dt_start.strftime("%Y%m%d")
        to_date = latest_data_date
        print(f"📅 Chế độ tự động: Phát hiện dữ liệu từ {start_date} đến {to_date}")
    else:
        print(f"📅 Chế độ thủ công: Xử lý từ {start_date} đến {to_date}")

    valid_paths = get_valid_paths(path, start_date, to_date)
    
    if not valid_paths:
        print("❌ Không tìm thấy dữ liệu nào trong khoảng thời gian này.")
        return

    # Load data
    df = load_parquet_logs(spark, valid_paths)
    if df is None:
        return

    # Xác định tháng để filter trong Spark
    end_dt = datetime.strptime(to_date, "%Y%m%d")
    curr_m = end_dt.month
    prev_m = (end_dt.replace(day=1) - timedelta(days=1)).month
    
    print(f"--- Đang phân tích so sánh Tháng {prev_m} và Tháng {curr_m} ---")

    print("------------- Làm sạch dữ liệu và tách tháng --------------")
    df_prev, df_curr = clean_and_filter(df, prev_m, curr_m)
    
    print("------------- Lấy top keyword theo từng tháng --------------")
    result = get_top_keywords(df_prev, df_curr)
    
    print("------------- Trích xuất và Phân loại Keyword bằng AI --------------")
    unique_keywords_df = extract_unique_keywords(result)
    classify_keywords_from_df(unique_keywords_df)

    print("------------- Map với từ điển Category --------------")
    mapped_result = map_categories_to_result(result)
    
    print("------------- Phân tích hành vi dịch chuyển Category --------------")
    final_result = classify_category_shift(mapped_result)
    final_result.show(5)
    
    print("------------- Lưu file Output --------------")
    save_data_parquet(final_result, output_path)
    print("TẤT CẢ QUÁ TRÌNH ETL_SEARCH ĐÃ HOÀN TẤT!")
    
    return final_result

import argparse
import sys
from datetime import datetime, timedelta

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Log Search Pipeline")
    parser.add_argument("--start", help="Start date (YYYYMMDD)")
    parser.add_argument("--end", help="End date (YYYYMMDD)")
    args = parser.parse_args()

    # Mặc định là ngày hôm qua nếu không truyền tham số
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    
    BASE_PATH = "log_search"
    START_DATE = args.start if args.start else yesterday
    END_DATE = args.end if args.end else START_DATE # Nếu không có end, chạy đúng 1 ngày start
    OUTPUT_PATH = "final_output_logsearch.parquet"
    
    print(f"📅 Chế độ tự động: Xử lý dữ liệu từ {START_DATE} đến {END_DATE}")
    maintask(BASE_PATH, START_DATE, END_DATE, OUTPUT_PATH)