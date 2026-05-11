import os
import sys
import time
import argparse
import logging
from datetime import datetime, timedelta

# --- ĐỒNG BỘ PHIÊN BẢN PYTHON CHO SPARK ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_latest_date_in_path(base_path):
    """Dò tìm ngày mới nhất trong các folder con (YYYYMMDD)."""
    try:
        search_path = os.path.join(base_path, "log_search")
        content_path = os.path.join(base_path, "log_content")
        all_dates = []
        for p in [search_path, content_path]:
            if os.path.exists(p):
                dates = [d for d in os.listdir(p) if d.isdigit() and len(d) == 8]
                all_dates.extend(dates)
        return max(all_dates) if all_dates else None
    except Exception:
        return None

def run_script(script_name, args=""):
    """Kích hoạt 1 Task trong luồng Data Pipeline (tương tự Airflow BashOperator)"""
    full_command = f"python {script_name} {args}"
    logging.info(f"🟢 ĐANG KÍCH HOẠT TASK: {full_command}")
    
    exit_code = os.system(full_command)
    
    if exit_code != 0:
        logging.error(f"❌ Task {script_name} thất bại với exit code {exit_code}!")
        logging.error("🛑 Dừng toàn bộ Pipeline để bảo vệ dữ liệu Cloud.")
        exit(1)
        
    logging.info(f"✅ HOÀN THÀNH TASK: {script_name}")

def main():
    parser = argparse.ArgumentParser(description="Main Data Pipeline Orchestrator")
    # Tham số chung (cho chạy hàng ngày)
    parser.add_argument("--start", help="Start date override (YYYYMMDD)")
    parser.add_argument("--end", help="End date override (YYYYMMDD)")
    
    # Tham số riêng (cho chạy dữ liệu lịch sử hoặc tùy chỉnh dải ngày)
    parser.add_argument("--search_start", help="Search log start date")
    parser.add_argument("--search_end", help="Search log end date")
    parser.add_argument("--content_start", help="Content log start date")
    parser.add_argument("--content_end", help="Content log end date")
    
    args = parser.parse_args()

    logging.info("🚀 BẮT ĐẦU CHẠY DATA PIPELINE (RAW → ETL → LOAD)")
    logging.info("="*70)
    
    # --- LOGIC TÍNH TOÁN NGÀY THÁNG ---
    # 1. Lấy ngày hôm qua (Real-time) làm dự phòng
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    
    # 2. Dò tìm ngày mới nhất thực tế trong dữ liệu
    latest_data_date = get_latest_date_in_path(".")
    
    if args.start:
        # Nếu người dùng truyền --start, ưu tiên dùng ngày này
        search_start = search_end = args.start
        if args.end: search_end = args.end
        content_start, content_end = search_start, search_end
    else:
        # Nếu KHÔNG truyền, ưu tiên dùng ngày mới nhất từ dữ liệu, fallback về yesterday
        base_date = latest_data_date if latest_data_date else yesterday
        
        # Tính toán ngày 1 của tháng trước để có dải so sánh
        dt_end = datetime.strptime(base_date, "%Y%m%d")
        dt_start = (dt_end.replace(day=1) - timedelta(days=1)).replace(day=1)
        
        search_start = content_start = dt_start.strftime("%Y%m%d")
        search_end = content_end = base_date

    logging.info(f"📅 Chu kỳ Search: {search_start} -> {search_end}")
    logging.info(f"📅 Chu kỳ Content: {content_start} -> {content_end}")
    
    # STEP 1: Xử lý raw log
    run_script("etl_step1_log_search.py", f"--start {search_start} --end {search_end}")
    run_script("etl_step1_log_content.py", f"--start {content_start} --end {content_end}")
    
    # STEP 2: Data Modeling (Star Schema)
    run_script("etl_step2_obt_concat_model.py", f"--end {search_end}")
    
    # STEP 3: Nạp lên Google BigQuery
    run_script("etl_step3_load_to_bigquery.py")
    
    logging.info("="*70)
    logging.info(f"🎉 PIPELINE KẾT THÚC THÀNH CÔNG.")
    logging.info("📊 Step 4 (ELT): Stored Procedure sẽ tự động chạy trên BigQuery (03:00 AM).")

if __name__ == "__main__":
    main()
