import pandas as pd
import os
from google.oauth2 import service_account
import pandas_gbq

def load_parquet_to_bigquery(parquet_path, table_id, credentials, mode='append'):
    """
    Hàm đọc dữ liệu từ đường dẫn Parquet (file hoặc thư mục Spark) 
    và Load lên Google BigQuery.
    """
    if not os.path.exists(parquet_path):
        print(f"⚠️ Cảnh báo: Đường dẫn {parquet_path} không tồn tại. Bỏ qua task này.")
        return

    print(f"📦 Đang nạp dữ liệu từ: {parquet_path}")
    try:
        # Pandas hỗ trợ đọc cả thư mục chứa nhiều file part của Spark
        df = pd.read_parquet(parquet_path)
        
        if df.empty:
            print(f"⚠️ Dữ liệu tại {parquet_path} trống. Không nạp.")
            return

        print(f"🚀 Đang nạp {len(df)} dòng vào bảng '{table_id}' ({mode.upper()})...")
        pandas_gbq.to_gbq(
            df,
            destination_table=table_id,
            project_id=credentials.project_id,
            credentials=credentials,
            if_exists=mode,
            progress_bar=True,
            chunksize=100000
        )
        print(f"✅ Thành công: {table_id}\n")
    except Exception as e:
        print(f"❌ Lỗi khi xử lý bảng {table_id}: {e}")

def main():
    # ==========================================
    # CẤU HÌNH KẾT NỐI GOOGLE CLOUD PLATFORM
    # ==========================================
    credentials_path = 'bigdata-mapping-b6ba7074c7d7.json' 
    dataset_name = 'cms_data_warehouse'
    
    if not os.path.exists(credentials_path):
        print(f"❌ CẢNH BÁO: Không tìm thấy file xác thực '{credentials_path}'.")
        return

    print("🔑 Đang thiết lập xác thực với Google Cloud...")
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    print(f"🌐 Project ID nhận diện được: {credentials.project_id}")

    # Nạp 5 bảng Parquet chuẩn hóa Star Schema lên BigQuery
    print("====================================")
    
    # 1. Fact Table (APPEND)
    load_parquet_to_bigquery("Fact_Customer_360.parquet", f"{dataset_name}.fact_customer_360", credentials, mode='replace')
    
    # 2. Dimensions (APPEND - Chứa dữ liệu thay đổi theo thời gian/từng đợt)
    load_parquet_to_bigquery("Dim_User.parquet", f"{dataset_name}.dim_user", credentials, mode='replace')
    load_parquet_to_bigquery("Dim_Cust.parquet", f"{dataset_name}.dim_cust", credentials, mode='replace')
    
    # 3. Reference Dimensions (REPLACE - Bảng tham chiếu tĩnh, ghi đè để cập nhật mới nhất)
    load_parquet_to_bigquery("Dim_Service.parquet", f"{dataset_name}.dim_service", credentials, mode='replace')
    load_parquet_to_bigquery("Dim_Date.parquet", f"{dataset_name}.dim_date", credentials, mode='replace')
    
    print("====================================")
    
    print("🎉 XONG BƯỚC 2! Toàn bộ Data Warehouse đã được cập nhật lên Google BigQuery.")
    print("👉 Hãy mở BigQuery Console để kiểm tra các bảng trong dataset 'cms_data_warehouse'.")

if __name__ == "__main__":
    main()
