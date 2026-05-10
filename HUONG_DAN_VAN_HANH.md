# 🚀 Hướng dẫn Vận hành Pipeline Data Engineering (Class 7)

Dự án này sử dụng PySpark để xử lý dữ liệu Log Search và Log Content, chuyển đổi sang mô hình Star Schema và đẩy lên BigQuery.

## ⚠️ Lưu ý Quan trọng (Khắc phục lỗi Crash)
**KHÔNG sử dụng Python 3.12** để chạy dự án này trên Windows. 
Hiện tại có một lỗi hệ thống giữa PySpark và Python 3.12 trên Windows gây ra lỗi `EOFException` (Worker Crash). Dự án bắt buộc phải chạy trên môi trường **Python 3.11**.

---

## 1. Thiết lập Môi trường (Chỉ làm 1 lần)

Nếu bạn chưa có môi trường `spark_env`, hãy mở **Anaconda Prompt** và chạy các lệnh sau:

```bash
# Tạo môi trường Python 3.11
conda create -n spark_env python=3.11 -y

# Kích hoạt môi trường
conda activate spark_env

# Cài đặt các thư viện cần thiết
pip install pyspark==4.1.1 findspark pandas pyarrow pandas-gbq google-auth
```

---

## 2. Cách chạy Pipeline

### Cách 1: Chạy tự động (Khuyến nghị)
Double-click vào file: `run_pipeline.bat`
* File này đã được cấu hình để tự động kích hoạt môi trường `spark_env` và chạy toàn bộ quy trình.

### Cách 2: Chạy bằng tay (Manual)
Mở Terminal/Anaconda Prompt tại thư mục dự án:
```powershell
conda activate spark_env
python main_pipeline.py
```

---

## 3. Cấu trúc Pipeline
1. **Step 1**: Thu thập và làm sạch log (`log_search`, `log_content`).
2. **Step 2**: Gộp dữ liệu (OBT) và tạo mô hình Star Schema (Fact/Dim tables).
3. **Step 3**: Tải dữ liệu lên Google BigQuery.

---

## 4. Kiểm tra Lỗi (Troubleshooting)
- **Lỗi `PYTHON_VERSION_MISMATCH`**: Đảm bảo bạn đã kích hoạt môi trường `spark_env`.
- **Lỗi `EOFException`**: Kiểm tra xem có đang dùng Python 3.12 không (phải dùng 3.11).
- **Lỗi Bộ nhớ (RAM)**: Nếu dữ liệu tăng lên quá lớn (trên 5-10 triệu dòng), hãy tăng thông số RAM trong file `etl_step2_obt_concat_model.py` tại hàm `get_spark_session`.

---

## 5. Tự động hóa với Task Scheduler
Để chạy Pipeline hàng ngày vào lúc 8:00 AM:
1. Mở **Task Scheduler** trên Windows.
2. Chọn **Create Basic Task**.
3. Tại phần **Action**, chọn **Start a program**.
4. Trỏ tới file `e:\DataEngineer\BigData\Class7\run_pipeline.bat`.
---

## 6. Chạy với Docker (Môi trường Cô lập)

Nếu bạn không muốn cài đặt Java/Spark trực tiếp trên máy, bạn có thể dùng Docker:

```bash
# 1. Xây dựng Image và chạy Container
docker-compose up --build

# 2. Vào bên trong Container để kiểm tra (nếu cần)
docker exec -it spark_data_pipeline bash
```

Docker sẽ tự động thiết lập Python 3.11, Java 17 và PySpark bên trong môi trường Linux, giúp loại bỏ hoàn toàn các lỗi xung đột hệ điều hành Windows.
