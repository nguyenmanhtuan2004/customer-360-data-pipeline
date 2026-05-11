# 📘 Hướng dẫn Vận hành Pipeline Data Customer 360 (Docker)

Tài liệu này hướng dẫn cách chạy toàn bộ hệ thống xử lý dữ liệu từ Log thô đến Star Schema và Dashboard, ngay cả khi bạn không cài đặt Spark/Java trên máy.

---

## 🛠 1. Chuẩn bị (Prerequisites)
1.  **Docker & Docker Compose**: Đảm bảo máy đã cài Docker Desktop.
2.  **Dữ liệu đầu vào**: Đặt các thư mục `log_search` và `log_content` vào cùng thư mục với dự án.
3.  **Quyền truy cập (Tùy chọn)**:
    *   Nếu muốn đẩy dữ liệu lên Cloud: Copy file `bigdata-mapping-b6ba7074c7d7.json` vào thư mục gốc.
    *   Nếu không có file này: Pipeline sẽ tự động chạy ở chế độ **Offline** (chỉ lưu file Parquet tại máy local).

---

## 🚀 2. Các bước triển khai

### Bước 1: Build hệ thống (Chỉ làm 1 lần đầu)
Mở Terminal tại thư mục dự án và chạy:
```bash
docker compose build
```
*Docker sẽ tự động tải Python 3.11, Spark 4.1.1 và Java 17 cho bạn.*

### Bước 2: Chạy Pipeline xử lý dữ liệu (ETL)
Lệnh này sẽ chạy các bước: Làm sạch dữ liệu -> Phân loại AI -> Tạo mô hình Star Schema.
```bash
docker compose up pipeline
```
*Sau khi chạy xong, kết quả sẽ được lưu vào các thư mục `.parquet` như `Fact_Customer_360.parquet`, `Dim_User.parquet`,...*

### Bước 3: Xem Dashboard báo cáo
Khởi động ứng dụng giao diện Streamlit:
```bash
docker compose up dashboard
```
*Sau đó, truy cập trình duyệt tại địa chỉ: **http://localhost:8501***

---

## 📂 3. Cấu trúc các thành phần chính
*   `main_pipeline.py`: Bộ điều khiển trung tâm (Orchestrator).
*   `etl_step1_...`: Tiền xử lý dữ liệu Search và Content.
*   `etl_step2_...`: Xây dựng mô hình dữ liệu (Fact/Dim).
*   `etl_step3_...`: Nạp dữ liệu lên BigQuery (Tự động bỏ qua nếu thiếu Key).
*   `app.py`: Giao diện Dashboard hiển thị báo cáo.

---

## 💡 4. Mẹo nhỏ
*   **Xóa Container sau khi chạy**: Thêm flag `--rm` để Docker tự dọn dẹp bộ nhớ:
    `docker compose run --rm pipeline`
*   **Cập nhật Code**: Nếu bạn sửa code Python, chỉ cần chạy lại `docker compose up`, Docker sẽ cập nhật thay đổi trong 1-2 giây nhờ cơ chế Cache.
