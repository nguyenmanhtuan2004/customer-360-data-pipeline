-- ==============================================================================
-- BƯỚC 3: STORED PROCEDURE — THỐNG KÊ CHO MARKETING (ELT)
-- File: elt_stored_procedure.sql
-- Mục tiêu: Tổng hợp dữ liệu từ Fact Table sang các bảng báo cáo cho Marketer
-- ==============================================================================

CREATE OR REPLACE PROCEDURE `cms_data_warehouse.Calculate_Marketing_Report`()
BEGIN

  -- 1. Report 1: Tổng quan xu hướng tìm kiếm theo thể loại
  -- Marketer cần biết xu hướng tìm kiếm tổng thể để chạy quảng cáo theo trend.
  CREATE OR REPLACE TABLE `cms_data_warehouse.Report_Marketing_Category` AS
  SELECT 
      category_T7,
      COUNT(DISTINCT Profile_ID) AS TongSoNguoi,
      CAST(SUM(count_t7) AS INT64) AS TongLuotTimKiem
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE category_T7 IS NOT NULL
  GROUP BY category_T7;

  -- 2. Report 2: Thống kê hành vi xem theo thể loại nội dung
  -- Marketer cần biết người dùng đang thực sự xem gì để tối ưu hóa gợi ý nội dung.
  CREATE OR REPLACE TABLE `cms_data_warehouse.Report_Content_Consumption` AS
  SELECT 
      Taste AS TheLoaiNoiDung,
      COUNT(DISTINCT Profile_ID) AS SoNguoiXem,
      CAST(SUM(
          COALESCE(Total_Giai_Tri, 0) + 
          COALESCE(Total_Phim_Truyen, 0) + 
          COALESCE(Total_The_Thao, 0) + 
          COALESCE(Total_Thieu_Nhi, 0) + 
          COALESCE(Total_Truyen_Hinh, 0)
      ) AS INT64) AS TongThoiLuongXem
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE Taste IS NOT NULL
  GROUP BY Taste;

  -- 3. Report 3: Cảnh báo Churn (Tìm nhiều nhưng xem ít)
  -- Đối tượng quan trọng: Những người tìm kiếm rất nhiều (T7 > 50 lần) 
  -- nhưng không xem nội dung gì cụ thể (MostWatch rỗng hoặc Other) -> Có nguy cơ rời bỏ ứng dụng.
  CREATE OR REPLACE TABLE `cms_data_warehouse.Report_Churn_Warning` AS
  SELECT 
      Profile_ID, 
      most_searched_T7, 
      CAST(count_t7 AS INT64) AS SoLanTimKiem, 
      Taste AS TheLoaiHayXem
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE count_t7 > 50 
    AND (MostWatch IS NULL OR MostWatch = 'Other');

  -- Log lại thời gian cập nhật (Tùy chọn)
  print('✅ Thành công: Đã cập nhật 3 báo cáo Marketing lúc ' || CAST(CURRENT_TIMESTAMP() AS STRING));

END;

/* 
-- CÁCH CHẠY THỬ TRÊN BIGQUERY CONSOLE:
CALL `cms_data_warehouse.Calculate_Marketing_Report`();
*/
