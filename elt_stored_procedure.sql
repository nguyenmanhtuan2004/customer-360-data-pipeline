CREATE OR REPLACE PROCEDURE `cms_data_warehouse.sp_olap_content`()
BEGIN
  DECLARE max_date INT64;
  DECLARE min_date INT64;

  -- Chỉ đọc đến date mới nhất, không đọc tương lai
  SET max_date = (
    SELECT MAX(date_key)
    FROM `cms_data_warehouse.fact_customer_360`
    WHERE data_source = 'log_content'
  );

  -- Tính ngày đầu tiên của tháng trước (vd: max_date=20220714 -> 20220601)
  SET min_date = CAST(
    FORMAT_DATE('%Y%m%d', DATE_TRUNC(DATE_SUB(PARSE_DATE('%Y%m%d', CAST(max_date AS STRING)), INTERVAL 1 MONTH), MONTH)) 
    AS INT64
  );

  -- 1. Category giữ chân user tốt nhất
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_category_loyalty` AS
  SELECT
    MostWatch                                                       AS Category,
    COUNT(DISTINCT Profile_ID)                                      AS TongUser,
    COUNT(DISTINCT CASE WHEN Active = 'High' THEN Profile_ID END)   AS UserTrungThanh,
    COUNT(DISTINCT CASE WHEN Active = 'Low'  THEN Profile_ID END)   AS UserNguyCo,
    ROUND(
      COUNT(DISTINCT CASE WHEN Active = 'High' THEN Profile_ID END) * 100.0
      / NULLIF(COUNT(DISTINCT Profile_ID), 0)
    , 2)                                                            AS TiLe_TrungThanh
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE data_source = 'log_content'
    AND MostWatch IS NOT NULL
    AND date_key BETWEEN min_date AND max_date  -- chỉ lấy data từ đầu tháng trước đến hiện tại
  GROUP BY MostWatch
  ORDER BY TiLe_TrungThanh DESC;

  -- 2. Trong từng Category, Taste nào dominant
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_category_taste` AS
  SELECT
    MostWatch                                                       AS Category,
    Taste,
    COUNT(DISTINCT Profile_ID)                                      AS TongUser,
    ROUND(
      COUNT(DISTINCT Profile_ID) * 100.0
      / SUM(COUNT(DISTINCT Profile_ID)) OVER (PARTITION BY MostWatch)
    , 2)                                                            AS PhanTram_TrongCategory
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE data_source = 'log_content'
    AND MostWatch IS NOT NULL
    AND Taste IS NOT NULL
    AND date_key BETWEEN min_date AND max_date
  GROUP BY MostWatch, Taste
  ORDER BY MostWatch, PhanTram_TrongCategory DESC;

  -- 3. Power user (High Active) xem gì
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_power_user_profile` AS
  SELECT
    MostWatch,
    COUNT(DISTINCT Profile_ID)                                      AS TongUser,
    ROUND(
      COUNT(DISTINCT Profile_ID) * 100.0
      / SUM(COUNT(DISTINCT Profile_ID)) OVER ()
    , 2)                                                            AS PhanTram_TongUser
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE data_source = 'log_content'
    AND Active = 'High'
    AND Taste IS NOT NULL
    AND date_key BETWEEN min_date AND max_date
  GROUP BY MostWatch
  ORDER BY TongUser DESC;

END;

-- ============================================
CREATE OR REPLACE PROCEDURE `cms_data_warehouse.sp_olap_search`()
BEGIN
  DECLARE max_date INT64;
  DECLARE min_date INT64;
  DECLARE max_day_of_month INT64;
  DECLARE days_in_prev_month INT64;

  SET max_date = (
    SELECT MAX(date_key)
    FROM `cms_data_warehouse.fact_customer_360`
    WHERE data_source = 'log_search'
  );

  -- Tính ngày đầu tiên của tháng trước
  SET min_date = CAST(
    FORMAT_DATE('%Y%m%d', DATE_TRUNC(DATE_SUB(PARSE_DATE('%Y%m%d', CAST(max_date AS STRING)), INTERVAL 1 MONTH), MONTH)) 
    AS INT64
  );

  -- Tính số ngày đã có data của tháng hiện tại (VD: max_date=20220714 -> 14 ngày)
  SET max_day_of_month = EXTRACT(DAY FROM PARSE_DATE('%Y%m%d', CAST(max_date AS STRING)));
  
  -- Tính tổng số ngày của tháng trước (VD: Tháng 6 có 30 ngày)
  SET days_in_prev_month = EXTRACT(DAY FROM LAST_DAY(PARSE_DATE('%Y%m%d', CAST(min_date AS STRING))));

  -- 4. Tăng trưởng search (So sánh TỐC ĐỘ TRUNG BÌNH NGÀY - Run Rate)
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_search_growth` AS
  SELECT
    category_prev                                                   AS Category,
    CAST(SUM(count_prev) AS BIGINT)                                 AS LuotTimKiem_ThangTruoc,
    CAST(SUM(count_curr) AS BIGINT)                                 AS LuotTimKiem_ThangNay,
    -- Tính Trung bình mỗi ngày của tháng này
    ROUND(SUM(count_curr) / NULLIF(max_day_of_month, 0), 2)         AS TB_MoiNgay_ThangNay,
    -- Tính Trung bình mỗi ngày của tháng trước
    ROUND(SUM(count_prev) / NULLIF(days_in_prev_month, 0), 2)       AS TB_MoiNgay_ThangTruoc,
    -- Phần trăm tăng trưởng dựa trên Tốc độ trung bình (Apples-to-Apples)
    ROUND(
      ( (SUM(count_curr) / NULLIF(max_day_of_month, 0)) - (SUM(count_prev) / NULLIF(days_in_prev_month, 0)) )
      * 100.0 / NULLIF((SUM(count_prev) / NULLIF(days_in_prev_month, 0)), 0)
    , 2)                                                            AS PhanTram_TangTruong
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE data_source = 'log_search'
    AND category_prev IS NOT NULL
    AND category_curr IS NOT NULL
    AND date_key BETWEEN min_date AND max_date
  GROUP BY category_prev
  ORDER BY PhanTram_TangTruong DESC;

  -- 5. User dịch chuyển sở thích (Chuyển từ Prev sang Curr)
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_interest_migration` AS
  SELECT
    category_prev                                                   AS RoiKhoi,
    category_curr                                                   AS ChuyenSang,
    COUNT(DISTINCT Profile_ID)                                      AS TongUser,
    ROUND(
      COUNT(DISTINCT Profile_ID) * 100.0
      / SUM(COUNT(DISTINCT Profile_ID)) OVER (PARTITION BY category_prev)
    , 2)                                                            AS PhanTram_RoiKhoi
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE data_source = 'log_search'
    AND Trending_Type = 'Changed' -- Chỉ lấy những user có sự thay đổi
    AND category_prev IS NOT NULL
    AND category_curr IS NOT NULL
    AND date_key BETWEEN min_date AND max_date
  GROUP BY category_prev, category_curr
  ORDER BY TongUser DESC;

END;


-- ============================================
CREATE OR REPLACE PROCEDURE `cms_data_warehouse.sp_run_pipeline`()
OPTIONS (strict_mode=false)
BEGIN
  CALL `cms_data_warehouse.sp_olap_content`();
  CALL `cms_data_warehouse.sp_olap_search`();
END;

CALL `cms_data_warehouse.sp_run_pipeline`();

