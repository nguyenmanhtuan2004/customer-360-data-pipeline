CREATE OR REPLACE PROCEDURE `cms_data_warehouse.sp_olap_content`()
BEGIN
  DECLARE max_date INT64;

  -- Chỉ đọc đến date mới nhất, không đọc tương lai
  SET max_date = (
    SELECT MAX(date_key)
    FROM `cms_data_warehouse.fact_customer_360`
    WHERE data_source = 'log_content'
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
    AND date_key <= max_date  -- chỉ đọc đến ngày mới nhất
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
    AND date_key <= max_date
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
    AND date_key <= max_date
  GROUP BY MostWatch
  ORDER BY TongUser DESC;

END;

-- ============================================
CREATE OR REPLACE PROCEDURE `cms_data_warehouse.sp_olap_search`()
BEGIN
  DECLARE max_date INT64;

  SET max_date = (
    SELECT MAX(date_key)
    FROM `cms_data_warehouse.fact_customer_360`
    WHERE data_source = 'log_search'
  );

  -- 4. Tăng trưởng search (So sánh Tháng hiện tại vs Tháng trước)
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_search_growth` AS
  SELECT
    category_prev                                                   AS Category,
    CAST(SUM(count_prev) AS BIGINT)                                 AS LuotTimKiem_ThangTruoc,
    CAST(SUM(count_curr) AS BIGINT)                                 AS LuotTimKiem_ThangNay,
    CAST(SUM(count_curr) AS BIGINT) 
      - CAST(SUM(count_prev) AS BIGINT)                             AS TangTruong,
    ROUND(
      (CAST(SUM(count_curr) AS BIGINT) - CAST(SUM(count_prev) AS BIGINT))
      * 100.0 / NULLIF(CAST(SUM(count_prev) AS BIGINT), 0)
    , 2)                                                            AS PhanTram_TangTruong
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE data_source = 'log_search'
    AND category_prev IS NOT NULL
    AND category_curr IS NOT NULL
    AND date_key <= max_date
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
    AND date_key <= max_date
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
