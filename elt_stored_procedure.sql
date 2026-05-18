CREATE OR REPLACE PROCEDURE `cms_data_warehouse.sp_olap_content`()
BEGIN
  DECLARE max_date INT64;
  DECLARE min_date INT64;

  SET max_date = (
    SELECT MAX(date_key)
    FROM `cms_data_warehouse.fact_customer_360`
    WHERE data_source = 'log_content'
  );

  SET min_date = CAST(
    FORMAT_DATE('%Y%m%d', DATE_TRUNC(DATE_SUB(PARSE_DATE('%Y%m%d', CAST(max_date AS STRING)), INTERVAL 1 MONTH), MONTH)) 
    AS INT64
  );

  -- 1. Category Loyalty — JOIN dim_service để lấy category_group
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_category_loyalty` AS
  SELECT
    ds.category_group                                                AS CategoryGroup,
    ds.Type                                                          AS Category,
    COUNT(DISTINCT f.Profile_ID)                                     AS TongUser,
    COUNT(DISTINCT CASE WHEN f.Active = 'High' THEN f.Profile_ID END) AS UserTrungThanh,
    COUNT(DISTINCT CASE WHEN f.Active = 'Low'  THEN f.Profile_ID END) AS UserNguyCo,
    ROUND(
      COUNT(DISTINCT CASE WHEN f.Active = 'High' THEN f.Profile_ID END) * 100.0
      / NULLIF(COUNT(DISTINCT f.Profile_ID), 0)
    , 2)                                                             AS TiLe_TrungThanh
  FROM `cms_data_warehouse.fact_customer_360` f
  JOIN `cms_data_warehouse.dim_service` ds ON f.service_key = ds.service_key
  WHERE f.data_source = 'log_content'
    AND f.date_key BETWEEN min_date AND max_date
  GROUP BY ds.category_group, ds.Type
  ORDER BY TiLe_TrungThanh DESC;

  -- 2. Category × Taste — JOIN dim_service
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_category_taste` AS
  SELECT
    ds.category_group                                                AS CategoryGroup,
    ds.Type                                                          AS Category,
    f.Taste,
    COUNT(DISTINCT f.Profile_ID)                                     AS TongUser,
    ROUND(
      COUNT(DISTINCT f.Profile_ID) * 100.0
      / SUM(COUNT(DISTINCT f.Profile_ID)) OVER (PARTITION BY ds.Type)
    , 2)                                                             AS PhanTram_TrongCategory
  FROM `cms_data_warehouse.fact_customer_360` f
  JOIN `cms_data_warehouse.dim_service` ds ON f.service_key = ds.service_key
  WHERE f.data_source = 'log_content'
    AND f.Taste IS NOT NULL
    AND f.date_key BETWEEN min_date AND max_date
  GROUP BY ds.category_group, ds.Type, f.Taste
  ORDER BY ds.Type, PhanTram_TrongCategory DESC;

  -- 3. Power User vs Casual User — JOIN dim_service (BẢNG MỚI thay thế bảng cũ)
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_power_vs_casual` AS
  SELECT
    f.Active                                                         AS UserType,
    ds.category_group                                                AS CategoryGroup,
    ds.Type                                                          AS Category,
    COUNT(DISTINCT f.Profile_ID)                                     AS TongUser,
    ROUND(
      COUNT(DISTINCT f.Profile_ID) * 100.0
      / SUM(COUNT(DISTINCT f.Profile_ID)) OVER (PARTITION BY f.Active)
    , 2)                                                             AS PhanTram_TrongNhom
  FROM `cms_data_warehouse.fact_customer_360` f
  JOIN `cms_data_warehouse.dim_service` ds ON f.service_key = ds.service_key
  WHERE f.data_source = 'log_content'
    AND f.date_key BETWEEN min_date AND max_date
  GROUP BY f.Active, ds.category_group, ds.Type
  ORDER BY f.Active, TongUser DESC;

  -- 4. Taste Diversity — Đa chiều nội dung (BẢNG MỚI)
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_taste_diversity` AS
  SELECT
    CASE 
      WHEN (LENGTH(f.Taste) - LENGTH(REPLACE(f.Taste, '-', ''))) = 0 THEN '1 thể loại'
      WHEN (LENGTH(f.Taste) - LENGTH(REPLACE(f.Taste, '-', ''))) = 1 THEN '2 thể loại'
      WHEN (LENGTH(f.Taste) - LENGTH(REPLACE(f.Taste, '-', ''))) = 2 THEN '3 thể loại'
      WHEN (LENGTH(f.Taste) - LENGTH(REPLACE(f.Taste, '-', ''))) >= 3 THEN '4+ thể loại'
    END                                                              AS SoTheLoaiXem,
    f.Active,
    COUNT(DISTINCT f.Profile_ID)                                     AS TongUser,
    ROUND(
      COUNT(DISTINCT f.Profile_ID) * 100.0
      / SUM(COUNT(DISTINCT f.Profile_ID)) OVER ()
    , 2)                                                             AS PhanTram
  FROM `cms_data_warehouse.fact_customer_360` f
  WHERE f.data_source = 'log_content'
    AND f.Taste IS NOT NULL
    AND f.date_key BETWEEN min_date AND max_date
  GROUP BY SoTheLoaiXem, f.Active
  ORDER BY SoTheLoaiXem;

END;

-- ============================================
-- PROCEDURE 2: PHÂN TÍCH SEARCH
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

  SET min_date = CAST(
    FORMAT_DATE('%Y%m%d', DATE_TRUNC(DATE_SUB(PARSE_DATE('%Y%m%d', CAST(max_date AS STRING)), INTERVAL 1 MONTH), MONTH)) 
    AS INT64
  );

  SET max_day_of_month = EXTRACT(DAY FROM PARSE_DATE('%Y%m%d', CAST(max_date AS STRING)));
  SET days_in_prev_month = EXTRACT(DAY FROM LAST_DAY(PARSE_DATE('%Y%m%d', CAST(min_date AS STRING))));

  -- 5. Search Growth (Run Rate) — Giữ nguyên logic
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_search_growth` AS
  SELECT
    category_prev                                                    AS Category,
    CAST(SUM(count_prev) AS INT64)                                   AS LuotTimKiem_ThangTruoc,
    CAST(SUM(count_curr) AS INT64)                                   AS LuotTimKiem_ThangNay,
    ROUND(SUM(count_curr) / NULLIF(max_day_of_month, 0), 2)         AS TB_MoiNgay_ThangNay,
    ROUND(SUM(count_prev) / NULLIF(days_in_prev_month, 0), 2)       AS TB_MoiNgay_ThangTruoc,
    ROUND(
      ( (SUM(count_curr) / NULLIF(max_day_of_month, 0)) - (SUM(count_prev) / NULLIF(days_in_prev_month, 0)) )
      * 100.0 / NULLIF((SUM(count_prev) / NULLIF(days_in_prev_month, 0)), 0)
    , 2)                                                             AS PhanTram_TangTruong
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE data_source = 'log_search'
    AND category_prev IS NOT NULL
    AND category_curr IS NOT NULL
    AND date_key BETWEEN min_date AND max_date
  GROUP BY category_prev
  ORDER BY PhanTram_TangTruong DESC;

  -- 6. Interest Migration — Giữ nguyên logic
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_interest_migration` AS
  SELECT
    category_prev                                                    AS RoiKhoi,
    category_curr                                                    AS ChuyenSang,
    COUNT(DISTINCT Profile_ID)                                       AS TongUser,
    ROUND(
      COUNT(DISTINCT Profile_ID) * 100.0
      / SUM(COUNT(DISTINCT Profile_ID)) OVER (PARTITION BY category_prev)
    , 2)                                                             AS PhanTram_RoiKhoi
  FROM `cms_data_warehouse.fact_customer_360`
  WHERE data_source = 'log_search'
    AND Trending_Type = 'Changed'
    AND category_prev IS NOT NULL
    AND category_curr IS NOT NULL
    AND date_key BETWEEN min_date AND max_date
  GROUP BY category_prev, category_curr
  ORDER BY TongUser DESC;

END;

-- ============================================
-- PROCEDURE 3: CROSS-ANALYSIS + OVERVIEW (JOIN CẢ DIM_SERVICE VÀ DIM_DATE)
-- ============================================
CREATE OR REPLACE PROCEDURE `cms_data_warehouse.sp_olap_cross`()
BEGIN
  DECLARE max_date INT64;
  DECLARE min_date INT64;

  SET max_date = (
    SELECT MAX(date_key) FROM `cms_data_warehouse.fact_customer_360`
  );

  SET min_date = CAST(
    FORMAT_DATE('%Y%m%d', DATE_TRUNC(DATE_SUB(PARSE_DATE('%Y%m%d', CAST(max_date AS STRING)), INTERVAL 1 MONTH), MONTH)) 
    AS INT64
  );

  -- 7. Executive Summary KPIs — JOIN dim_date để lấy thông tin Tháng/Năm snapshot động
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_executive_summary` AS
  SELECT
    dd.month_name                                                    AS ThangSnapshot,
    dd.year                                                          AS NamSnapshot,
    COUNT(DISTINCT CASE WHEN f.data_source IN ('log_content','both_search_and_content') 
          THEN f.Profile_ID END)                                     AS TotalContentUsers,
    COUNT(DISTINCT CASE WHEN f.data_source IN ('log_content','both_search_and_content') AND f.Active = 'High' 
          THEN f.Profile_ID END)                                     AS TotalPowerUsers,
    COUNT(DISTINCT CASE WHEN f.data_source IN ('log_search','both_search_and_content') 
          THEN f.Profile_ID END)                                     AS TotalSearchUsers,
    COUNT(DISTINCT CASE WHEN f.Trending_Type = 'Changed' 
          THEN f.Profile_ID END)                                     AS UsersChangedInterest,
    COUNT(DISTINCT CASE WHEN f.data_source = 'both_search_and_content' 
          THEN f.Profile_ID END)                                     AS UsersBothSources,
    COUNT(DISTINCT f.Profile_ID)                                     AS TotalUniqueUsers
  FROM `cms_data_warehouse.fact_customer_360` f
  JOIN `cms_data_warehouse.dim_date` dd ON f.date_key = dd.date_key
  WHERE f.date_key BETWEEN min_date AND max_date
  GROUP BY dd.month_name, dd.year;

  -- 8. Search vs Watch — JOIN dim_service (CROSS-ANALYSIS)
  CREATE OR REPLACE TABLE `cms_data_warehouse.olap_search_vs_watch` AS
  SELECT
    f.category_curr                                                  AS SearchCategory,
    ds.category_group                                                AS WatchCategoryGroup,
    ds.Type                                                          AS WatchCategory,
    COUNT(DISTINCT f.Profile_ID)                                     AS TongUser,
    CASE 
      WHEN f.category_curr = ds.Type THEN 'Khớp'
      ELSE 'Lệch'
    END                                                              AS MatchStatus
  FROM `cms_data_warehouse.fact_customer_360` f
  JOIN `cms_data_warehouse.dim_service` ds ON f.service_key = ds.service_key
  WHERE f.data_source = 'both_search_and_content'
    AND f.category_curr IS NOT NULL
    AND f.date_key BETWEEN min_date AND max_date
  GROUP BY f.category_curr, ds.category_group, ds.Type
  ORDER BY TongUser DESC;

END;

-- ============================================
-- MASTER PIPELINE
-- ============================================
CREATE OR REPLACE PROCEDURE `cms_data_warehouse.sp_run_pipeline`()
OPTIONS (strict_mode=false)
BEGIN
  CALL `cms_data_warehouse.sp_olap_content`();
  CALL `cms_data_warehouse.sp_olap_search`();
  CALL `cms_data_warehouse.sp_olap_cross`();
END;

CALL `cms_data_warehouse.sp_run_pipeline`();
