CREATE OR REPLACE TEMP VIEW lnk_cal AS (
  SELECT CAST(_c0 AS int) AS DateKey, _c1 AS FullDate, CAST(_c2 AS int) AS Year, CAST(_c3 AS int) AS Quarter, CAST(_c4 AS int) AS Month, _c5 AS MonthName, CAST(_c6 AS int) AS Day, CAST(_c7 AS int) AS DayOfWeekISO, CAST(_c8 AS int) AS IsWeekend, CAST(_c9 AS int) AS IsHoliday
  FROM read_files('/Volumes/unitygo/retail_dw/landing/calendar.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string, _c2 string, _c3 string, _c4 string, _c5 string, _c6 string, _c7 string, _c8 string, _c9 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_dim_date AS (
SELECT
CAST(lnk_cal.DateKey AS int) as date_key,
lnk_cal.FullDate as full_date,
CAST(lnk_cal.Year AS int) as year,
CAST(lnk_cal.Quarter AS int) as quarter,
CAST(lnk_cal.Month AS int) as month,
coalesce(lnk_cal.MonthName, '') as month_name,
CAST(lnk_cal.Day AS int) as day,
CAST(lnk_cal.DayOfWeekISO AS int) as day_of_week,
date_format(lnk_cal.FullDate, 'yyyyMM') as year_month,
IF ( CAST(lnk_cal.IsWeekend AS int) = 0 And CAST(lnk_cal.IsHoliday AS int) = 0 , 1 , 0 ) as is_trading_day
FROM lnk_cal
);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dim_date AS
SELECT
  *
FROM lnk_dim_date;

CREATE OR REPLACE TEMP VIEW lnk_promo AS (
  SELECT _c0 AS PromoID, _c1 AS Category, _c2 AS StartDate, _c3 AS EndDate, CAST(_c4 AS int) AS DiscountPct
  FROM read_files('/Volumes/unitygo/retail_dw/landing/promotions.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string, _c2 string, _c3 string, _c4 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_dim_promo AS (
SELECT
row_number() over (order by 1) as promo_sk,
trim(coalesce(lnk_promo.PromoID, '')) as promo_id,
trim(coalesce(lnk_promo.Category, '')) as category,
lnk_promo.StartDate as start_date,
lnk_promo.EndDate as end_date,
CAST(lnk_promo.DiscountPct AS int) as discount_pct,
DATEDIFF(lnk_promo.EndDate, lnk_promo.StartDate) + 1 as duration_days,
date_format(lnk_promo.StartDate, 'yyyyMMdd') as start_key,
date_format(lnk_promo.EndDate, 'yyyyMMdd') as end_key
FROM lnk_promo
);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dim_promotion AS
SELECT
  *
FROM lnk_dim_promo;

CREATE OR REPLACE TEMP VIEW lnk_fx AS (
  SELECT _c0 AS YearMonth, _c1 AS currency, CAST(_c2 AS decimal(18,6)) AS RateFromGBP
  FROM read_files('/Volumes/unitygo/retail_dw/landing/fx_rates.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string, _c2 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_cc AS (
  SELECT _c0 AS country, _c1 AS currency
  FROM read_files('/Volumes/unitygo/retail_dw/landing/country_currency.csv', format => 'csv', header => true,
    schema => '_c0 string, _c1 string', escape => '"')
);

CREATE OR REPLACE TEMP VIEW lnk_cc_fx AS (
SELECT
coalesce(lnk_cc.country, '') as country,
coalesce(lnk_cc.currency, '') as currency,
coalesce(lnk_fx.YearMonth, '') as YearMonth,
CAST(lnk_fx.RateFromGBP AS decimal(18,6)) as RateFromGBP
FROM lnk_cc INNER JOIN lnk_fx ON coalesce(lnk_cc.currency, '') = coalesce(lnk_fx.currency, '')
);

CREATE OR REPLACE TEMP VIEW lnk_dim_fx AS (
SELECT
trim(coalesce(lnk_cc_fx.country, '')) as country,
REPLACE( coalesce(lnk_cc_fx.YearMonth, '') , '-' , '' ) as year_month,
coalesce(lnk_cc_fx.currency, '') as currency,
CAST(lnk_cc_fx.RateFromGBP AS decimal(18,6)) as fx_rate
FROM
lnk_cc_fx

);

CREATE OR REPLACE TABLE unitygo.retail_dw_dbxconv_dbsql.dim_fx AS
SELECT
  *
FROM lnk_dim_fx;