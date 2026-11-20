SELECT
.DateKey as date_key,
.FullDate as full_date,
.Year as year,
.Quarter as quarter,
.Month as month,
.MonthName as month_name,
.Day as day,
.DayOfWeekISO as day_of_week,
date_format(.FullDate, 'yyyyMM') as year_month,
IF ( .IsWeekend = 0 And .IsHoliday = 0 , 1 , 0 ) as is_trading_day
FROM


SELECT
row_number() over (order by 1) as promo_sk,
trim(.PromoID) as promo_id,
trim(.Category) as category,
.StartDate as start_date,
.EndDate as end_date,
.DiscountPct as discount_pct,
DATEDIFF(.EndDate, .StartDate) + 1 as duration_days,
date_format(.StartDate, 'yyyyMMdd') as start_key,
date_format(.EndDate, 'yyyyMMdd') as end_key
FROM


SELECT
.country as country,
.currency as currency,
.YearMonth as YearMonth,
.RateFromGBP as RateFromGBP
FROM

INNER JOIN  ON .currency = .currency

SELECT CONCAT_WS(',',
lnk_dim_date.date_key,
lnk_dim_date.full_date,
lnk_dim_date.year,
lnk_dim_date.quarter,
lnk_dim_date.month,
lnk_dim_date.month_name,
lnk_dim_date.day,
lnk_dim_date.day_of_week,
lnk_dim_date.year_month,
lnk_dim_date.is_trading_day)
FROM
lnk_dim_date



SELECT CONCAT_WS(',',
lnk_dim_promo.promo_sk,
lnk_dim_promo.promo_id,
lnk_dim_promo.category,
lnk_dim_promo.start_date,
lnk_dim_promo.end_date,
lnk_dim_promo.discount_pct,
lnk_dim_promo.duration_days,
lnk_dim_promo.start_key,
lnk_dim_promo.end_key)
FROM
lnk_dim_promo



SELECT
trim(lnk_cc_fx.country) as country,
REPLACE( lnk_cc_fx.YearMonth , '-' , '' ) as year_month,
lnk_cc_fx.currency as currency,
lnk_cc_fx.RateFromGBP as fx_rate
FROM
lnk_cc_fx

SELECT CONCAT_WS(',',
lnk_dim_fx.country,
lnk_dim_fx.year_month,
lnk_dim_fx.currency,
lnk_dim_fx.fx_rate)
FROM
lnk_dim_fx

