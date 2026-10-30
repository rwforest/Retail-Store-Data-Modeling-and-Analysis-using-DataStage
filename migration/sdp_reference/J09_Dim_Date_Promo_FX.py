# Databricks notebook source
# J09_Dim_Date_Promo_FX - Build Dim_Date, Dim_Promotion and Dim_FX (country x month)
# Layer: 2_Dimensions. SDP source file compiled from the DataStage job by migration/ds2spark.py --sdp.
from pyspark import pipelines as dp

src_dir = spark.conf.get("src_dir")
pRunDate = spark.conf.get("pRunDate", "2011-12-31")
pEffDate = spark.conf.get("pEffDate", "2011-07-01")

# COMMAND ----------
# src_calendar: source file calendar.csv (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j09_lnk_cal")
def j09_lnk_cal():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/calendar.csv")
    assert len(raw.columns) == 10, f"calendar.csv: expected 10 columns, got {len(raw.columns)}"
    return raw.toDF('DateKey', 'FullDate', 'Year', 'Quarter', 'Month', 'MonthName', 'Day', 'DayOfWeekISO', 'IsWeekend', 'IsHoliday').selectExpr(
        'CAST(`DateKey` AS int) AS DateKey',
        'CAST(`FullDate` AS date) AS FullDate',
        'CAST(`Year` AS int) AS Year',
        'CAST(`Quarter` AS int) AS Quarter',
        'CAST(`Month` AS int) AS Month',
        "coalesce(`MonthName`, '') AS MonthName",
        'CAST(`Day` AS int) AS Day',
        'CAST(`DayOfWeekISO` AS int) AS DayOfWeekISO',
        'CAST(`IsWeekend` AS int) AS IsWeekend',
        'CAST(`IsHoliday` AS int) AS IsHoliday')

# COMMAND ----------
# xfm_Date_Attrs: Transformer
@dp.temporary_view(name="j09_lnk_dim_date")
def j09_lnk_dim_date():
    return spark.sql(f"""
SELECT
  DateKey AS date_key,
  FullDate AS full_date,
  Year AS year,
  Quarter AS quarter,
  Month AS month,
  MonthName AS month_name,
  Day AS day,
  DayOfWeekISO AS day_of_week,
  CAST(date_format(FullDate, 'yyyyMM') AS string) AS year_month,
  CAST(CASE WHEN ((IsWeekend = 0) AND (IsHoliday = 0)) THEN 1 ELSE 0 END AS int) AS is_trading_day
FROM j09_lnk_cal
""")

# COMMAND ----------
# tgt_dim_date: target file -> materialized view dim_date
@dp.materialized_view(name="dim_date")
def dim_date():
    return spark.read.table("j09_lnk_dim_date").select('date_key', 'full_date', 'year', 'quarter', 'month', 'month_name', 'day', 'day_of_week', 'year_month', 'is_trading_day')

# COMMAND ----------
# src_promotions: source file promotions.csv (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j09_lnk_promo")
def j09_lnk_promo():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/promotions.csv")
    assert len(raw.columns) == 5, f"promotions.csv: expected 5 columns, got {len(raw.columns)}"
    return raw.toDF('PromoID', 'Category', 'StartDate', 'EndDate', 'DiscountPct').selectExpr(
        "coalesce(`PromoID`, '') AS PromoID",
        "coalesce(`Category`, '') AS Category",
        'CAST(`StartDate` AS date) AS StartDate',
        'CAST(`EndDate` AS date) AS EndDate',
        'CAST(`DiscountPct` AS int) AS DiscountPct')

# COMMAND ----------
# xfm_Promo_SK: Transformer
@dp.temporary_view(name="j09_lnk_dim_promo")
def j09_lnk_dim_promo():
    return spark.sql(f"""
SELECT
  CAST(row_number() OVER (ORDER BY monotonically_increasing_id()) AS int) AS promo_sk,
  CAST(trim(PromoID) AS string) AS promo_id,
  CAST(trim(Category) AS string) AS category,
  StartDate AS start_date,
  EndDate AS end_date,
  DiscountPct AS discount_pct,
  CAST((datediff(EndDate, StartDate) + 1) AS int) AS duration_days,
  CAST(date_format(StartDate, 'yyyyMMdd') AS int) AS start_key,
  CAST(date_format(EndDate, 'yyyyMMdd') AS int) AS end_key
FROM j09_lnk_promo
""")

# COMMAND ----------
# tgt_dim_promotion: target file -> materialized view dim_promotion
@dp.materialized_view(name="dim_promotion")
def dim_promotion():
    return spark.read.table("j09_lnk_dim_promo").select('promo_sk', 'promo_id', 'category', 'start_date', 'end_date', 'discount_pct', 'duration_days', 'start_key', 'end_key')

# COMMAND ----------
# src_fx_rates: source file fx_rates.csv (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j09_lnk_fx")
def j09_lnk_fx():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/fx_rates.csv")
    assert len(raw.columns) == 3, f"fx_rates.csv: expected 3 columns, got {len(raw.columns)}"
    return raw.toDF('YearMonth', 'currency', 'RateFromGBP').selectExpr(
        "coalesce(`YearMonth`, '') AS YearMonth",
        "coalesce(`currency`, '') AS currency",
        'CAST(`RateFromGBP` AS decimal(18,6)) AS RateFromGBP')

# COMMAND ----------
# src_country_currency: source file country_currency.csv (columns mapped by position, as DataStage does)
@dp.temporary_view(name="j09_lnk_cc")
def j09_lnk_cc():
    raw = spark.read.option("header", "true").option("escape", '"').csv(f"{src_dir}/country_currency.csv")
    assert len(raw.columns) == 2, f"country_currency.csv: expected 2 columns, got {len(raw.columns)}"
    return raw.toDF('country', 'currency').selectExpr(
        "coalesce(`country`, '') AS country",
        "coalesce(`currency`, '') AS currency")

# COMMAND ----------
# jn_Country_FX: Join (inner) on currency
@dp.temporary_view(name="j09_lnk_cc_fx")
def j09_lnk_cc_fx():
    return spark.sql(f"""
SELECT
  l.country,
  l.currency,
  r.YearMonth,
  r.RateFromGBP
FROM j09_lnk_cc l
INNER JOIN j09_lnk_fx r ON l.currency = r.currency
""")

# COMMAND ----------
# xfm_FX_Key: Transformer
@dp.temporary_view(name="j09_lnk_dim_fx")
def j09_lnk_dim_fx():
    return spark.sql(f"""
SELECT
  CAST(trim(country) AS string) AS country,
  CAST(replace(YearMonth, '-', '') AS string) AS year_month,
  currency,
  RateFromGBP AS fx_rate
FROM j09_lnk_cc_fx
""")

# COMMAND ----------
# tgt_dim_fx: target file -> materialized view dim_fx
@dp.materialized_view(name="dim_fx")
def dim_fx():
    return spark.read.table("j09_lnk_dim_fx").select('country', 'year_month', 'currency', 'fx_rate')
