# Databricks notebook source
# J09_Dim_Date_Promo_FX - Build Dim_Date, Dim_Promotion and Dim_FX (country x month)
# Layer: 2_Dimensions. Compiled from the DataStage job by migration/ds2spark.py; do not edit by hand.

# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "retail_dw")
dbutils.widgets.text("src_dir", "")
dbutils.widgets.text("pRunDate", "2011-12-31")
dbutils.widgets.text("pEffDate", "2011-07-01")
catalog, schema, src_dir = (dbutils.widgets.get(k) for k in ("catalog", "schema", "src_dir"))
pRunDate, pEffDate = dbutils.widgets.get("pRunDate"), dbutils.widgets.get("pEffDate")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------
# src_calendar: source file calendar.csv (columns mapped by position, as DataStage does)
_lnk_cal_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/calendar.csv"))
assert len(_lnk_cal_raw.columns) == 10, f"calendar.csv: expected 10 columns, got {len(_lnk_cal_raw.columns)}"
_lnk_cal_raw = _lnk_cal_raw.toDF('DateKey', 'FullDate', 'Year', 'Quarter', 'Month', 'MonthName', 'Day', 'DayOfWeekISO', 'IsWeekend', 'IsHoliday')
_lnk_cal_raw.createOrReplaceTempView("_lnk_cal_raw")
lnk_cal = spark.sql(f"""SELECT
  CAST(`DateKey` AS int) AS DateKey,
  CAST(`FullDate` AS date) AS FullDate,
  CAST(`Year` AS int) AS Year,
  CAST(`Quarter` AS int) AS Quarter,
  CAST(`Month` AS int) AS Month,
  coalesce(`MonthName`, '') AS MonthName,
  CAST(`Day` AS int) AS Day,
  CAST(`DayOfWeekISO` AS int) AS DayOfWeekISO,
  CAST(`IsWeekend` AS int) AS IsWeekend,
  CAST(`IsHoliday` AS int) AS IsHoliday
FROM _lnk_cal_raw""")
lnk_cal.createOrReplaceTempView("lnk_cal")

# COMMAND ----------
# xfm_Date_Attrs: Transformer
lnk_dim_date = spark.sql(f"""
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
FROM lnk_cal
""")
lnk_dim_date.createOrReplaceTempView("lnk_dim_date")

# COMMAND ----------
# tgt_dim_date: write Delta table dim_date
(lnk_dim_date.select('date_key', 'full_date', 'year', 'quarter', 'month', 'month_name', 'day', 'day_of_week', 'year_month', 'is_trading_day').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_date"))
print("dim_date:", spark.table(f"{catalog}.{schema}.dim_date").count(), "rows")

# COMMAND ----------
# src_promotions: source file promotions.csv (columns mapped by position, as DataStage does)
_lnk_promo_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/promotions.csv"))
assert len(_lnk_promo_raw.columns) == 5, f"promotions.csv: expected 5 columns, got {len(_lnk_promo_raw.columns)}"
_lnk_promo_raw = _lnk_promo_raw.toDF('PromoID', 'Category', 'StartDate', 'EndDate', 'DiscountPct')
_lnk_promo_raw.createOrReplaceTempView("_lnk_promo_raw")
lnk_promo = spark.sql(f"""SELECT
  coalesce(`PromoID`, '') AS PromoID,
  coalesce(`Category`, '') AS Category,
  CAST(`StartDate` AS date) AS StartDate,
  CAST(`EndDate` AS date) AS EndDate,
  CAST(`DiscountPct` AS int) AS DiscountPct
FROM _lnk_promo_raw""")
lnk_promo.createOrReplaceTempView("lnk_promo")

# COMMAND ----------
# xfm_Promo_SK: Transformer
lnk_dim_promo = spark.sql(f"""
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
FROM lnk_promo
""")
lnk_dim_promo.createOrReplaceTempView("lnk_dim_promo")

# COMMAND ----------
# tgt_dim_promotion: write Delta table dim_promotion
(lnk_dim_promo.select('promo_sk', 'promo_id', 'category', 'start_date', 'end_date', 'discount_pct', 'duration_days', 'start_key', 'end_key').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_promotion"))
print("dim_promotion:", spark.table(f"{catalog}.{schema}.dim_promotion").count(), "rows")

# COMMAND ----------
# src_fx_rates: source file fx_rates.csv (columns mapped by position, as DataStage does)
_lnk_fx_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/fx_rates.csv"))
assert len(_lnk_fx_raw.columns) == 3, f"fx_rates.csv: expected 3 columns, got {len(_lnk_fx_raw.columns)}"
_lnk_fx_raw = _lnk_fx_raw.toDF('YearMonth', 'currency', 'RateFromGBP')
_lnk_fx_raw.createOrReplaceTempView("_lnk_fx_raw")
lnk_fx = spark.sql(f"""SELECT
  coalesce(`YearMonth`, '') AS YearMonth,
  coalesce(`currency`, '') AS currency,
  CAST(`RateFromGBP` AS decimal(18,6)) AS RateFromGBP
FROM _lnk_fx_raw""")
lnk_fx.createOrReplaceTempView("lnk_fx")

# COMMAND ----------
# src_country_currency: source file country_currency.csv (columns mapped by position, as DataStage does)
_lnk_cc_raw = (spark.read.option("header", "true").option("escape", '"')
    .csv(f"{src_dir}/country_currency.csv"))
assert len(_lnk_cc_raw.columns) == 2, f"country_currency.csv: expected 2 columns, got {len(_lnk_cc_raw.columns)}"
_lnk_cc_raw = _lnk_cc_raw.toDF('country', 'currency')
_lnk_cc_raw.createOrReplaceTempView("_lnk_cc_raw")
lnk_cc = spark.sql(f"""SELECT
  coalesce(`country`, '') AS country,
  coalesce(`currency`, '') AS currency
FROM _lnk_cc_raw""")
lnk_cc.createOrReplaceTempView("lnk_cc")

# COMMAND ----------
# jn_Country_FX: Join (inner) on currency
lnk_cc_fx = spark.sql(f"""
SELECT
  l.country,
  l.currency,
  r.YearMonth,
  r.RateFromGBP
FROM lnk_cc l
INNER JOIN lnk_fx r ON l.currency = r.currency
""")
lnk_cc_fx.createOrReplaceTempView("lnk_cc_fx")

# COMMAND ----------
# xfm_FX_Key: Transformer
lnk_dim_fx = spark.sql(f"""
SELECT
  CAST(trim(country) AS string) AS country,
  CAST(replace(YearMonth, '-', '') AS string) AS year_month,
  currency,
  RateFromGBP AS fx_rate
FROM lnk_cc_fx
""")
lnk_dim_fx.createOrReplaceTempView("lnk_dim_fx")

# COMMAND ----------
# tgt_dim_fx: write Delta table dim_fx
(lnk_dim_fx.select('country', 'year_month', 'currency', 'fx_rate').write.mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_fx"))
print("dim_fx:", spark.table(f"{catalog}.{schema}.dim_fx").count(), "rows")
