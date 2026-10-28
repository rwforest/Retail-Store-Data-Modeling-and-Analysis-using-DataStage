"""The retail DW pipeline: 23 parallel jobs. Each function returns a Job built with dsx_builder.

Layers:  1_Land -> 2_Dimensions -> 3_Facts -> 4_Marts -> 5_DataQuality
Schemas are passed between jobs through REG (file path -> columns), so a downstream job
can never disagree with the job that wrote the file.
"""
from dsx_builder import Job

S = lambda f: f"#pSrcDir#/{f}"
STG = lambda f: f"#pStgDir#/{f}"
DWH = lambda f: f"#pDwhDir#/{f}"
MART = lambda f: f"#pMartDir#/{f}"
AUD = lambda f: f"#pAuditDir#/{f}"
REG = {}
SK = "@PARTITIONNUM + (@NUMPARTITIONS * (@INROWNUM - 1)) + 1"  # standard parallel surrogate-key idiom
TS_FMT = '"%yyyy-%mm-%dd %hh:%nn:%ss"'
D_FMT = '"%yyyy-%mm-%dd"'
RUN_KEY = 'StringToDecimal(Ereplace(pRunDate, "-", ""))'
EFF_KEY = 'StringToDecimal(Ereplace(pEffDate, "-", ""))'


def tgt(j, name, lk, path, append=False):
    j.seq_tgt(name, lk, path, append)
    REG[path] = [(c.name, c.type) for c in lk.cols]


def src(j, name, path, out):
    return j.seq_src(name, path, REG[path], out)


def nullfix(j, name, inp, out, defaults):
    """Replace nulls produced by an outer join so the link can be written to a file."""
    need = {c.name for c in inp.cols if c.nullable}
    assert need == set(defaults), f"{j.name}.{name}: nullable {need ^ set(defaults)}"
    return j.xfm(name, inp, out, add=[(c, inp.col(c).type, f"NullToValue({{i}}.{c}, {d})")
                                      for c, d in defaults.items()])


def sums(*specs):
    return [("sum", s, o, t) for s, o, t in specs]


# ============================================================ 1_Land ================================================
ORD_COLS = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID",
            "Country", "StoreID", "EmployeeID"]
NONPRODUCT = ["POST", "D", "M", "DOT", "CRUK", "BANK CHARGES", "AMAZONFEE", "S", "B", "C2", "PADS", "m"]


def j01_land_orders():
    j = Job("J01_Land_Orders", "Land raw Online Retail lines: type, flag, reject, de-duplicate, audit", "1_Land")
    raw = j.seq_src("src_orders_raw", S("orders_raw.csv"), [(c, "str") for c in ORD_COLS], "lnk_raw")
    nonprod = " Or ".join(f'Upcase(Trim({{i}}.StockCode)) = "{c.upper()}"' for c in NONPRODUCT)
    typed = j.xfm("xfm_Type_Orders", raw, "lnk_typed", only=(), add=[
        ("invoice_no", "str", "Trim({i}.InvoiceNo)"),
        ("stock_code", "str", "Upcase(Trim({i}.StockCode))"),
        ("description", "str", "Trim({i}.Description)"),
        ("quantity", "int", "Trim({i}.Quantity)"),
        ("invoice_ts", "ts", f"StringToTimestamp({{i}}.InvoiceDate, {TS_FMT})"),
        ("unit_price", "dec", "StringToDecimal(Trim({i}.UnitPrice))"),
        ("customer_id", "int", 'If Len(Trim({i}.CustomerID)) = 0 Then "0" Else Trim({i}.CustomerID)'),
        ("country", "str", "Trim({i}.Country)"),
        ("store_id", "str", "Trim({i}.StoreID)"),
        ("employee_id", "str", "Trim({i}.EmployeeID)"),
        ("invoice_date_key", "int",
         f'TimestampToString(StringToTimestamp({{i}}.InvoiceDate, {TS_FMT}), "%yyyy%mm%dd")'),
        ("line_amount", "dec", "StringToDecimal(Trim({i}.Quantity)) * StringToDecimal(Trim({i}.UnitPrice))"),
        ("is_cancel", "int", 'If Left(Trim({i}.InvoiceNo), 1) = "C" Then 1 Else 0'),
        ("is_nonproduct", "int", f"If {nonprod} Then 1 Else 0"),
        ("is_guest", "int", "If Len(Trim({i}.CustomerID)) = 0 Then 1 Else 0"),
    ])
    a, b = j.copy("cpy_Typed", typed, ["lnk_typed_ok", "lnk_typed_bad"])
    valid = j.filter("flt_Valid_Rows", a, "quantity <> 0 and unit_price >= 0", "lnk_valid", uses=["quantity", "unit_price"])
    dd = j.remdup("rmd_Dedup_Lines", valid, ["invoice_no", "stock_code", "invoice_ts", "customer_id", "quantity",
                                              "unit_price"], "lnk_dedup")
    stg, anom, cnt = j.copy("cpy_Orders", dd, ["lnk_stg", "lnk_anom_in", "lnk_cnt_in"])
    tgt(j, "tgt_stg_orders", stg, STG("stg_orders.csv"))
    bad = j.filter("flt_Invalid_Rows", b, "quantity = 0 or unit_price < 0", "lnk_invalid", uses=["quantity", "unit_price"])
    tgt(j, "tgt_rej_orders", bad, AUD("rej_orders.csv"))
    an = j.filter("flt_Anomalies", anom, "unit_price = 0 or quantity > 5000 or quantity < -5000", "lnk_anom",
                  uses=["unit_price", "quantity"])
    tgt(j, "tgt_dq_order_anomalies", an, AUD("dq_order_anomalies.csv"))
    by_c = j.agg("agg_Rows_By_Country", cnt, ["country"], "lnk_rows_by_country", count="rows_loaded")
    tgt(j, "tgt_audit_land_orders", by_c, AUD("audit_land_orders.csv"))
    return j


CUST_RAW = [("CustomerID", "int"), ("CustomerName", "str"), ("Gender", "str"), ("Age", "int"),
            ("SpendingScore", "int"), ("AnnualIncomeK", "int"), ("Country", "str"), ("CustomerType", "str"),
            ("LoyaltyTier", "str"), ("Email", "str"), ("City", "str"), ("SnapshotDate", "str")]


def j02_land_customers():
    j = Job("J02_Land_Customers", "Land the two customer snapshots, standardise and hash tracked attributes", "1_Land")
    for yr in ("2010", "2011"):
        r = j.seq_src(f"src_cust_{yr}", S(f"customers_snapshot_{yr}.csv"), CUST_RAW, f"lnk_raw_{yr}")
        x = j.xfm(f"xfm_Std_Cust_{yr}", r, f"lnk_std_{yr}", only=(), add=[
            ("customer_id", "int", "{i}.CustomerID"),
            ("cust_name", "str", "Upcase(Trim({i}.CustomerName))"),
            ("gender", "str", "Trim({i}.Gender)"),
            ("age", "int", "{i}.Age"),
            ("spending_score", "int", "{i}.SpendingScore"),
            ("annual_income_k", "int", "{i}.AnnualIncomeK"),
            ("country", "str", "Trim({i}.Country)"),
            ("customer_type", "str", "Trim({i}.CustomerType)"),
            ("loyalty_tier", "str", "Upcase(Trim({i}.LoyaltyTier))"),
            ("email", "str", "Downcase(Trim({i}.Email))"),
            ("city", "str", "Trim({i}.City)"),
            ("snapshot_date", "date", f"StringToDate({{i}}.SnapshotDate, {D_FMT})"),
            ("attr_hash", "bigint", 'CRC32(Upcase(Trim({i}.CustomerName)) : "|" : {i}.Age : "|" : {i}.SpendingScore'
                                    ' : "|" : {i}.AnnualIncomeK : "|" : Trim({i}.Country) : "|" : Upcase(Trim({i}.LoyaltyTier))'
                                    ' : "|" : Trim({i}.City) : "|" : Downcase(Trim({i}.Email)))'),
        ])
        d = j.remdup(f"rmd_Cust_{yr}", x, ["customer_id"], f"lnk_dedup_{yr}")
        tgt(j, f"tgt_stg_customers_{yr}", d, STG(f"stg_customers_{yr}.csv"))
    return j


def j03_land_products():
    j = Job("J03_Land_Products", "Land product catalogue + suppliers, derive cost and price band", "1_Land")
    p = j.seq_src("src_products", S("products_catalog.csv"),
                  [("StockCode", "str"), ("Description", "str"), ("Category", "str"), ("ListPrice", "dec"),
                   ("SupplierID", "str"), ("Status", "str")], "lnk_products")
    s = j.seq_src("src_suppliers", S("suppliers.csv"),
                  [("SupplierID", "str"), ("SupplierName", "str"), ("SupplierCountry", "str"),
                   ("LeadTimeDays", "int"), ("ReliabilityScore", "dec")], "lnk_suppliers")
    px = j.xfm("xfm_Std_Product", p, "lnk_std_prod", only=(), add=[
        ("stock_code", "str", "Upcase(Trim({i}.StockCode))"),
        ("product_name", "str", 'If Len(Trim({i}.Description)) = 0 Then "UNKNOWN" Else Upcase(Trim({i}.Description))'),
        ("category", "str", "Trim({i}.Category)"),
        ("list_price", "dec", "{i}.ListPrice"),
        ("supplier_id", "str", "Trim({i}.SupplierID)"),
        ("status", "str", "Trim({i}.Status)"),
        ("unit_cost", "dec", "{i}.ListPrice * 0.55"),
        ("price_band", "str", 'If {i}.ListPrice < 1 Then "LOW" Else If {i}.ListPrice < 5 Then "MID" Else "HIGH"'),
    ])
    sx = j.xfm("xfm_Std_Supplier", s, "lnk_std_sup", only=(), add=[
        ("supplier_id", "str", "Trim({i}.SupplierID)"), ("supplier_name", "str", "Trim({i}.SupplierName)"),
        ("supplier_country", "str", "Trim({i}.SupplierCountry)"), ("lead_time_days", "int", "{i}.LeadTimeDays"),
        ("reliability_score", "dec", "{i}.ReliabilityScore")])
    jn = j.join("jn_Product_Supplier", px, sx, ["supplier_id"], "lnk_prod_sup", how="left")
    nf = nullfix(j, "xfm_Supplier_Defaults", jn, "lnk_prod_sup_nn",
                 {"supplier_name": '"UNKNOWN"', "supplier_country": '"UNKNOWN"', "lead_time_days": "0",
                  "reliability_score": "0"})
    d = j.remdup("rmd_Products", nf, ["stock_code"], "lnk_dedup")
    a, b = j.copy("cpy_Products", d, ["lnk_all", "lnk_for_active"])
    tgt(j, "tgt_stg_products", a, STG("stg_products.csv"))
    act = j.filter("flt_Active_Products", b, 'status = "ACTIVE"', "lnk_active", uses=["status"])
    tgt(j, "tgt_stg_active_products", act, STG("stg_active_products.csv"))
    return j


def j04_land_stores():
    j = Job("J04_Land_Stores_Employees", "Land stores + employees, derive region and headcount", "1_Land")
    st = j.seq_src("src_stores", S("stores.csv"), [("StoreID", "str"), ("StoreName", "str"), ("Country", "str"),
                                                   ("Channel", "str"), ("Rating", "int"), ("NoOfEmployees", "int")],
                   "lnk_stores")
    em = j.seq_src("src_employees", S("employees.csv"), [("EmployeeID", "str"), ("StoreID", "str"),
                                                         ("EmployeeName", "str"), ("Role", "str"),
                                                         ("HireDate", "str")], "lnk_employees")
    sx = j.xfm("xfm_Store_Region", st, "lnk_std_store", only=(), add=[
        ("store_id", "str", "Trim({i}.StoreID)"), ("store_name", "str", "Upcase(Trim({i}.StoreName))"),
        ("country", "str", "Trim({i}.Country)"), ("channel", "str", "Trim({i}.Channel)"),
        ("rating", "int", "{i}.Rating"), ("no_of_employees", "int", "{i}.NoOfEmployees"),
        ("region", "str", 'If Trim({i}.Country) = "United Kingdom" Then "UK" Else If Trim({i}.Country) = "EIRE" '
                          'Then "UK&I" Else If Trim({i}.Channel) = "ONLINE" Then "ONLINE" Else "EUROPE_ROW"')])
    ex = j.xfm("xfm_Emp_Std", em, "lnk_std_emp", only=(), add=[
        ("employee_id", "str", "Trim({i}.EmployeeID)"), ("store_id", "str", "Trim({i}.StoreID)"),
        ("employee_name", "str", "Upcase(Trim({i}.EmployeeName))"), ("role", "str", "Upcase(Trim({i}.Role))"),
        ("hire_date", "date", f"StringToDate({{i}}.HireDate, {D_FMT})"),
        ("tenure_years", "int", f"DaysSinceFromDate(StringToDate(pRunDate, {D_FMT}), "
                                f"StringToDate({{i}}.HireDate, {D_FMT})) / 365")])
    e1, e2 = j.copy("cpy_Emp", ex, ["lnk_emp_out", "lnk_emp_cnt"])
    cnt = j.agg("agg_Emp_By_Store", e2, ["store_id"], "lnk_emp_by_store", count="emp_cnt")
    jn = j.join("jn_Store_EmpCount", sx, cnt, ["store_id"], "lnk_store_final")
    tgt(j, "tgt_stg_stores", jn, STG("stg_stores.csv"))
    tgt(j, "tgt_stg_employees", e1, STG("stg_employees.csv"))
    return j


# ============================================================ 2_Dimensions ==========================================
DIM_CUST = ["customer_sk", "customer_id", "cust_name", "gender", "age", "spending_score", "annual_income_k",
            "country", "customer_type", "loyalty_tier", "email", "city", "attr_hash", "version_no",
            "eff_start_key", "eff_end_key", "is_current"]
CUST_ATTRS = ["cust_name", "gender", "age", "spending_score", "annual_income_k", "country", "customer_type",
              "loyalty_tier", "email", "city", "attr_hash"]
DIM_TYPES = {"customer_sk": "int", "customer_id": "int", "attr_hash": "bigint", "version_no": "int",
             "eff_start_key": "int", "eff_end_key": "int", "is_current": "int", "age": "int",
             "spending_score": "int", "annual_income_k": "int"}


def dim_cust_adds(expr):
    """expr: dict col -> expression ({i} = input link). Emits adds in DIM_CUST order."""
    return [(c, DIM_TYPES.get(c, "str"), expr[c]) for c in DIM_CUST]


def j05_dim_customer_initial():
    j = Job("J05_Dim_Customer_Initial", "Initial load of Dim_Customer (SCD2 version 1) from the 2010 snapshot", "2_Dimensions")
    s = src(j, "src_stg_customers_2010", STG("stg_customers_2010.csv"), "lnk_stg")
    e = {c: "{i}." + c for c in CUST_ATTRS + ["customer_id"]}
    e.update(customer_sk="{i}.customer_id * 1000 + 1", version_no="1", eff_start_key="19000101",
             eff_end_key="99991231", is_current="1")
    x = j.xfm("xfm_Version1", s, "lnk_v1", only=(), add=dim_cust_adds(e))
    o = j.sort("srt_Dim_Customer", x, [("customer_id", "asc"), ("version_no", "asc")], "lnk_sorted")
    tgt(j, "tgt_dim_customer", o, DWH("dim_customer.csv"))
    return j


def j06_dim_customer_scd2():
    j = Job("J06_Dim_Customer_SCD2", "Apply the 2011 snapshot to Dim_Customer: expire changed rows, add new versions",
            "2_Dimensions")
    dim = src(j, "src_dim_customer", DWH("dim_customer.csv"), "lnk_dim")
    d_cur, d_his = j.copy("cpy_Dim", dim, ["lnk_dim_a", "lnk_dim_b"])
    cur = j.filter("flt_Current_Rows", d_cur, "is_current = 1", "lnk_current", uses=["is_current"])
    his = j.filter("flt_History_Rows", d_his, "is_current = 0", "lnk_history", uses=["is_current"])
    old = j.xfm("xfm_Prefix_Old", cur, "lnk_old", only=["customer_id"],
                add=[(f"old_{c}", DIM_TYPES.get(c, "str"), "{i}." + c) for c in DIM_CUST if c != "customer_id"])
    snap = src(j, "src_stg_customers_2011", STG("stg_customers_2011.csv"), "lnk_snap")
    jn = j.join("jn_Snapshot_vs_Current", snap, old, ["customer_id"], "lnk_cmp", how="left")
    olds = [c for c in jn.names if c.startswith("old_")]
    det = j.xfm("xfm_Detect_Change", jn, "lnk_detected", add=[
        ("change_flag", "str", 'If IsNull({i}.old_attr_hash) Then "N" Else If {i}.old_attr_hash <> {i}.attr_hash '
                               'Then "C" Else "U"')] +
        [(c, jn.col(c).type, f'NullToValue({{i}}.{c}, {0 if jn.col(c).type != "str" else chr(34) * 2})') for c in olds])
    u, nc, ch, au = j.copy("cpy_Detect", det, ["lnk_u", "lnk_nc", "lnk_c", "lnk_audit"])
    fu = j.filter("flt_Unchanged", u, 'change_flag = "U"', "lnk_unchanged", uses=["change_flag"])
    fn = j.filter("flt_New_Or_Changed", nc, 'change_flag <> "U"', "lnk_new_or_changed", uses=["change_flag"])
    fc = j.filter("flt_Changed", ch, 'change_flag = "C"', "lnk_changed", uses=["change_flag"])
    from_old = {c: "{i}.old_" + c for c in DIM_CUST if c != "customer_id"}
    from_old["customer_id"] = "{i}.customer_id"
    carry = j.xfm("xfm_Carry_Unchanged", fu, "lnk_carry", only=(), add=dim_cust_adds(from_old))
    exp = dict(from_old, is_current="0",
               eff_end_key='StringToDecimal(Ereplace(DateToString(DateFromDaysSince(-1, StringToDate(pEffDate, '
                           + D_FMT + ')), ' + D_FMT + '), "-", ""))')
    expired = j.xfm("xfm_Expire_Old", fc, "lnk_expired", only=(), add=dim_cust_adds(exp))
    new = {c: "{i}." + c for c in CUST_ATTRS + ["customer_id"]}
    new.update(customer_sk="{i}.customer_id * 1000 + {i}.old_version_no + 1", version_no="{i}.old_version_no + 1",
               eff_start_key=f"If {{i}}.old_version_no = 0 Then 19000101 Else {EFF_KEY}", eff_end_key="99991231",
               is_current="1")
    newv = j.xfm("xfm_New_Version", fn, "lnk_newver", only=(), add=dim_cust_adds(new))
    allr = j.funnel("fnl_Dim_Next", [his, carry, expired, newv], "lnk_all_versions")
    o = j.sort("srt_Dim_Next", allr, [("customer_id", "asc"), ("version_no", "asc")], "lnk_sorted")
    tgt(j, "tgt_dim_customer_next", o, DWH("dim_customer_next.csv"))
    cnt = j.agg("agg_Change_Counts", au, ["change_flag"], "lnk_change_counts", count="customers")
    tgt(j, "tgt_audit_scd2_customer", cnt, AUD("audit_scd2_customer.csv"))
    return j


def j07_dim_product():
    j = Job("J07_Dim_Product", "Build Dim_Product with surrogate key and category group", "2_Dimensions")
    s = src(j, "src_stg_products", STG("stg_products.csv"), "lnk_stg")
    d = j.remdup("rmd_Product", s, ["stock_code"], "lnk_dedup")
    o = j.sort("srt_Product", d, [("stock_code", "asc")], "lnk_sorted")
    x = j.xfm("xfm_Product_SK", o, "lnk_dim", add=[
        ("product_sk", "int", SK),
        ("category_group", "str", 'If {i}.category = "Seasonal" Then "SEASONAL" Else If {i}.category = '
                                  '"General Merchandise" Then "OTHER" Else "CORE"')])
    cols = ["product_sk", "stock_code", "product_name", "category", "category_group", "list_price", "unit_cost",
            "price_band", "status", "supplier_id", "supplier_name", "supplier_country", "lead_time_days",
            "reliability_score"]
    x2 = j.xfm("xfm_Product_Cols", x, "lnk_dim_cols", only=cols)
    a, b = j.copy("cpy_Dim_Product", x2, ["lnk_dim_out", "lnk_audit_in"])
    tgt(j, "tgt_dim_product", a, DWH("dim_product.csv"))
    cnt = j.agg("agg_Products_By_Category", b, ["category"], "lnk_cat_counts", count="products")
    tgt(j, "tgt_audit_products_by_category", cnt, AUD("audit_products_by_category.csv"))
    return j


def j08_dim_store_employee():
    j = Job("J08_Dim_Store_Employee", "Build Dim_Store and Dim_Employee with surrogate keys", "2_Dimensions")
    s = src(j, "src_stg_stores", STG("stg_stores.csv"), "lnk_stg_store")
    x = j.xfm("xfm_Store_SK", s, "lnk_dim_store", add=[("store_sk", "int", SK)])
    a, b = j.copy("cpy_Store", x, ["lnk_store_out", "lnk_store_keys_in"])
    tgt(j, "tgt_dim_store", a, DWH("dim_store.csv"))
    keys = j.xfm("xfm_Store_Keys", b, "lnk_store_keys", only=["store_id", "store_sk"])
    e = src(j, "src_stg_employees", STG("stg_employees.csv"), "lnk_stg_emp")
    jn = j.join("jn_Emp_Store", e, keys, ["store_id"], "lnk_emp_store")
    ex = j.xfm("xfm_Emp_SK", jn, "lnk_dim_emp", add=[("employee_sk", "int", SK)])
    tgt(j, "tgt_dim_employee", ex, DWH("dim_employee.csv"))
    return j


def j09_dim_date_promo_fx():
    j = Job("J09_Dim_Date_Promo_FX", "Build Dim_Date, Dim_Promotion and Dim_FX (country x month)", "2_Dimensions")
    c = j.seq_src("src_calendar", S("calendar.csv"), [
        ("DateKey", "int"), ("FullDate", "date"), ("Year", "int"), ("Quarter", "int"), ("Month", "int"),
        ("MonthName", "str"), ("Day", "int"), ("DayOfWeekISO", "int"), ("IsWeekend", "int"), ("IsHoliday", "int")],
        "lnk_cal")
    cx = j.xfm("xfm_Date_Attrs", c, "lnk_dim_date", only=(), add=[
        ("date_key", "int", "{i}.DateKey"), ("full_date", "date", "{i}.FullDate"), ("year", "int", "{i}.Year"),
        ("quarter", "int", "{i}.Quarter"), ("month", "int", "{i}.Month"), ("month_name", "str", "{i}.MonthName"),
        ("day", "int", "{i}.Day"), ("day_of_week", "int", "{i}.DayOfWeekISO"),
        ("year_month", "str", 'DateToString({i}.FullDate, "%yyyy%mm")'),
        ("is_trading_day", "int", "If {i}.IsWeekend = 0 And {i}.IsHoliday = 0 Then 1 Else 0")])
    tgt(j, "tgt_dim_date", cx, DWH("dim_date.csv"))
    p = j.seq_src("src_promotions", S("promotions.csv"), [
        ("PromoID", "str"), ("Category", "str"), ("StartDate", "date"), ("EndDate", "date"), ("DiscountPct", "int")],
        "lnk_promo")
    px = j.xfm("xfm_Promo_SK", p, "lnk_dim_promo", only=(), add=[
        ("promo_sk", "int", SK), ("promo_id", "str", "Trim({i}.PromoID)"), ("category", "str", "Trim({i}.Category)"),
        ("start_date", "date", "{i}.StartDate"), ("end_date", "date", "{i}.EndDate"),
        ("discount_pct", "int", "{i}.DiscountPct"),
        ("duration_days", "int", "DaysSinceFromDate({i}.EndDate, {i}.StartDate) + 1"),
        ("start_key", "int", 'DateToString({i}.StartDate, "%yyyy%mm%dd")'),
        ("end_key", "int", 'DateToString({i}.EndDate, "%yyyy%mm%dd")')])
    tgt(j, "tgt_dim_promotion", px, DWH("dim_promotion.csv"))
    f = j.seq_src("src_fx_rates", S("fx_rates.csv"), [("YearMonth", "str"), ("currency", "str"), ("RateFromGBP", "rate")],
                  "lnk_fx")
    cc = j.seq_src("src_country_currency", S("country_currency.csv"), [("country", "str"), ("currency", "str")],
                   "lnk_cc")
    jn = j.join("jn_Country_FX", cc, f, ["currency"], "lnk_cc_fx")
    fx = j.xfm("xfm_FX_Key", jn, "lnk_dim_fx", only=(), add=[
        ("country", "str", "Trim({i}.country)"), ("year_month", "str", 'Ereplace({i}.YearMonth, "-", "")'),
        ("currency", "str", "{i}.currency"), ("fx_rate", "rate", "{i}.RateFromGBP")])
    tgt(j, "tgt_dim_fx", fx, DWH("dim_fx.csv"))
    return j


# ============================================================ 3_Facts ===============================================
def j10_fact_sales():
    j = Job("J10_Fact_Sales", "Fact_Sales at invoice-line grain with point-in-time customer SCD2 lookup", "3_Facts")
    o = src(j, "src_stg_orders", STG("stg_orders.csv"), "lnk_orders")
    ln = j.filter("flt_Sales_Lines", o, "is_cancel = 0 and is_nonproduct = 0 and quantity > 0", "lnk_lines",
                  uses=["is_cancel", "is_nonproduct", "quantity"])
    k = j.xfm("xfm_Fact_Keys", ln, "lnk_keyed", add=[("year_month", "str", 'TimestampToString({i}.invoice_ts, "%yyyy%mm")')])
    # customer: point-in-time SCD2 lookup
    dc = src(j, "src_dim_customer", DWH("dim_customer.csv"), "lnk_dim_customer")
    jc = j.join("jn_Customer", k, dc, ["customer_id"], "lnk_c", how="left",
                take=["customer_sk", "eff_start_key", "eff_end_key", "customer_type", "loyalty_tier"])
    pit = j.filter("flt_PIT_Customer", jc, "customer_sk is null or (eff_start_key <= invoice_date_key and "
                   "eff_end_key >= invoice_date_key)", "lnk_c_pit",
                   uses=["customer_sk", "eff_start_key", "eff_end_key", "invoice_date_key"])
    cd = j.xfm("xfm_Customer_Defaults", pit, "lnk_c_nn", drop=["eff_start_key", "eff_end_key"], add=[
        ("customer_sk", "int", "NullToValue({i}.customer_sk, -1)"),
        ("customer_type", "str", 'NullToValue({i}.customer_type, "unknown")'),
        ("loyalty_tier", "str", 'NullToValue({i}.loyalty_tier, "NONE")')])
    dp = src(j, "src_dim_product", DWH("dim_product.csv"), "lnk_dim_product")
    jp = j.join("jn_Product", cd, dp, ["stock_code"], "lnk_p", how="left",
                take=["product_sk", "category", "supplier_id", "unit_cost"])
    pd = nullfix(j, "xfm_Product_Defaults", jp, "lnk_p_nn", {
        "product_sk": "-1", "category": '"Unknown"', "supplier_id": '"UNKNOWN"', "unit_cost": "0"})
    ds = src(j, "src_dim_store", DWH("dim_store.csv"), "lnk_dim_store")
    js = j.join("jn_Store", pd, ds, ["store_id"], "lnk_s", take=["store_sk", "region"])
    de = src(j, "src_dim_employee", DWH("dim_employee.csv"), "lnk_dim_emp")
    je = j.join("jn_Employee", js, de, ["employee_id"], "lnk_e", take=["employee_sk"])
    df = src(j, "src_dim_fx", DWH("dim_fx.csv"), "lnk_dim_fx")
    jf = j.join("jn_FX", je, df, ["country", "year_month"], "lnk_f", how="left", take=["currency", "fx_rate"])
    fd = nullfix(j, "xfm_FX_Defaults", jf, "lnk_f_nn", {"currency": '"GBP"', "fx_rate": "1"})
    keep = ["invoice_no", "stock_code", "invoice_ts", "invoice_date_key", "year_month", "customer_id", "customer_sk",
            "customer_type", "loyalty_tier", "product_sk", "category", "store_id", "store_sk", "region",
            "employee_sk", "country", "currency", "quantity", "unit_price"]
    m = j.xfm("xfm_Fact_Measures", fd, "lnk_fact", only=keep, add=[
        ("sales_amount", "dec", "{i}.line_amount"),
        ("cost_amount", "dec", "{i}.quantity * {i}.unit_cost"),
        ("gross_profit", "dec", "{i}.line_amount - ({i}.quantity * {i}.unit_cost)"),
        ("local_amount", "dec", "{i}.line_amount * {i}.fx_rate"),
        ("line_cnt", "int", "1")])
    srt = j.sort("srt_Fact_Sales", m, [("invoice_no", "asc"), ("stock_code", "asc")], "lnk_sorted")
    a, orph, au = j.copy("cpy_Fact", srt, ["lnk_fact_out", "lnk_orph_in", "lnk_audit_in"])
    tgt(j, "tgt_fact_sales", a, DWH("fact_sales.csv"))
    fo = j.filter("flt_Orphan_Product", orph, "product_sk = -1", "lnk_orphans", uses=["product_sk"])
    tgt(j, "tgt_dq_orphan_products", fo, AUD("dq_orphan_products.csv"))
    am = j.agg("agg_Fact_By_Month", au, ["year_month"], "lnk_by_month",
               calc=sums(("sales_amount", "sales_amount", "dec"), ("gross_profit", "gross_profit", "dec")))
    tgt(j, "tgt_audit_fact_by_month", am, AUD("audit_fact_by_month.csv"))
    return j


def j11_fact_returns():
    j = Job("J11_Fact_Returns", "Fact_Returns from cancellation invoices (prefix C)", "3_Facts")
    o = src(j, "src_stg_orders", STG("stg_orders.csv"), "lnk_orders")
    c = j.filter("flt_Cancels", o, "is_cancel = 1 and is_nonproduct = 0", "lnk_cancels", uses=["is_cancel", "is_nonproduct"])
    r = j.xfm("xfm_Return_Measures", c, "lnk_ret", only=["invoice_no", "stock_code", "customer_id", "invoice_date_key",
                                                       "country"], add=[
        ("year_month", "str", 'TimestampToString({i}.invoice_ts, "%yyyy%mm")'),
        ("returned_qty", "int", "Abs({i}.quantity)"),
        ("returned_amount", "dec", "Abs({i}.line_amount)")])
    dp = src(j, "src_dim_product", DWH("dim_product.csv"), "lnk_dim_product")
    jp = j.join("jn_Product", r, dp, ["stock_code"], "lnk_p", how="left", take=["product_sk", "category"])
    pd = nullfix(j, "xfm_Product_Defaults", jp, "lnk_p_nn", {"product_sk": "-1", "category": '"Unknown"'})
    dc = src(j, "src_dim_customer", DWH("dim_customer.csv"), "lnk_dim_customer")
    cur = j.filter("flt_Current_Customer", dc, "is_current = 1", "lnk_cur", uses=["is_current"])
    jc = j.join("jn_Customer", pd, cur, ["customer_id"], "lnk_c", how="left", take=["customer_sk"])
    cd = nullfix(j, "xfm_Customer_Defaults", jc, "lnk_c_nn", {"customer_sk": "-1"})
    srt = j.sort("srt_Fact_Returns", cd, [("invoice_no", "asc"), ("stock_code", "asc")], "lnk_sorted")
    a, b = j.copy("cpy_Returns", srt, ["lnk_out", "lnk_audit_in"])
    tgt(j, "tgt_fact_returns", a, DWH("fact_returns.csv"))
    ag = j.agg("agg_Returns_By_Category", b, ["category"], "lnk_by_cat",
               calc=sums(("returned_amount", "returned_amount", "dec")))
    tgt(j, "tgt_audit_returns_by_category", ag, AUD("audit_returns_by_category.csv"))
    return j


def j12_fact_invoice():
    j = Job("J12_Fact_Invoice", "Invoice-grain fact (basket level) rolled up from Fact_Sales", "3_Facts")
    f = src(j, "src_fact_sales", DWH("fact_sales.csv"), "lnk_fact")
    a = j.agg("agg_Invoice", f, ["invoice_no", "customer_id", "customer_sk", "store_sk", "employee_sk", "invoice_date_key", "year_month"],
              "lnk_inv", calc=sums(("sales_amount", "invoice_sales", "dec"), ("gross_profit", "invoice_profit", "dec"),
                                   ("quantity", "item_qty", "bigint"), ("line_cnt", "line_total", "bigint")))
    x = j.xfm("xfm_Basket_Metrics", a, "lnk_basket", add=[
        ("avg_line_value", "dec", "If {i}.line_total > 0 Then {i}.invoice_sales / {i}.line_total Else 0"),
        ("invoice_cnt", "int", "1")])
    s = j.sort("srt_Fact_Invoice", x, [("invoice_no", "asc")], "lnk_sorted")
    tgt(j, "tgt_fact_invoice", s, DWH("fact_invoice.csv"))
    return j


# ============================================================ 4_Marts ===============================================
def j13_mart_retail_data_mart():
    j = Job("J13_Mart_RETAIL_DATA_MART", "RETAIL_DATA_MART: every identified-customer transaction, citizen/foreign, with product & store",
            "4_Marts")
    f = src(j, "src_fact_sales", DWH("fact_sales.csv"), "lnk_fact")
    dc = src(j, "src_dim_customer", DWH("dim_customer.csv"), "lnk_dim_customer")
    # customer_sk identifies the customer *version* in force at purchase time, so join all versions
    jc = j.join("jn_Customer", f, dc, ["customer_sk"], "lnk_c",
                take=["cust_name", "gender", "age", "spending_score", "annual_income_k"])
    dp = src(j, "src_dim_product", DWH("dim_product.csv"), "lnk_dim_product")
    jp = j.join("jn_Product", jc, dp, ["product_sk"], "lnk_p", take=["product_name", "list_price"])
    ds = src(j, "src_dim_store", DWH("dim_store.csv"), "lnk_dim_store")
    js = j.join("jn_Store", jp, ds, ["store_sk"], "lnk_s", take=["store_name", "rating", "no_of_employees"])
    ft = j.filter("flt_Customer_Types", js, 'customer_type = "citizen" or customer_type = "foreign"', "lnk_types",
                  uses=["customer_type"])
    up = j.xfm("xfm_Upper_Names", ft, "lnk_upper", add=[("cust_name", "str", "Upcase({i}.cust_name)"),
                                                          ("product_name", "str", "Upcase({i}.product_name)")])
    s = j.sort("srt_Mart", up, [("invoice_no", "asc"), ("stock_code", "asc")], "lnk_sorted")
    tgt(j, "tgt_mart_retail_data_mart", s, MART("RETAIL_DATA_MART.csv"))
    return j


def j14_q1_employee_transactions():
    j = Job("J14_Analysis_Employee_Transactions", "Q1: number of transactions per employee in each store", "4_Marts")
    m = src(j, "src_mart", MART("RETAIL_DATA_MART.csv"), "lnk_mart")
    d = j.remdup("rmd_Invoices", m, ["store_sk", "employee_sk", "invoice_no"], "lnk_invoices")
    a = j.agg("agg_Transactions", d, ["store_sk", "employee_sk"], "lnk_counts", count="total_transactions")
    de = src(j, "src_dim_employee", DWH("dim_employee.csv"), "lnk_dim_emp")
    jn = j.join("jn_Employee", a, de, ["employee_sk"], "lnk_named",
                take=["employee_id", "employee_name", "role"])
    s = j.sort("srt_Store_Rank", jn, [("store_sk", "asc"), ("total_transactions", "desc")], "lnk_ranked")
    x, y = j.copy("cpy_Ranked", s, ["lnk_all", "lnk_top_in"])
    tgt(j, "tgt_employee_transactions", x, MART("Q1_Employee_Transactions.csv"))
    h = j.head("hd_Top10", y, 10, "lnk_top10")
    tgt(j, "tgt_top10_employees", h, MART("Q1_Top10_Employees.csv"))
    return j


def j15_q2_customer_type_profit():
    j = Job("J15_Analysis_CustomerType_Profit", "Q2: which customer type (citizen/foreign) generates the highest profit", "4_Marts")
    m = src(j, "src_mart", MART("RETAIL_DATA_MART.csv"), "lnk_mart")
    a = j.agg("agg_By_Type", m, ["customer_type"], "lnk_by_type", calc=sums(
        ("gross_profit", "profit_total", "dec"), ("sales_amount", "sales_total", "dec"), ("line_cnt", "lines", "bigint")))
    s = j.sort("srt_Profit", a, [("profit_total", "desc")], "lnk_ranked")
    x, y = j.copy("cpy_Ranked", s, ["lnk_all", "lnk_win_in"])
    tgt(j, "tgt_customer_type_ranking", x, MART("Q2_CustomerType_Profit.csv"))
    h = j.head("hd_Winner", y, 1, "lnk_winner")
    tgt(j, "tgt_customer_type_winner", h, MART("Q2_Winning_CustomerType.csv"))
    return j


def j16_q3_bonus_program():
    j = Job("J16_Bonus_Program", "Q3: bonus for the customers contributing the most profit, scaled by loyalty tier", "4_Marts")
    m = src(j, "src_mart", MART("RETAIL_DATA_MART.csv"), "lnk_mart")
    a = j.agg("agg_Customer_Profit", m, ["customer_id"], "lnk_cust_profit",
              calc=sums(("gross_profit", "profit_total", "dec")))
    s = j.sort("srt_Profit", a, [("profit_total", "desc")], "lnk_ranked")
    h = j.head("hd_Top100", s, 100, "lnk_top100")
    dc = src(j, "src_dim_customer", DWH("dim_customer.csv"), "lnk_dim_customer")
    cur = j.filter("flt_Current_Customer", dc, "is_current = 1", "lnk_cur", uses=["is_current"])
    jn = j.join("jn_Customer", h, cur, ["customer_id"], "lnk_named",
                take=["customer_sk", "cust_name", "customer_type", "loyalty_tier"])
    x = j.xfm("xfm_Bonus", jn, "lnk_bonus", add=[
        ("bonus_rate", "rate", 'If {i}.loyalty_tier = "PLATINUM" Then 0.05 Else If {i}.loyalty_tier = "GOLD" Then 0.04 '
                               'Else If {i}.loyalty_tier = "SILVER" Then 0.03 Else 0.02'),
        ("bonus_amount", "dec", 'If {i}.loyalty_tier = "PLATINUM" Then {i}.profit_total * 0.05 Else If {i}.loyalty_tier = '
                                '"GOLD" Then {i}.profit_total * 0.04 Else If {i}.loyalty_tier = "SILVER" Then '
                                '{i}.profit_total * 0.03 Else {i}.profit_total * 0.02'),
        ("program", "str", '"BONUS_2011"')])
    b1, b2 = j.copy("cpy_Bonus", x, ["lnk_out", "lnk_total_in"])
    tgt(j, "tgt_bonus_program", b1, MART("Q3_Bonus_Program.csv"))
    t = j.agg("agg_Bonus_Total", b2, ["program"], "lnk_total", calc=sums(("bonus_amount", "bonus_total", "dec")))
    tgt(j, "tgt_bonus_summary", t, MART("Q3_Bonus_Summary.csv"))
    return j


def j17_mart_customer_360():
    j = Job("J17_Mart_Customer_360", "Customer 360 with RFM scoring, returns and segment", "4_Marts")
    i = src(j, "src_fact_invoice", DWH("fact_invoice.csv"), "lnk_inv")
    k = j.filter("flt_Known_Customers", i, "customer_id > 0", "lnk_known", uses=["customer_id"])
    rev = j.agg("agg_Cust_Revenue", k, ["customer_id"], "lnk_rev", calc=sums(
        ("invoice_sales", "total_sales", "dec"), ("invoice_profit", "total_profit", "dec"),
        ("invoice_cnt", "invoice_total", "bigint")) + [("min", "invoice_date_key", "first_date_key", "int"),
                                                       ("max", "invoice_date_key", "last_date_key", "int")])
    r = src(j, "src_fact_returns", DWH("fact_returns.csv"), "lnk_ret")
    rk = j.filter("flt_Known_Returns", r, "customer_id > 0", "lnk_ret_known", uses=["customer_id"])
    ra = j.agg("agg_Cust_Returns", rk, ["customer_id"], "lnk_ret_agg", calc=sums(("returned_amount", "total_returned", "dec")))
    jn = j.join("jn_Revenue_Returns", rev, ra, ["customer_id"], "lnk_rr", how="left")
    nf = nullfix(j, "xfm_Returns_Defaults", jn, "lnk_rr_nn", {"total_returned": "0"})
    dc = src(j, "src_dim_customer", DWH("dim_customer.csv"), "lnk_dim_customer")
    cur = j.filter("flt_Current_Customer", dc, "is_current = 1", "lnk_cur", uses=["is_current"])
    ja = j.join("jn_Customer_Attrs", nf, cur, ["customer_id"], "lnk_attrs",
                take=["customer_sk", "cust_name", "country", "customer_type", "loyalty_tier", "age"])
    rec = (f'DaysSinceFromDate(StringToDate(pRunDate, {D_FMT}), StringToDate({{i}}.last_date_key, "%yyyy%mm%dd"))')
    x = j.xfm("xfm_RFM", ja, "lnk_rfm", add=[
        ("recency_days", "int", rec),
        ("r_score", "int", f"If {rec} <= 30 Then 5 Else If {rec} <= 90 Then 4 Else If {rec} <= 180 Then 3 Else "
                           f"If {rec} <= 270 Then 2 Else 1"),
        ("f_score", "int", "If {i}.invoice_total >= 10 Then 5 Else If {i}.invoice_total >= 6 Then 4 Else If "
                           "{i}.invoice_total >= 3 Then 3 Else If {i}.invoice_total >= 2 Then 2 Else 1"),
        ("m_score", "int", "If {i}.total_sales >= 5000 Then 5 Else If {i}.total_sales >= 2000 Then 4 Else If "
                           "{i}.total_sales >= 800 Then 3 Else If {i}.total_sales >= 300 Then 2 Else 1"),
        ("net_sales", "dec", "{i}.total_sales - {i}.total_returned")])
    y = j.xfm("xfm_Segment", x, "lnk_seg", add=[
        ("segment", "str", 'If {i}.r_score >= 4 And {i}.f_score >= 4 Then "CHAMPION" Else If {i}.r_score >= 3 And '
                           '{i}.f_score >= 3 Then "LOYAL" Else If {i}.r_score >= 4 Then "RECENT" Else If '
                           '{i}.r_score <= 2 And {i}.f_score >= 3 Then "AT_RISK" Else "HIBERNATING"')])
    s = j.sort("srt_Customer_360", y, [("net_sales", "desc")], "lnk_sorted")
    a, b = j.copy("cpy_360", s, ["lnk_out", "lnk_seg_in"])
    tgt(j, "tgt_mart_customer_360", a, MART("Customer_360.csv"))
    sc = j.agg("agg_Segments", b, ["segment"], "lnk_seg_counts", count="customers")
    tgt(j, "tgt_segment_summary", sc, MART("Customer_Segments.csv"))
    return j


def j18_mart_product_performance():
    j = Job("J18_Mart_Product_Performance", "Product performance: revenue, margin, return rate, ABC class", "4_Marts")
    f = src(j, "src_fact_sales", DWH("fact_sales.csv"), "lnk_fact")
    a = j.agg("agg_Product_Sales", f, ["product_sk"], "lnk_sales", calc=sums(
        ("quantity", "units_sold", "bigint"), ("sales_amount", "revenue", "dec"), ("gross_profit", "profit", "dec")))
    r = src(j, "src_fact_returns", DWH("fact_returns.csv"), "lnk_ret")
    ra = j.agg("agg_Product_Returns", r, ["product_sk"], "lnk_ret_agg", calc=sums(("returned_amount", "returned", "dec")))
    jn = j.join("jn_Sales_Returns", a, ra, ["product_sk"], "lnk_sr", how="left")
    nf = nullfix(j, "xfm_Returns_Defaults", jn, "lnk_sr_nn", {"returned": "0"})
    k = j.xfm("xfm_KPIs", nf, "lnk_kpi", add=[
        ("return_rate", "rate", "If {i}.revenue > 0 Then {i}.returned / {i}.revenue Else 0"),
        ("margin_pct", "rate", "If {i}.revenue > 0 Then {i}.profit / {i}.revenue Else 0"),
        ("abc_class", "str", 'If {i}.revenue >= 5000 Then "A" Else If {i}.revenue >= 1000 Then "B" Else "C"')])
    dp = src(j, "src_dim_product", DWH("dim_product.csv"), "lnk_dim_product")
    jp = j.join("jn_Product", k, dp, ["product_sk"], "lnk_named",
                take=["stock_code", "product_name", "category", "supplier_name"])
    s = j.sort("srt_Revenue", jp, [("revenue", "desc")], "lnk_sorted")
    x, y = j.copy("cpy_Perf", s, ["lnk_all", "lnk_top_in"])
    tgt(j, "tgt_mart_product_performance", x, MART("Product_Performance.csv"))
    h = j.head("hd_Top50", y, 50, "lnk_top50")
    tgt(j, "tgt_top50_products", h, MART("Product_Top50.csv"))
    return j


def j19_mart_basket_affinity():
    j = Job("J19_Mart_Basket_Affinity", "Market-basket: product pairs bought on the same invoice", "4_Marts")
    f = src(j, "src_fact_sales", DWH("fact_sales.csv"), "lnk_fact")
    q = j.filter("flt_Positive_Lines", f, "quantity > 0", "lnk_pos", uses=["quantity"])
    p = j.xfm("xfm_Pair_Prep", q, "lnk_prep", only=["invoice_no", "stock_code"])
    d = j.remdup("rmd_Invoice_Product", p, ["invoice_no", "stock_code"], "lnk_dedup")
    a, b = j.copy("cpy_Basket", d, ["lnk_a", "lnk_b_in"])
    bx = j.xfm("xfm_Rename_B", b, "lnk_b", only=["invoice_no"], add=[("stock_code_b", "str", "{i}.stock_code")])
    jn = j.join("jn_Self", a, bx, ["invoice_no"], "lnk_pairs_all")
    fp = j.filter("flt_Ordered_Pairs", jn, "stock_code < stock_code_b", "lnk_pairs", uses=["stock_code", "stock_code_b"])
    ag = j.agg("agg_Pair_Counts", fp, ["stock_code", "stock_code_b"], "lnk_counts", count="pair_count")
    fs = j.filter("flt_Min_Support", ag, "pair_count >= 5", "lnk_support", uses=["pair_count"])
    s = j.sort("srt_Pairs", fs, [("pair_count", "desc")], "lnk_sorted")
    h = j.head("hd_Top1000", s, 1000, "lnk_top")
    tgt(j, "tgt_mart_basket_affinity", h, MART("Basket_Affinity.csv"))
    return j


def j20_mart_store_monthly():
    j = Job("J20_Mart_Store_Monthly", "Store x month sales KPIs incl. sales per employee", "4_Marts")
    f = src(j, "src_fact_sales", DWH("fact_sales.csv"), "lnk_fact")
    a = j.agg("agg_Store_Month", f, ["store_sk", "year_month"], "lnk_sm", calc=sums(
        ("sales_amount", "sales", "dec"), ("gross_profit", "profit", "dec"), ("line_cnt", "lines", "bigint")))
    ds = src(j, "src_dim_store", DWH("dim_store.csv"), "lnk_dim_store")
    jn = j.join("jn_Store", a, ds, ["store_sk"], "lnk_named",
                take=["store_id", "store_name", "region", "no_of_employees"])
    x = j.xfm("xfm_Store_KPIs", jn, "lnk_kpi", add=[
        ("sales_per_employee", "dec", "If {i}.no_of_employees > 0 Then {i}.sales / {i}.no_of_employees Else 0"),
        ("margin_pct", "rate", "If {i}.sales > 0 Then {i}.profit / {i}.sales Else 0")])
    s = j.sort("srt_Store_Month", x, [("store_sk", "asc"), ("year_month", "asc")], "lnk_sorted")
    tgt(j, "tgt_mart_store_monthly", s, MART("Store_Monthly.csv"))
    return j


def j21_mart_promo_effectiveness():
    j = Job("J21_Mart_Promo_Effectiveness", "Sales inside each promotion window (range join on category + date)", "4_Marts")
    f = src(j, "src_fact_sales", DWH("fact_sales.csv"), "lnk_fact")
    p = src(j, "src_dim_promotion", DWH("dim_promotion.csv"), "lnk_dim_promo")
    jn = j.join("jn_Promo_Category", f, p, ["category"], "lnk_fp", left_cols=["category", "invoice_date_key", "sales_amount", "gross_profit", "line_cnt"],
                take=["promo_id", "discount_pct", "start_key", "end_key"])
    w = j.filter("flt_In_Window", jn, "invoice_date_key >= start_key and invoice_date_key <= end_key", "lnk_window",
                 uses=["invoice_date_key", "start_key", "end_key"])
    a = j.agg("agg_By_Promo", w, ["promo_id", "category", "discount_pct"], "lnk_promo", calc=sums(
        ("sales_amount", "promo_sales", "dec"), ("gross_profit", "promo_profit", "dec"), ("line_cnt", "promo_lines", "bigint")))
    x = j.xfm("xfm_Discount_Cost", a, "lnk_cost", add=[
        ("est_discount_cost", "dec", "{i}.promo_sales * {i}.discount_pct / 100"),
        ("profit_after_discount", "dec", "{i}.promo_profit - ({i}.promo_sales * {i}.discount_pct / 100)")])
    s = j.sort("srt_Promo", x, [("profit_after_discount", "desc")], "lnk_sorted")
    tgt(j, "tgt_mart_promo_effectiveness", s, MART("Promo_Effectiveness.csv"))
    return j


# ============================================================ 5_DataQuality =========================================
def j22_dq_checks():
    j = Job("J22_DQ_Fact_Checks", "Row-level data-quality rules on Fact_Sales, one result row per rule", "5_DataQuality")
    f = src(j, "src_fact_sales", DWH("fact_sales.csv"), "lnk_fact")
    branches = j.copy("cpy_Fact", f, [f"lnk_chk{i}" for i in range(1, 5)])
    rules = [("guest_customer_lines", "customer_sk = -1", ["customer_sk"]),
             ("orphan_product_lines", "product_sk = -1", ["product_sk"]),
             ("non_positive_sales", "sales_amount <= 0", ["sales_amount"]),
             ("negative_gross_profit", "gross_profit < 0", ["gross_profit"])]
    outs = []
    for n, (b, (rule, where, uses)) in enumerate(zip(branches, rules), 1):
        fl = j.filter(f"flt_{rule}", b, where, f"lnk_hit{n}", uses=uses)
        tg = j.xfm(f"xfm_Tag_{rule}", fl, f"lnk_tag{n}", only=(), add=[("check_name", "str", f'"{rule}"')])
        outs.append(j.agg(f"agg_Count_{rule}", tg, ["check_name"], f"lnk_cnt{n}", count="hits"))
    fn = j.funnel("fnl_Checks", outs, "lnk_all_checks")
    s = j.sort("srt_Checks", fn, [("check_name", "asc")], "lnk_sorted")
    tgt(j, "tgt_dq_fact_checks", s, AUD("dq_fact_checks.csv"))
    return j


def j23_reconcile():
    j = Job("J23_Reconcile_Source_To_Fact", "Reconcile staged order amounts to Fact_Sales amounts", "5_DataQuality")
    o = src(j, "src_stg_orders", STG("stg_orders.csv"), "lnk_orders")
    ln = j.filter("flt_Sales_Lines", o, "is_cancel = 0 and is_nonproduct = 0 and quantity > 0", "lnk_lines",
                  uses=["is_cancel", "is_nonproduct", "quantity"])
    k1 = j.xfm("xfm_Key_Stage", ln, "lnk_k1", only=["line_amount"], add=[("rec_key", "str", '"ALL"')])
    a1 = j.agg("agg_Stage_Amount", k1, ["rec_key"], "lnk_a1", calc=sums(("line_amount", "stage_amount", "dec")))
    f = src(j, "src_fact_sales", DWH("fact_sales.csv"), "lnk_fact")
    k2 = j.xfm("xfm_Key_Fact", f, "lnk_k2", only=["sales_amount"], add=[("rec_key", "str", '"ALL"')])
    a2 = j.agg("agg_Fact_Amount", k2, ["rec_key"], "lnk_a2", calc=sums(("sales_amount", "fact_amount", "dec")))
    jn = j.join("jn_Reconcile", a1, a2, ["rec_key"], "lnk_rec")
    x = j.xfm("xfm_Diff", jn, "lnk_diff", add=[
        ("diff_amount", "dec", "{i}.stage_amount - {i}.fact_amount"),
        ("status", "str", 'If Abs({i}.stage_amount - {i}.fact_amount) < 0.01 Then "PASS" Else "FAIL"')])
    tgt(j, "tgt_audit_reconciliation", x, AUD("audit_reconciliation.csv"))
    return j


JOB_BUILDERS = [j01_land_orders, j02_land_customers, j03_land_products, j04_land_stores,
                j05_dim_customer_initial, j06_dim_customer_scd2, j07_dim_product, j08_dim_store_employee,
                j09_dim_date_promo_fx, j10_fact_sales, j11_fact_returns, j12_fact_invoice,
                j13_mart_retail_data_mart, j14_q1_employee_transactions, j15_q2_customer_type_profit,
                j16_q3_bonus_program, j17_mart_customer_360, j18_mart_product_performance,
                j19_mart_basket_affinity, j20_mart_store_monthly, j21_mart_promo_effectiveness,
                j22_dq_checks, j23_reconcile]


def build_all():
    REG.clear()
    return [b() for b in JOB_BUILDERS]
