#!/usr/bin/env python3
"""Build the source files consumed by the DataStage pipeline (Source_Data/*.csv).

Real data : UCI "Online Retail" (id 352) -> orders_raw.csv  (the same dataset the
            original repo's Stockid/Invoiceid/Description/Quantity/Price columns came from).
Synthetic : everything else (customers, stores, employees, suppliers, promotions,
            FX rates, calendar). Deterministic (hash based), so reruns are identical.

Usage:
    python3 pipeline/prepare_sources.py --xlsx "Online Retail.xlsx"                  # sample, 1500 invoices
    python3 pipeline/prepare_sources.py --xlsx "Online Retail.xlsx" --max-invoices 0 # full, ~541k rows
Standard library only.
"""
import argparse, csv, datetime as dt, hashlib, re, statistics, sys, zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
ROOT = Path(__file__).resolve().parent.parent


def h(*parts, mod=1 << 32):
    """Deterministic pseudo-random int."""
    return int(hashlib.md5("|".join(map(str, parts)).encode()).hexdigest(), 16) % mod


def read_xlsx(path):
    with zipfile.ZipFile(path) as z:
        strings = []
        with z.open("xl/sharedStrings.xml") as f:
            for _, el in ET.iterparse(f):
                if el.tag == NS + "si":
                    strings.append("".join(t.text or "" for t in el.iter(NS + "t")))
                    el.clear()
        sheet = [n for n in z.namelist() if n.startswith("xl/worksheets/sheet")][0]
        with z.open(sheet) as f:
            for _, el in ET.iterparse(f):
                if el.tag != NS + "row":
                    continue
                row = {}
                for c in el.iter(NS + "c"):
                    col = re.match(r"[A-Z]+", c.get("r")).group()
                    v = c.find(NS + "v")
                    if v is None:
                        continue
                    row[col] = strings[int(v.text)] if c.get("t") == "s" else v.text
                yield [row.get(k, "") for k in "ABCDEFGH"]
                el.clear()


def serial_to_dt(s):
    return dt.datetime(1899, 12, 30) + dt.timedelta(days=float(s))


def write(name, header, rows):
    out = ROOT / "Source_Data"
    out.mkdir(exist_ok=True)
    with open(out / name, "w", newline="", encoding="cp1252", errors="replace") as f:
        w = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    print(f"  {name:28s} {len(rows):>8,} rows")


CATEGORY_RULES = [  # keyword -> category (rule based, deterministic)
    ("LIGHT|LANTERN|CANDLE|T-LIGHT|LAMP", "Lighting & Candles"),
    ("BAG|PURSE|WALLET|BASKET|BOX|TIN|JAR|CASE", "Storage & Bags"),
    ("MUG|CUP|TEAPOT|PLATE|BOWL|SPOON|CAKE|TEA|GLASS|KITCHEN", "Kitchen & Dining"),
    ("CARD|PAPER|WRAP|NOTEBOOK|PEN|PENCIL|STICKER|RIBBON", "Stationery & Gifting"),
    ("CHRISTMAS|XMAS|SNOW|SANTA|ADVENT", "Seasonal"),
    ("HEART|FLOWER|BIRD|BUTTERFLY|DECORATION|ORNAMENT|FRAME|MIRROR|CLOCK|SIGN", "Home Decor"),
    ("TOY|GAME|DOLL|PUZZLE|BALLOON|PARTY|CHILDREN|KIDS", "Toys & Party"),
]
CURRENCY = {"United Kingdom": "GBP", "EIRE": "EUR", "France": "EUR", "Germany": "EUR", "Netherlands": "EUR",
            "Spain": "EUR", "Belgium": "EUR", "Portugal": "EUR", "Italy": "EUR", "Austria": "EUR",
            "Finland": "EUR", "Switzerland": "CHF", "Norway": "NOK", "Sweden": "SEK", "Denmark": "DKK",
            "Australia": "AUD", "Japan": "JPY", "Canada": "CAD", "USA": "USD"}
FIRST = ["ALEX", "DAVID", "ROSE", "NANCY", "RUBIAN", "SARA", "OMAR", "LIN", "MAYA", "JONAS", "PRIYA", "LUCA",
         "HANA", "ELI", "NOOR", "TOM", "ANYA", "KEN", "ZOE", "RAUL"]
CITIES = ["London", "Manchester", "Leeds", "Bristol", "Dublin", "Paris", "Berlin", "Madrid", "Amsterdam", "Sydney"]
TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", required=True)
    ap.add_argument("--max-invoices", type=int, default=1500, help="0 = full dataset")
    a = ap.parse_args()

    print("reading", a.xlsx)
    it = read_xlsx(a.xlsx)
    next(it)  # header
    raw = list(it)
    if a.max_invoices:  # spread the sample across the whole year, not just the first invoices
        invs = sorted({r[0] for r in raw})
        step = max(1, len(invs) // a.max_invoices)
        keep = set(invs[::step])
        raw = [r for r in raw if r[0] in keep]
    print(f"  kept {len(raw):,} order lines")

    # ---- stores / employees (synthetic; names reuse the original repo's Retail.txt) -----------------------
    base = list(csv.DictReader(open(ROOT / "dataset" / "Retail.txt")))
    stores = [(f"S{100 + i}", b["name"], b["location"].replace("UnitedKingdom", "United Kingdom"),
               "STORE", int(b["rating"]), int(b["noofemployees"])) for i, b in enumerate(base)]
    extra = [("Germany", "KaDeWe"), ("France", "GaleriesLafayette"), ("EIRE", "BrownThomas"),
             ("Netherlands", "DeBijenkorf"), ("Spain", "ElCorteIngles"), ("Australia", "MyerSydney")]
    for i, (c, n) in enumerate(extra):
        stores.append((f"S{200 + i}", n, c, "STORE", 3 + h(n, mod=3), 800 + h(n, mod=3000)))
    stores.append(("S900", "GlobalOnline", "United Kingdom", "ONLINE", 4, 120))
    write("stores.csv", ["StoreID", "StoreName", "Country", "Channel", "Rating", "NoOfEmployees"], stores)
    by_country = defaultdict(list)
    for s in stores:
        if s[3] == "STORE":
            by_country[s[2]].append(s[0])
    employees = []
    for s in stores:
        for k in range(6):
            employees.append((f"E{s[0][1:]}{k:02d}", s[0], f"{FIRST[h(s[0], k, mod=len(FIRST))]}_{k}",
                              ["MANAGER", "CASHIER", "CASHIER", "SALES", "SALES", "SUPERVISOR"][k],
                              dt.date(2005 + h(s[0], k, 1, mod=6), 1 + h(s[0], k, 2, mod=12), 1).isoformat()))
    write("employees.csv", ["EmployeeID", "StoreID", "EmployeeName", "Role", "HireDate"], employees)
    emp_by_store = defaultdict(list)
    for e in employees:
        emp_by_store[e[1]].append(e[0])

    # ---- orders_raw: real data + StoreID/EmployeeID (synthetic assignment) ----------------------------------
    orders, invoice_ts = [], {}
    for inv, sc, desc, qty, ts, price, cust, ctry in raw:
        d = serial_to_dt(ts) if re.fullmatch(r"[\d.]+", ts or "") else None
        if d is None:
            continue
        cands = by_country.get(ctry) or ["S900"]
        sid = cands[h(inv, mod=len(cands))]
        eid = emp_by_store[sid][h(inv, "emp", mod=len(emp_by_store[sid]))]
        cust = cust.split(".")[0] if cust else ""
        price = f"{float(price):.2f}" if price else ""
        invoice_ts[inv] = d
        orders.append((inv, sc, desc, qty, d.strftime("%Y-%m-%d %H:%M:%S"), price, cust, ctry, sid, eid))
    write("orders_raw.csv", ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice",
                             "CustomerID", "Country", "StoreID", "EmployeeID"], orders)

    # ---- products ---------------------------------------------------------------------------------------------
    prices, descs = defaultdict(list), defaultdict(Counter)
    for o in orders:
        try:
            p = float(o[5])
        except ValueError:
            continue
        if p > 0:
            prices[o[1]].append(p)
        if o[2]:
            descs[o[1]][o[2].strip()] += 1
    prods = []
    for sc in sorted(prices):
        d = descs[sc].most_common(1)[0][0] if descs[sc] else ""
        cat = next((c for pat, c in CATEGORY_RULES if re.search(pat, d.upper())), "General Merchandise")
        prods.append((sc, d, cat, round(statistics.median(prices[sc]), 2), f"SUP{1 + h(sc, mod=12):02d}",
                      "DISCONTINUED" if h(sc, "st", mod=20) == 0 else "ACTIVE"))
    write("products_catalog.csv", ["StockCode", "Description", "Category", "ListPrice", "SupplierID", "Status"], prods)
    write("suppliers.csv", ["SupplierID", "SupplierName", "Country", "LeadTimeDays", "ReliabilityScore"],
          [(f"SUP{i:02d}", f"Supplier_{i:02d}", ["China", "India", "Poland", "Turkey", "Vietnam", "Portugal"][i % 6],
            7 + h(i, mod=40), round(0.7 + h(i, "r", mod=30) / 100, 2)) for i in range(1, 13)])

    # ---- customer snapshots (SCD2 input) ----------------------------------------------------------------------
    first_seen, country_of = {}, {}
    for o in orders:
        if o[6]:
            first_seen.setdefault(o[6], invoice_ts[o[0]])
            country_of.setdefault(o[6], o[7])

    def cust_row(cid, snap, ver):
        c = country_of[cid]
        tier = TIERS[h(cid, "tier", ver, mod=4)]
        return (int(cid), FIRST[h(cid, "n", mod=len(FIRST))], ["Male", "Female"][h(cid, "g", mod=2)],
                18 + h(cid, "a", mod=62), 1 + h(cid, "s", ver, mod=100), 10 + h(cid, "i", ver, mod=190), c,
                "citizen" if c == "United Kingdom" else "foreign", tier,
                f"c{cid}@{['mail.com', 'post.net', 'web.org'][h(cid, 'e', ver, mod=3)]}",
                CITIES[h(cid, "city", ver, mod=len(CITIES))], snap)

    hdr = ["CustomerID", "CustomerName", "Gender", "Age", "SpendingScore", "AnnualIncomeK", "Country",
           "CustomerType", "LoyaltyTier", "Email", "City", "SnapshotDate"]
    cutoff = dt.datetime(2011, 1, 1)
    snapA = [cust_row(c, "2010-12-31", 0) for c, d in sorted(first_seen.items()) if d < cutoff]
    snapB = []
    for c, d in sorted(first_seen.items()):
        v = 1 if (d < cutoff and h(c, "chg", mod=100) < 15) else 0  # ~15% of existing customers change
        snapB.append(cust_row(c, "2011-12-31", v))
    write("customers_snapshot_2010.csv", hdr, snapA)
    write("customers_snapshot_2011.csv", hdr, snapB)

    # ---- promotions, fx, calendar ------------------------------------------------------------------------------
    cats = sorted({p[2] for p in prods})
    promos = []
    for i, c in enumerate(cats):
        for q, (m1, m2) in enumerate([(2, 3), (6, 7), (11, 12)]):
            promos.append((f"P{i}{q}", c, dt.date(2011, m1, 1).isoformat(),
                           dt.date(2011, m2, 28).isoformat(), 5 * (1 + h(c, q, mod=5))))
    write("promotions.csv", ["PromoID", "Category", "StartDate", "EndDate", "DiscountPct"], promos)
    write("country_currency.csv", ["Country", "Currency"], sorted(CURRENCY.items()))
    base_fx = {"GBP": 1.0, "EUR": 1.17, "USD": 1.55, "CHF": 1.45, "NOK": 9.1, "SEK": 10.5, "DKK": 8.7,
               "AUD": 1.6, "JPY": 130.0, "CAD": 1.58}
    fx = [(f"{y}-{m:02d}", cur, round(r * (1 + (h(cur, y, m, mod=100) - 50) / 1500), 4))
          for (y, m) in [(2010, 12)] + [(2011, k) for k in range(1, 13)] for cur, r in base_fx.items()]
    write("fx_rates.csv", ["YearMonth", "Currency", "RateFromGBP"], fx)
    cal, d = [], dt.date(2010, 12, 1)
    while d <= dt.date(2011, 12, 31):
        cal.append((int(d.strftime("%Y%m%d")), d.isoformat(), d.year, (d.month - 1) // 3 + 1, d.month,
                    d.strftime("%B"), d.day, d.isoweekday(), 1 if d.isoweekday() >= 6 else 0,
                    1 if (d.month, d.day) in {(1, 1), (12, 25), (12, 26)} else 0))
        d += dt.timedelta(days=1)
    write("calendar.csv", ["DateKey", "FullDate", "Year", "Quarter", "Month", "MonthName", "Day",
                           "DayOfWeekISO", "IsWeekend", "IsHoliday"], cal)


if __name__ == "__main__":
    sys.exit(main())
