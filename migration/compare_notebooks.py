"""Compare Switch-generated notebooks with the compiled ones, link by link."""
import re, sys, json
from pathlib import Path

def norm(sql):
    s = sql.lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("(", " ").replace(")", " ")
    s = re.sub(r"\bcast\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def views(src):
    out = {}
    for m in re.finditer(r'^(\w+)\s*=\s*spark\.sql\(f?"""(.*?)"""\)', src, re.S | re.M):
        out[m.group(1)] = m.group(2)
    return out

def tables_written(src):
    return sorted(set(re.findall(r'saveAsTable\(f"\{catalog\}\.\{schema\}\.(\w+)"\)', src)))

def tables_read(src):
    return sorted(set(re.findall(r'spark\.table\(f"\{catalog\}\.\{schema\}\.(\w+)"\)', src)))

OTHER = Path(sys.argv[1] if len(sys.argv) > 1 else "switch_output_nb")
res = {}
tot = same = lost_order = csv_reads = csv_positional = 0
for f in sorted(Path("bundle/src/notebooks").glob("J*.py")):
    a, b = f.read_text(), (OTHER / f.name).read_text()
    csv_reads += len(re.findall(r'\.csv\(f"\{src_dir\}', b))
    csv_positional += len(re.findall(r'\.toDF\(', b))
    va, vb = views(a), views(b)
    row = {"views_mine": len(va), "views_switch": len(vb),
           "missing_in_switch": sorted(set(va) - set(vb)), "extra_in_switch": sorted(set(vb) - set(va)),
           "writes_equal": tables_written(a) == tables_written(b), "reads_equal": tables_read(a) == tables_read(b),
           "sql_differs": []}
    for k in va:
        if k in vb:
            tot += 1
            if "limit" in norm(va[k]) and "order by" in norm(va[k]) and "order by" not in norm(vb[k]):
                lost_order += 1
                row.setdefault("limit_lost_order_by", []).append(k)
            if norm(va[k]) == norm(vb[k]):
                same += 1
            else:
                row["sql_differs"].append(k)
    res[f.stem] = row
for j, r in res.items():
    flag = "OK " if not (r["missing_in_switch"] or r["extra_in_switch"] or r["sql_differs"]) and r["writes_equal"] and r["reads_equal"] else "DIFF"
    print(f'{flag} {j:38} views {r["views_mine"]:>2}/{r["views_switch"]:>2} writes_eq={r["writes_equal"]} reads_eq={r["reads_equal"]} '
          f'missing={len(r["missing_in_switch"])} extra={len(r["extra_in_switch"])} sql_diff={len(r["sql_differs"])}')
print(f"\ncompared against: {OTHER}")
print(f"views with equivalent normalized SQL: {same}/{tot}")
print(f"LIMIT views that lost the upstream ORDER BY: {lost_order}")
print(f"CSV source reads: {csv_reads}, renamed by position (.toDF): {csv_positional}")
json.dump(res, open(f"compare_{OTHER.name}.json", "w"), indent=1)
