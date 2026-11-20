---
name: datastage-databricks-migration-lessons
description: Lessons learned migrating DataStage to Databricks in this repo (migration/) -- debugging methodology, BladeBridge/dbxconv's 12 systematic output defects and their generalized fixes, Spark/PySpark/Databricks gotchas, and safety rules for scoping work to an LLM transpiler (Lakebridge Switch). Use before touching dbxconv/BladeBridge conversion output, fix_dbxconv_output.py, triage_for_switch.py, optimize.py, or common.py; before debugging a DataStage-derived Spark job that runs but produces wrong data, or one that fails with an opaque platform error; before deploying/reconciling two independently-produced implementations of the same pipeline; or before deciding what to send to an LLM-based transpiler at scale.
---

# DataStage -> Databricks migration: lessons learned

Context for this repo: `migration/` holds three independently-built DataStage -> Databricks
implementations (`compiled`, `Switch`/LLM, `SDP`) plus a fourth, `dbxconv` (BladeBridge's
offline rule-based converter, lift-and-shift only). All of this package's tooling
(`optimize.py`, `triage_for_switch.py`, `fix_dbxconv_output.py`) is built on the condensed-job
model in `lakebridge_workflow/src/lakebridge_workflow/common.py`
(`CondensedJob`/`Stage`/`Link`, `read_condensed_job`/`iter_condensed_jobs`). Read that file
first -- every fixer/analyzer in this package parses jobs the same way; don't invent a second
parser.

Full writeups: `migration/README.md` §8c (dbxconv's 12 defects, deployment, reconciliation),
§7.3-7.4 (SDP lessons), §6 (reconciliation caveats), §8 (open items);
`migration/lakebridge_workflow/README.md` (per-tool docs, complexity/optimization scoring).
This skill is the condensed, reusable version -- read the README sections above for full
narrative and numbers.

## Debugging methodology that actually found real bugs here

- **Reading the generated code finds most defects; running it finds the rest.** Of dbxconv's
  12 systematic defects, 9 were visible just by reading the generated Python/SQL. The other 3
  (CSV escape-char default, a second shape of a timestamp-format bug, a bare numeric cast
  defaulting to `DECIMAL(10,0)`) only showed up as *wrong data on reconciliation*, not an
  error. Don't stop at "it parses" or even "it ran with no exception" -- reconcile output
  against a second independent implementation before trusting it.
- **Isolate before trusting a concurrency theory.** An opaque, 100%-reproducible
  `INTERNAL_ERROR` on 3 of 23 tasks looked like resource contention (all 3 started within
  ~300ms of each other). It wasn't -- resubmitting one of them completely alone reproduced the
  exact same failure. The real cause was a stage reading a file an earlier job's fix had
  turned into a Delta table write, so the read path no longer existed. Isolate first; don't
  patch around a contention theory you haven't actually ruled out.
- **`print()` is invisible in a submitted job's output.** To get diagnostic state back from a
  notebook run via `databricks jobs get-run-output`, use
  `dbutils.notebook.exit(json.dumps(result, default=str))`. This is the only reliable channel;
  build small one-off diagnostic notebooks this way when chasing a wrong-value bug (e.g. dump
  both implementations' rows for one known key side by side).
- **`databricks jobs repair-run --rerun-all-failed-tasks`** confirms reproducibility before you
  spend time root-causing -- rule out transience first.
- **Fix the generator, not the output.** When a converter/transpiler systematically
  mis-translates one construct, fix the generalized translation (reading the same condensed
  job model) so it corrects every job that hits the pattern, not just the one pipeline in
  front of you. Every fix in `fix_dbxconv_output.py` works this way: it re-derives the correct
  value from the DataStage stage/link properties rather than patching generated text by hand.
- **Reconciliation without a source to compare against is still real evidence, with caveats.**
  Two *independently derived* implementations (no shared ancestor, e.g. dbxconv's literal
  conversion vs. SDP's LLM-driven rewrite) agreeing is stronger evidence than two related ones
  agreeing, but it's still evidence, not proof -- always state what it does and doesn't prove
  (see README §6 and §8c's closing paragraphs for the exact wording pattern to reuse).
- **A validated venv beats `uv run --with <wheel>`.** `uv run --with` repeatedly served stale
  cached results in this repo's debugging. When validating a rebuilt wheel, use
  `rm -rf <venv> && python3 -m venv <venv> && <venv>/bin/python -m pip install <wheel>`, not
  `uv run --with`.
- **Untrusted downloads (plugin wheels, vendor tools) go in their own empty directory,
  separate from your own scripts**, per the sandbox rule -- e.g. the BladeBridge plugin wheel
  was extracted into an isolated `/tmp/bb_work/` directory, never alongside this package's
  source.

## BladeBridge `dbxconv`'s 12 systematic output defects (SPARKSQL target)

All fixed generically in
[`fix_dbxconv_output.py`](../../migration/lakebridge_workflow/src/lakebridge_workflow/fix_dbxconv_output.py)
(`lakebridge-workflow fix-dbxconv`), by re-parsing the same condensed job model, not by
hand-patching generated text. Full table with "found by" column: README §8c.

1. A target reading an Aggregator's output emits a blank `lnk_x..alias('lnk_x')` -- syntax error.
2. Every source read is left as literal `load(f'''%PATH%''')` -- parameter substitution never fires on the read side (only the write side).
3. Every source read hardcodes `option('header', 'false')` even when the source has a real header row.
4. No `escape` option is set on CSV reads -- Spark's CSV reader defaults to backslash escaping, not RFC 4180's doubled-quote (`""`) convention, so an escaped `"` inside a value comes out with the escape characters still in it.
5. A stage reading what an *earlier job in the pipeline* wrote is left pointing at a CSV path that a different fix (write-side) already turned into a Delta table -- fails with an opaque, non-retriable platform error, not a Python exception.
6. `PxHead` (Top-N) has no handler at all -- a literal, invalid-Python `// No handler defined...` comment.
7. `TimestampToString(ts, fmt)` loses its format, becoming `CAST(x AS STRING)` (Spark's default representation, not DataStage's).
8. The same function has a second generated shape (`CAST(bare_timestamp_col AS STRING)`, no `to_timestamp(` wrapper) that the first fix for #7 didn't match -- caused a silently-broken FX-rate join key downstream.
9. `DateToString(date, fmt)` becomes `date_format(...)` correctly, but the pattern is emitted all upper-case (`'YYYYMMDD'`) -- Spark 3.0+ rejects mixing capital `Y` (week-based-year) with a bare month.
10. `StringToDate(value, fmt)` loses its format the same way `TimestampToString` does, becoming a bare `CAST(value AS date)` -- works by luck on ISO-formatted input, breaks on `yyyyMMdd`.
11. `StringToDecimal(x)` (no explicit precision) becomes a bare `CAST(x AS numeric)` -- Spark's default is `DECIMAL(10,0)`, silently truncating every decimal place of a real money value.
12. Aggregator output is always `MIN(...)` regardless of the stage's actual `selection`/`reduce` property -- wrong aggregate with no error for `sum`/others, an unresolved-column runtime error for `count`.

**Scope note:** this fixer targets the SPARKSQL `dbxconv` target only. PYSPARK/DBSQL targets
have at least some different defects (a bare unquoted `sep` identifier, a write path missing
its `f`-string prefix, seen but not pursued) -- don't assume the same 12 fixes apply unchanged
if you switch targets; re-derive from scratch by reading that target's generated code first.

## Spark / PySpark / Databricks gotchas (general, not dbxconv-specific)

- **`coalesce(x, '')` means different things in different contexts.** In the PySpark
  DataFrame API, a bare `''` second argument resolves as a *column name* (fails with
  `UNRESOLVED_COLUMN`), not a string literal -- use `lit('')`. Inside `spark.sql()` text, a
  bare `''` is a correct SQL string literal. When generating code programmatically, decide
  per-occurrence which syntax context you're in (e.g. a lookahead for an immediately-following
  `.alias(` distinguishes DataFrame-API chains from SQL text) -- don't assume one global rule.
- **Two sequential regex `.subn()` passes on the same text can double-apply.** If pass 2's
  "don't touch this" condition can itself be satisfied by pass 1's *output*, you get
  `coalesce(coalesce(x, ''), lit(''))`. Fix by combining into one pattern + one callback that
  decides the right replacement per-occurrence from the *original* text, not two independent
  passes.
- **`DECIMAL(10,0)` is Spark's default for a bare numeric cast with no precision/scale.** Any
  translation of an untyped "to numeric" source function must carry over the real
  precision/scale from the schema, or money values silently lose their decimal places.
- **CSV reader defaults:** `header` option is about whether a header row *is physically
  present* and should be skipped, not whether downstream code "maps columns by position" --
  those are unrelated facts, don't conflate them. `escape` defaults to backslash, not `"` (RFC
  4180 doubled-quote convention) -- an RFC-4180-escaped source needs `.option('escape', '"')`
  set explicitly.
- **Spark 3.0+ datetime pattern parsing is strict and case-sensitive.** Capital `Y` is
  week-based-year, not calendar year (`y`); mixing them raises
  `INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION`. Don't just upper-case a
  format string when translating from another system's date-format tokens.
- **`\b` word boundaries don't work on `Px`-prefixed / underscore-joined identifiers** (common
  in DataStage-derived naming) -- match on the actual delimiter characters instead.
- **Regex "first argument of a nested call" is not `[^,]+`** once arguments can themselves
  contain commas (nested function calls, e.g. `date_add(cast('{x}' as date),1)` as one
  argument). Greedy `.+` backtracking fixes some cases; a real balanced-paren pattern (matching
  explicit nesting depth) is needed once there's trailing content after the true closing paren
  that greedy backtracking alone will mis-bound.
- **Iterative re-testing against the same Delta tables needs `overwriteSchema`.** Stale schemas
  from earlier test runs cause `DELTA_FAILED_TO_MERGE_FIELDS` later -- add
  `.option('overwriteSchema', 'true')` to writes used in an iterative validation loop, as a
  general property of that workflow, not a one-off workaround.
- **Workspace FILES vs notebooks.** A plain `.py` module imported by a notebook (e.g.
  `databricks_conversion_supplements.py`) must be uploaded as a workspace *file*, not a
  notebook, or imports fail with `ModuleNotFoundError`.
- **DSX/condensed-job properties can be link properties, not stage properties** (e.g. `file` on
  a Sequential File stage lives on the link: `dict(lk.properties).get("file")`, not
  `stage.prop_map.get("file")`). Verify which object actually owns a property with a small
  targeted debug script rather than assuming based on where it conceptually "belongs."
- **DataStage's BASIC/UniVerse multi-value wrapper is inconsistent in the wild.** The unwrap
  regex for `\(2)\(2)0\(1)\(3)...` property wrappers needed an optional-space tolerance
  (`\w+ ?\\\(2\)`) after finding one link whose wrapper had a stray space where an
  otherwise-identical one didn't. Don't assume byte-for-byte consistency across DSX exports;
  verify against the actual source, and keep the tolerance narrow (don't over-generalize into
  matching things that aren't actually this wrapper).

## Safety rule: don't bundle-break jobs when scoping work for an LLM transpiler

`triage_for_switch.py` exists to scope an expensive LLM pass (Lakebridge Switch) down to only
the jobs/queries worth sending, for a source with hundreds of millions of queries where sending
everything is both unaffordable and unnecessary. The real bug found and fixed here: an SCD2 job
is **not safe to send to Switch in isolation** -- Switch transpiling a Type-2 slowly-changing
dimension stage on its own reproduces a documented failure mode (README §7.2: wrong row counts,
948 expected vs. 94 produced) because the correct behavior depends on how it's bundled with the
jobs around it. **Never let a triage/scoping tool present an SCD2 (or any inherently
multi-stage-dependent) candidate as "ready to send alone."** `triage_for_switch.py`'s fix:
split output into `ready/` vs `needs_manual_bundling/` subdirectories with a
`READ_ME_FIRST.txt` explanation and a `readyForSwitch` manifest field, so a human (or a
downstream script) can't accidentally feed an unsafe candidate through the cheap path. Apply
the same caution to *any* scoping/triage logic over a condensed job model: check whether a
candidate's correctness depends on sibling jobs before marking it independently actionable.

## Where things live

- `migration/lakebridge_workflow/src/lakebridge_workflow/common.py` -- the shared condensed-job
  parser (`CondensedJob`/`Stage`/`Link`, `unwrap_dsx_value`). Start here for any new tool.
- `migration/lakebridge_workflow/src/lakebridge_workflow/optimize.py` -- lift-and-shift-vs-SDP
  optimization scoring (truncate/reload, SCD2, duplicate queries, missing filters, and more).
- `migration/lakebridge_workflow/src/lakebridge_workflow/triage_for_switch.py` -- scopes the
  expensive LLM pass; enforces the SCD2 bundling-safety rule above.
- `migration/lakebridge_workflow/src/lakebridge_workflow/fix_dbxconv_output.py` -- the 12-defect
  fixer described above.
- `migration/lakebridge_workflow/src/lakebridge_workflow/run_dbxconv.py` -- invokes the
  BladeBridge `dbxconv` binary (installed via `pip`/`uv` as the `databricks-bb-plugin` package,
  not a manual download).
- `migration/dbxconv_output_fixed/` -- the 23 fixed/deployed notebooks, job spec, reconciliation
  notebook and result, checked in as evidence (same convention as `switch_output*/`).
- `migration/README.md` §8c -- full narrative, numbers, and the "what this does and doesn't
  prove" framing to reuse when reporting any reconciliation result.
