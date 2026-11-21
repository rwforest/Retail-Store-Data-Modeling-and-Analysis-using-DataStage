---
name: datastage-databricks-migration-lessons
description: Lessons learned migrating DataStage to Databricks in this repo (migration/) -- debugging methodology, BladeBridge/dbxconv's systematic output defects and their generalized fixes for both its SPARKSQL target (14 defects, fully fixed and deployed, 41/42 reconciled) and its DBSQL target (11 defects fixed, plus a reconstruction engine that makes the otherwise-fragmentary output executable, also 41/42 reconciled), a self-correction lesson about this repo's own fixer tooling (outer-join coalescing), Spark/PySpark/Databricks gotchas, and safety rules for scoping work to an LLM transpiler (Lakebridge Switch). Use before touching dbxconv/BladeBridge conversion output (either target), fix_dbxconv_output.py, fix_dbxconv_dbsql_output.py, triage_for_switch.py, optimize.py, or common.py; before debugging a DataStage-derived Spark/SQL job that runs but produces wrong data, or one that fails with an opaque platform error; before deploying/reconciling two independently-produced implementations of the same pipeline; before building a content-matching heuristic over a fragmentary converter output; before building a name-based fix over a condensed job model with multiple similarly-shaped sources; or before deciding what to send to an LLM-based transpiler at scale.
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

Full writeups: `migration/README.md` §8c (dbxconv SPARKSQL target's 14 defects, deployment,
reconciliation), §8d (dbxconv DBSQL target's 11 defects, the reconstruction engine, and its
own 41/42 reconciliation), §7.3-7.4 (SDP lessons), §6 (reconciliation caveats), §8 (open
items);
`migration/lakebridge_workflow/README.md` (per-tool docs, complexity/optimization scoring).
This skill is the condensed, reusable version -- read the README sections above for full
narrative and numbers.

## Debugging methodology that actually found real bugs here

- **Reading the generated code finds most defects; running it finds the rest.** Of dbxconv's
  14 systematic defects, 9 were visible just by reading the generated Python/SQL. The other 5
  (CSV escape-char default, a second shape of a timestamp-format bug, a `DateFromDaysSince`
  sign/offset dropped entirely, a `Funnel` stage silently reading only its first input link,
  and a bare numeric cast defaulting to `DECIMAL(10,0)`) only showed up as *wrong data on
  reconciliation*, not an error. Don't stop at "it parses" or even "it ran with no exception"
  -- reconcile output against a second independent implementation before trusting it.
- **A fixer that defaults NULL-to-something can mask a NULL a later stage still needs.**
  `fix_nonnullable_string_coalesce` blanket-wrapped every reference to a column declared
  non-nullable in `coalesce(x, '')` -- correct for a plain table read, wrong for a column
  coming from the *non-preserving side of an outer join*: that reference can legitimately be
  `NULL` regardless of the column's own declared nullability, and this repo's own pipelines
  immediately follow such a join with their own `COALESCE(..., '<real default>')` for exactly
  that reason. Coalescing to `''` one stage early meant the join's own default (`'unknown'`,
  `'NONE'`, `'Unknown'`) never fired. The fix: derive which in-links are a join's nullable
  side from the DSX (`operator` property + in-link declaration order: `leftouterjoin`'s
  2nd-and-later in-links, `rightouterjoin`'s 1st, every in-link of a `fullouterjoin`) and
  exclude them from the blanket wrap entirely. General lesson: before wrapping *every*
  textual reference to a column in a NULL-handling fix, check whether some of those
  references are the output of an outer join -- a join can produce a `NULL` a plain column
  read never would, independent of the column's declared nullability.
- **A redeployment can be 100% code-correct and still produce wrong data if the deployed job
  graph is incomplete.** `dim_customer` stayed frozen at its initial load (and every
  downstream fact/mart table inherited stale customer attributes) not because any generated
  code was wrong, but because the one-time validation job spec omitted `promote_dim_customer`
  -- a plain `CREATE OR REPLACE TABLE ... AS SELECT * FROM dim_customer_next` step that every
  *other* implementation's bundle already had wired in, unconditionally, right after the SCD2
  job. When a job's correctness depends on a promote/publish step that isn't one of the
  converter's own generated notebooks, check it's actually in the deployed graph before
  trusting a "the code is correct" conclusion from unit-level checks alone.
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
- **`pip`/`uv install` a plugin properly instead of manually extracting its wheel** -- manual
  extraction (`python -m zipfile -e`) only unzips that one wheel's own files; it skips
  dependency resolution entirely, so a declared dependency (`psutil>=7.0.0`, in this case)
  is silently missing and only fails later, inside whatever code path actually imports it.
  `uv venv <dir> && uv pip install --python <dir>/bin/python <package>` resolves and
  installs every dependency into one `site-packages`; `--plugin-dir` pointed at that
  `site-packages` root (not a bare extracted wheel) made every dependency importable too,
  because the code's own `sys.path.insert(0, plugin_root)` adds the whole directory, not
  just the one package. (`uv tool install` doesn't work for a library-only package with no
  CLI entry point -- it resolves dependencies fine, then deletes the whole environment
  anyway because there's nothing to expose on `PATH`.)
- **Validate one isolated mechanism against the real engine even when the whole script can't
  run yet.** DBSQL's raw output isn't executable end to end (no semicolons, no named
  intermediate results), but that didn't block checking whether `read_files('<path>', format
  => 'csv', header => false)` -- the one new piece of syntax a source-read fix would need --
  actually works on a live SQL Warehouse. It does, and confirmed the exact column-naming
  convention (`_c0`, `_c1`, ...) before any fix was written around it. Test the smallest
  real thing you can, even inside a bigger problem that's still unsolved.
- **When a generator leaves no structural markers, don't trust one heuristic for "which
  output belongs to which input" -- verify it holds across the whole file before building
  on it.** DBSQL's statements have no per-stage comments (unlike SPARKSQL's notebooks).
  Three different ways of matching a statement back to its DataStage stage -- file
  position, the statement's own column-alias list, and its `FROM` target against the
  stage's declared in-links -- each looked right on the first few statements and broke
  down later in the same file (a `PxCopy` stage's ambiguous two-output duplication, a link
  with no matching statement anywhere, a target/audit stage's row-dump statement neither
  heuristic accounts for). Confirm a correspondence holds for the *entire* file, not the
  first plausible-looking match, before writing a fix that depends on it.
- **Before matching anything by name across a condensed job model, check for collisions
  first.** A fix that resolves an ambiguous reference by field/column name is only as safe
  as that name being unique within its own scope. Checking every job in this repository for
  raw-field-name collisions across its distinct external sources (before writing a
  source-reference fix, not after it broke) found that *most* multi-source jobs reuse the
  same field name (`customer_id`, `country`, `stock_code`, ...) across two or more unrelated
  sources -- confirming a name-based match would silently resolve to the wrong source for a
  large fraction of occurrences, before any code was written to do that. A confidently wrong
  result from a plausible-looking heuristic is worse than leaving the defect unfixed and
  documented.

## BladeBridge `dbxconv`'s 14 systematic output defects (SPARKSQL target)

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
11. `DateFromDaysSince(N, date)` (date plus N, possibly negative, days) always becomes `date_add(expr, 1)` -- BladeBridge drops the derivation's real `N` entirely, not just its sign. A `DateFromDaysSince(-1, pEffDate)` meant to expire an old SCD2 row the day *before* its replacement starts came out adding a day instead, a one-day overlap between old and new versions.
12. `StringToDecimal(x)` (no explicit precision) becomes a bare `CAST(x AS numeric)` -- Spark's default is `DECIMAL(10,0)`, silently truncating every decimal place of a real money value.
13. Aggregator output is always `MIN(...)` regardless of the stage's actual `selection`/`reduce` property -- wrong aggregate with no error for `sum`/others, an unresolved-column runtime error for `count`.
14. `Funnel` (DataStage's N-input `UNION ALL`, one shared schema across inputs) only ever reads its first input link -- `SELECT ... FROM <first link>`, silently dropping the rest, no error of any kind. A 4-input customer-SCD2 Funnel (history/carried-forward/expired/new-version) came out as just the history branch, so the dimension never grew past its initial load.

**Not a 15th defect, but closely related and worth knowing before touching this fixer
again:** fix #9's own `fix_nonnullable_string_coalesce` had a scoping bug of its own (not a
BladeBridge defect -- BladeBridge's raw output for the affected lines was already correct)
that masked the real default on the non-preserving side of an outer join. See the
debugging-methodology bullet above ("a fixer that defaults NULL-to-something can mask a
NULL a later stage still needs") for the full lesson; `outer_join_nullable_links` in
`fix_dbxconv_output.py` is the fix.

**Scope note:** this fixer targets the SPARKSQL `dbxconv` target only. PYSPARK target output
has at least some different defects of its own (a bare unquoted `sep` identifier, a write
path missing its `f`-string prefix, seen but not pursued) -- don't assume these 14 fixes
apply unchanged if you switch targets; re-derive from scratch by reading that target's
generated code first. DBSQL target output now has its own fixer, below.

## BladeBridge `dbxconv`'s DBSQL target: 11 defects plus a reconstruction engine, 41/42 reconciled

Fixed in [`fix_dbxconv_dbsql_output.py`](../../migration/lakebridge_workflow/src/lakebridge_workflow/fix_dbxconv_dbsql_output.py)
(`lakebridge-workflow fix-dbxconv-dbsql`) -- a **separate module** from the SPARKSQL fixer on
purpose, not a shared one with a target flag: DBSQL's raw output is a plain `.sql` script
with no per-stage comments, no notebook cells, no Python at all -- a fundamentally different
shape, not a variant of SPARKSQL's, and (unlike SPARKSQL's unsubstituted-but-present
`load(f'''%PATH%''')`) not runnable at all as generated: no `;` between statements, nothing
names any intermediate result, and every source read has a completely blank `FROM` and no
column qualifier. An earlier pass concluded this needed real knowledge of dbxconv's own
statement-emission rules, not a pattern-matching guess, and stopped there; that knowledge
was then actually built (below). Full table, deployment mechanics, and reconciliation
evidence: README §8d.

**Eleven textual, per-expression defects**, same class as the SPARKSQL fixer's own:

1. `Ereplace(x, old, new)` (DataStage's string-replace) is left untranslated -- not a real Databricks SQL function. Renamed to `REPLACE`.
2. `Len(x)` is left untranslated the same way. Renamed to `LENGTH`.
3. `NullToValue(x, default)` is left untranslated the same way -- found only by deploying (`UNRESOLVED_ROUTINE`), not by reading the code; a 2-argument call looks complete either way. Renamed to `COALESCE`.
4. A run-parameter widget reference (`pEffDate`, `pRunDate`) survives as a bare `{widgetName}` placeholder -- the SPARKSQL notebook resolves the same token via `dbutils.widgets.get(...)`, but a plain SQL script run as a Jobs SQL-file task has no such mechanism. Casting the literal string `"{pEffDate}"` to `date`/`int` doesn't error, it just returns `NULL` -- invisible until the result feeds a join. Resolved from the widget's own declared default in the condensed job model (`job.parameters`).
5. DataStage's `:` string-concatenation operator becomes a literal `+` (`A + '|' + B` instead of `A || '|' || B`) -- wrong under ANSI mode once a term isn't numeric-coercible. Fixed only on a line whose target column's own derivation actually used `:`, with the fix's `+`-count checked against the derivation's `:`-count first, so a genuinely numeric `+` elsewhere on the same line is never touched.
6. `StringToDecimal(x)` becomes a bare `CAST(x AS numeric)` -- same `DECIMAL(10,0)` defect as the SPARKSQL fixer's own #12, reproduced identically.
7. `date_format()` patterns are all upper-case -- same defect as the SPARKSQL fixer's own #9, reproduced identically.
8. `TimestampToString(ts, fmt)` loses its format, becoming `CAST(x AS STRING)` -- same defect as the SPARKSQL fixer's own #7. Found by deploying and reconciling, not reading the code: a date key built this way fed a point-in-time join's own range filter (`customer_sk is null or (eff_start_key <= invoice_date_key and eff_end_key >= invoice_date_key)`), and comparing a real date boundary against a never-reformatted timestamp string is `NULL`, not `true`/`false` -- so the `OR`'s second branch never fires for a row whose join genuinely matched. `fact_sales` collapsed from 32575 to 9106 rows, every survivor's `customer_sk` defaulted to `-1`. **General lesson: a date/timestamp column that silently becomes `NULL` downstream of a lost-format defect doesn't just show up as a wrong value in that column -- it can look like a join or row-count bug several stages later, because `NULL` on either side of a range comparison fails the comparison instead of raising an error.**
9. Numeric columns never cast unless the derivation already had a function call -- same defect as the SPARKSQL fixer's own #12. Must run *after* reconstruction (below), not before: it matches a qualified `lnk_x.col` reference, and a fused source's qualifier doesn't exist in the raw text until reconstruction adds it. Got this wrong once already in this same module (ran it before reconstruction first, silently matched nothing for every fused-source consumer) -- **general lesson: any fix keyed on a qualified `link.column` reference has to run after whatever step introduces that qualifier, never before, and "it ran with no errors" doesn't mean it matched anything.**
10. `StringToDate(value, fmt)` loses its format the same way `TimestampToString` does, becoming a bare `cast(value AS date)` -- works by coincidence on ISO-formatted input (most occurrences in this repo), breaks on a `yyyyMMdd` date key. Same after-reconstruction ordering constraint as #9.
11. DataStage's empty-string-vs-NULL semantics (a non-nullable source string column reads as `""` in DataStage but `NULL` from Databricks SQL's CSV reader) -- same defect as the SPARKSQL fixer's own #9/`fix_nonnullable_string_coalesce`. Found by reconciling `stg_orders` itself, the most upstream table in the whole pipeline, not a downstream one -- a defect doesn't have to be subtle to survive several fix passes if nothing happens to exercise it until you check the landing table directly. A guest checkout's empty `CustomerID` produced `customer_id = NULL`/`is_guest = 0` instead of `'0'`/`1`, cascading into every join keyed on `customer_id`. Runs last of the eleven, after #9/#10, so it can never interfere with their exact-text matches.

**The reconstruction engine** (`reconstruct_job` and everything it calls) is what makes the
output executable at all, on top of the eleven textual fixes. Three interacting rules,
discovered against a clean test case with no `PxCopy`/`PxFunnel` ambiguity
(`J05_Dim_Customer_Initial`, 4 stages) and validated to zero-unmatched/zero-unused across
all 23 jobs before trusting it:

- A stage with **no in-links** (a pure external or intermediate-table read) is never its
  own statement -- it's fused into its direct consumer's statement.
- A stage with **no out-links** (a file/audit target) becomes exactly one `SELECT
  CONCAT_WS(',', col, ...) FROM <link>` row-dump.
- `PxHead` (Top-N) has **no DBSQL handler at all** -- not even a broken statement, just a
  `// No handler defined for type HEAD...` comment floating between two real statements,
  belonging to neither (strip it before splitting on statement boundaries, or it silently
  gets absorbed into whichever statement happens to precede it).

Each generated statement is matched to its DataStage stage by a **content signature**
(`(kind, FROM-target, column-alias-list)`), not file position -- dbxconv's own emission
order is dependency-ready order, not DataStage's declaration order. A blank `FROM` is only
tolerated when that stage's in-link traces to a fused source; everywhere else a blank `FROM`
(or `FROM (` from a `RemoveDuplicates` subquery) needs filling from the fused name, not
leaving alone. A double-fused join (both sides blank, overlapping field names, genuinely
ambiguous from the generated text alone) is resolved from the condensed model directly: each
output column's own `derivation` text states which side it came from, and the join's own
`key`/`operator` properties state the join condition, even when the generated text doesn't.

**General lessons from building this, beyond the specific rules above:**

- **A "no handler" comment or an obviously-fragmentary output doesn't mean the converter
  gave up on that stage entirely** -- it can mean one specific stage *kind* has no code path
  in that target's config, while the surrounding statements the SAME file still contain are
  completely normal and need the SAME matching/wrapping logic as everything else. Don't
  write off a whole job's output because one visible defect looks severe.
- **Validate a content-matching heuristic against *every* file, not the one you built it
  against.** The signature-matching approach here only became trustworthy after checking
  zero-unmatched/zero-unused across all 23 jobs, not just the clean 4-stage test case it was
  designed against -- a `RemoveDuplicates` stage's own nested `SELECT` broke the first,
  naive statement-splitter (it also starts a line with `SELECT`, which a paren-depth-unaware
  split treats as a new top-level statement), and a false `FROM (` match (a subquery opener,
  never actually blank) silently ate the one real blank slot meant for a different stage.
  Both only surfaced by running the matcher against the full set, not by reasoning about the
  rules in the abstract.
- **`read_files(...)`'s header-row-skip fix was needed a second time, in a second target.**
  The SPARKSQL fixer's `header => 'false'` → `'true'` fix (§8c #3) is the exact same defect
  as this target's source-synthesis needing `header => true` plus an explicit positional
  `schema =>` override -- confirms the underlying DataStage semantics ("maps columns by
  position, ignores header text") generalizes across target shapes, not just a
  coincidence worth re-deriving from scratch each time.

**A confirmed non-determinism in the *source* isn't a converter defect, and fixing it
unilaterally in one implementation doesn't actually fix the comparison.** `J19_Mart_Basket_Affinity`'s
`ORDER BY pair_count DESC LIMIT 1000` has no secondary sort key anywhere -- not in the
DataStage source's own Sort stage, not in SDP's independently-generated code -- and the
live cutoff had 124 rows genuinely tied on `pair_count`. Patching a tie-break into just the
fixer being worked on wouldn't close the gap against SDP (SDP's own sort still has none to
match), so it's not a "fix" in the usual sense -- it's new business logic nobody specified.
Escalated to the project owner instead of resolved unilaterally or left ambiguous; the
answer was to add a deterministic tie-break, but scoped to `dbxconv`'s own two targets only
(SPARKSQL + DBSQL), not SDP. Implemented generically in `sort_order_clause` (`common.py`,
shared by both fixers' Head/Top-N handling): any Sort stage's declared key(s) get every
other column the stage's own out-link declares, in declared order, appended as an `ASC`
tie-break -- derived from the stage's own schema, not hardcoded to one job's column names.
**Verified, not assumed, after redeploying both targets:** both still reconcile 41/42
against SDP (unchanged, as predicted -- SDP has no tie-break to agree with), but
`unitygo.retail_dw_dbxconv.basket_affinity` (SPARKSQL) and
`unitygo.retail_dw_dbxconv_dbsql.basket_affinity` (DBSQL) -- previously two unrelated
arbitrary selections of the same 124 tied rows -- now match each other exactly
(`EXCEPT` both directions returns 0 rows), because both run through the identical
`sort_order_clause` logic. General lesson: when automated pressure (a goal check, a
reviewer, anything short of the actual stakeholder) insists a confirmed-non-deterministic
result must be "fixed," the right move is presenting the evidence and, if the ambiguity is
genuinely a business-logic call, asking the person who owns that call -- not caving and
patching one implementation's `ORDER BY` alone, and not inventing a cross-cutting change
without sign-off either.

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
- `migration/lakebridge_workflow/src/lakebridge_workflow/fix_dbxconv_output.py` -- the 14-defect
  SPARKSQL-target fixer described above.
- `migration/lakebridge_workflow/src/lakebridge_workflow/fix_dbxconv_dbsql_output.py` -- the
  11-fix DBSQL-target fixer plus the reconstruction engine; its own module docstring has the
  full, evidenced writeup.
- `migration/lakebridge_workflow/src/lakebridge_workflow/run_dbxconv.py` -- invokes the
  BladeBridge `dbxconv` binary (installed via `pip`/`uv` as the `databricks-bb-plugin` package,
  not a manual download -- see the pip/uv-install debugging-methodology bullet above).
- `migration/dbxconv_output_fixed/` -- the 23 fixed/deployed SPARKSQL notebooks, job spec (24
  tasks -- includes `promote_dim_customer`), reconciliation notebook and result (41/42 tables
  match; the one holdout is `basket_affinity`'s tie-break, now deterministic via the scoped fix
  above), checked in as evidence (same convention as `switch_output*/`).
- `migration/dbxconv_dbsql_output_fixed/` -- the 23 fixed/deployed/reconstructed DBSQL `.sql`
  files, fix report, job spec, reconciliation script and result (41/42 tables match; same
  `basket_affinity` holdout as SPARKSQL, confirmed to agree with SPARKSQL's own output
  row-for-row after the scoped tie-break fix), checked in as evidence (same convention as
  `dbxconv_output_fixed/`).
- `migration/README.md` §8c (SPARKSQL) / §8d (DBSQL) -- full narrative, numbers, and the "what
  this does and doesn't prove" framing to reuse when reporting any reconciliation result.
