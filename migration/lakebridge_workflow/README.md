# Lakebridge workflow package

`lakebridge_workflow/` contains the end-to-end Lakebridge Analyzer, lineage, planning, and
migration workflow as an installable Python package. It is adapted to the jobs that are
actually present in this repository and remains ready for a future large DataStage export.

The large `EFBI_PRD*.xml`/`.txt` export is **not** checked in. The current source of truth is:

- 23 condensed parallel-job specifications in `../switch_input/*.txt`
- 1 condensed master-sequence specification in `../switch_input/Seq_Retail_DW_Master.txt`
- authoritative analyzer metrics in `../analysis/lakebridge-analyzer.xlsx`
- original per-job DSX exports under `../../DataStage Jobs/Pipeline/`

## Package layout

```text
lakebridge_workflow/
├── README.md
├── pyproject.toml
├── docs/lakebridge_analyzer_windows_guide-v6.md
├── scripts/run_dbxconv.ps1
├── src/lakebridge_workflow/
│   ├── __main__.py              # lakebridge-workflow CLI dispatcher
│   ├── workflow.py              # one-command end-to-end workflow
│   ├── common.py                # condensed-job parser and XLSX utilities
│   ├── paths.py                 # checkout discovery for installed wheel
│   ├── split_datastage_xml.py
│   ├── analyze.py
│   ├── analyze_parallel.py
│   ├── merge_reports.py
│   ├── build_hierarchy.py
│   ├── plan_migration_batch.py
│   ├── transpile_batch.py
│   └── run_dbxconv.py
└── dist/lakebridge_workflow-0.1.0-py3-none-any.whl
```

## Build the wheel

```powershell
cd migration/lakebridge_workflow
uv build --wheel --out-dir dist
```

The checked-in artifact is:

```text
dist/lakebridge_workflow-0.1.0-py3-none-any.whl
```

## Install and run

From either the repository root or `migration/`, use the local wheel with `uv`:

```powershell
cd migration
uv run `
  --with ./lakebridge_workflow/dist/lakebridge_workflow-0.1.0-py3-none-any.whl `
  lakebridge-workflow --version
```

Or install it into an existing Python environment:

```powershell
python -m pip install ./lakebridge_workflow/dist/lakebridge_workflow-0.1.0-py3-none-any.whl
lakebridge-workflow --version
```

The package locates the migration checkout from the current directory or from:

```powershell
$env:LAKEBRIDGE_MIGRATION_ROOT = "C:\path\to\Retail-Store-Data-Modeling-and-Analysis-using-DataStage\migration"
```

## One-command end-to-end workflow

This command runs all four phases:

1. Offline assessment of the 24 available jobs
2. Sequence-to-ETL hierarchy construction
3. Closed-lineage batch planning
4. Deterministic batch transpilation

```powershell
cd migration
uv run `
  --with ./lakebridge_workflow/dist/lakebridge_workflow-0.1.0-py3-none-any.whl `
  lakebridge-workflow workflow `
  --num-jobs 12 `
  --reset-history `
  --overwrite
```

Preview assessment, hierarchy, and the batch plan without changing migration state:

```powershell
lakebridge-workflow workflow --plan-only
```

The retail project has one master root (`Seq_Retail_DW_Master`) and 23 downstream ETL jobs.
Because the planner never cuts a root tree in half, a `--num-jobs 12` request still selects
the complete 24-item closure. Batch 001 therefore contains:

- 23 compiled PySpark/Spark SQL notebooks
- 1 Lakeflow Workflow resource (`workflow_batch_001.yml`)
- 24 condensed Switch input specifications
- 24 standalone DataStage XML files converted from the available DSX exports
- 4 copied support notebooks (`_setup`, `_check_dim_customer`, `_promote_dim_customer`,
  `_dqx_validate`)
- a self-contained Databricks Asset Bundle
- `manifest.json`, `batch_summary.csv`, and `migration_state.json`

## Individual commands

After installing the wheel, the same tools are available through one CLI:

```powershell
lakebridge-workflow analyze --list-tech
lakebridge-workflow analyze --source-directory ../switch_input --report-file ../consolidated_all_jobs.xlsx --source-tech Datastage --generate-json
lakebridge-workflow build-hierarchy --input-json ../consolidated_all_jobs.json --out-prefix ../job_hierarchy
lakebridge-workflow plan --num-jobs 12 --dry-run
lakebridge-workflow transpile --num-jobs 12 --reset-history --overwrite
lakebridge-workflow plan --status
```

Equivalent module form is also supported:

```powershell
python -m lakebridge_workflow analyze --list-tech
python -m lakebridge_workflow workflow --plan-only
```

## Generated assessment

The consolidated fallback report uses the checked-in official analyzer workbook for node
counts, complexity, checksums, and transformation counts. It reports:

- **24 unique items**: 23 parallel jobs + 1 sequence
- **946 total nodes**
- Complexity: **5 VERY_HIGH**, **17 HIGH**, **2 MEDIUM**
- **23 sequence-to-job links**
- **23 root-to-leaf paths**, all at depth 1

## Large-export workflow

When the original export is available, split it first:

```powershell
lakebridge-workflow split EFBI_PRD_for_Sequence_Job_Only.txt --profile-only

lakebridge-workflow split EFBI_PRD_for_Sequence_Job_Only.txt `
  --out-dir seq_only_chunks --jobs-per-file 500 --only-job-type 2

lakebridge-workflow split EFBI_PRD_for_Sequence_Job_Only.txt `
  --out-dir etl_only_chunks --jobs-per-file 500 --exclude-job-type 2 --skip-table-defs
```

Analyze chunks with isolated workers:

```powershell
lakebridge-workflow analyze-parallel `
  --chunks-dir seq_only_chunks `
  --out-dir seq_reports `
  --merge-prefix consolidated_sequence_jobs `
  --workers 6 `
  --generate-json
```

`analyze` automatically uses the official `databricks-bb-analyzer` console script when it is
on `PATH`. For a nonstandard launcher, pass its command through `--external-command`:

```powershell
lakebridge-workflow analyze-parallel `
  --chunks-dir etl_only_chunks `
  --out-dir etl_reports `
  --merge-prefix consolidated_etl_jobs `
  --workers 6 `
  --generate-json `
  --external-command '"C:\path\to\analyzer.exe" --source-directory {source} --report-file {report} --source-tech Datastage --generate-json'
```

Combine report directories without rerunning workers:

```powershell
lakebridge-workflow merge-reports `
  --reports-dir seq_reports `
  --reports-dir etl_reports `
  --out-prefix consolidated_all_jobs
```

The official technology name remains case-sensitive: `Datastage` (capital `D`, lowercase `s`).

## Offline BladeBridge conversion

Unpack the BladeBridge plugin wheel into `migration/bb_plugin/`, then run:

```powershell
lakebridge-workflow run-dbxconv `
  bb_plugin `
  migrated_batches/batch_001/xml `
  migrated_batches/batch_001/bladebridge_pyspark `
  PYSPARK
```

On Windows, use the wrapper:

```powershell
.\scripts\run_dbxconv.ps1 `
  -XmlPath ..\migrated_batches\batch_001\xml `
  -OutputDir ..\migrated_batches\batch_001\bladebridge_pyspark `
  -Target PYSPARK `
  -PluginDir ..\bb_plugin
```

Supported targets are `PYSPARK`, `SPARKSQL`, and `DBSQL`.

## Validation summary

The checked-in wheel was built with Python 3.13 and validated with Python 3.14:

```text
lakebridge-workflow 0.1.0
Supported source technologies:
  Datastage
```

The packaged end-to-end workflow was exercised against this checkout and produced:

```text
assessment=24 items/946 nodes
hierarchy=1 root/23 active ETL
batch=23 notebooks, 24 specs, 24 XML, 28 workflow tasks, 24 state items
```
