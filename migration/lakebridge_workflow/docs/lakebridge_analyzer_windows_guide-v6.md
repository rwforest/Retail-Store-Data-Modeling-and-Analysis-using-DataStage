# Running Lakebridge Analyzer on Large DataStage XML Exports (Windows Guide)

## 1. Overview & Findings on This Dataset

When inspecting the files in the project root directory:
- `EFBI_PRD_for_Sequence_Job_Only.txt` (`1,200.9 MB`) is a full DataStage XML export (`<DSExport>`) with a `.txt` extension—equivalent in size to `EFBI_PRD_pre_eot.xml` (`666.2 MB`) + `EFBI_PRD_post_eot.xml` (`535.5 MB`).
- Profiling `EFBI_PRD_for_Sequence_Job_Only.txt` with `split_datastage_xml.py --profile-only` (finished in ~115 seconds) showed:

| Top-Level XML Tag | Count | Approx. Size (MB) | Breakdown / Notes |
| :--- | :--- | :--- | :--- |
| `<Job>` (Total) | **5,664** | **1,158.75 MB** | All jobs in the `EFBI_PRD` export |
| ├─ **Sequence Jobs (`JobType="2"`)** | **2,717** | **~282.7 MB** | Orchestration / workflow sequences |
| └─ **Parallel / Server ETL Jobs (`JobType!="2"`)** | **2,947** | **~876.0 MB** | Data movement & transformation jobs |
| `<ParameterSets>` | 1 | 0.38 MB | Project parameter sets |
| `<Routines>` | 1 | 0.23 MB | Custom routines (`35` routines) |
| `<Header>` | 1 | 0.00 MB | DataStage 11.7 export header (`EFBI_PRD`) |

### Why the Original Analyzer Run Was Taking Overnight
1. **Monolithic XML DOM Parsing:** Passing a 500MB–1.2GB XML file with thousands of `<Job>` nodes into `databricks-bb-analyzer` forces its underlying single-threaded Perl binary (`analyzer.exe`) to construct and traverse a massive AST in memory.
2. **Scanning the Entire Folder (`.`):** Running `--source-directory .` caused the analyzer to pick up **both** `EFBI_PRD_pre_eot.xml` (`666 MB`) and `EFBI_PRD_post_eot.xml` (`535 MB`) at the same time.
3. **File Extension `.txt`:** `databricks-bb-analyzer` only scans `.xml` files by default (`-E .xml`)—it ignores `.txt` files unless split/renamed to `.xml`.
4. **Verbose `--debug` (`-v`) Overhead:** Passing `--debug` causes `analyzer.exe` to dump millions of lines of Perl `Data::Dumper` (`$VAR1 = { ... }`) AST structures to stdout, slowing execution by **5x–10x**.
5. **Case-Sensitive `--source-tech`:** The exact technology name required by `databricks-bb-analyzer` is `"Datastage"` (capital `D`, lowercase `s`).

---

## 2. Why We Split in Two Ways: (1) By Job Type and (2) Into 500-Job Chunks

To make the analyzer both **fast (minutes instead of days)** and **actionable for migration**, we split the monolithic `1.2 GB` export along two dimensions:

### Split #1: Separating Sequence Jobs (`JobType="2"`) from Parallel/Server ETL Jobs (`JobType!="2"`)
In IBM DataStage, Sequence Jobs and Parallel/Server Jobs represent two completely different architectural layers that migrate to **two different targets in Databricks**:

| Dimension | 1. Sequence Jobs (`JobType="2"`) | 2. Parallel & Server ETL Jobs (`JobType!="2"`) |
| :--- | :--- | :--- |
| **Role in DataStage** | **Orchestration / Control Flow:** Triggers jobs in order (`CJobActivity`), runs shell scripts (`CExecCommandActivity`), checks conditions, handles loops and failure emails (`CExceptionHandler`). | **Data Movement & Transformation:** Reads from Oracle/DB2/files (`OracleConnectorPX`, `PxDataSet`), transforms/joins/aggregates data (`CTransformerStage`, `PxJoin`, `PxLookup`), and loads targets. |
| **Databricks Target** | **Databricks Workflows (Lakeflow Jobs):** Multi-task DAGs, task dependencies, `if/else` conditions, and job parameters. | **PySpark / Spark SQL Notebooks or DLT Pipelines:** Spark DataFrame / SQL transformations and Delta table writes. |
| **Why Analyze Separately?** | **1. Active vs. Dead Code Discovery:** Sequence Jobs contain the parent $\rightarrow$ child call graph (`subJobInfo`). Analyzing them first tells you which Parallel ETL jobs are actually scheduled in production vs. dead/orphan jobs.<br>**2. Clean Complexity Metrics:** Sequence jobs have no SQL queries or column transformations; mixing them into ETL job reports skews average stage complexity and node counts. | **1. Embedded SQL Extraction:** Parallel jobs contain embedded SQL (`OracleConnectorPX`, `DB2ConnectorPX`) which triggers a second pass (`-t SQL`) inside `analyzer.exe`.<br>**2. Accurate Effort Sizing:** Lets you size ETL notebook conversion effort independently from workflow orchestration effort. |

### Split #2: Breaking Each Group into 500-Job XML Chunks (`chunk_0001.xml` .. `chunk_0006.xml`)
Even after separating Sequence Jobs (`282 MB`) and Parallel Jobs (`876 MB`), feeding a single 876 MB XML file to `databricks-bb-analyzer` is still a bottleneck because:
1. **Non-Linear DOM Memory Scaling:** `analyzer.exe` (a compiled 32-bit/64-bit Perl2Exe binary) parses each `.xml` file into an in-memory tree. A single 1.2 GB file causes massive memory allocation and garbage-collection stalls, whereas 500 jobs per file (~50–150 MB) parses cleanly in a few minutes.
2. **Single-Threaded Engine (`THREAD 1`):** By default, `analyzer.exe` assigns all input files in a folder to a single worker thread (`Started thread # 1`). Splitting into 6 chunks (`chunk_0001.xml` to `chunk_0006.xml`) allows `analyze_parallel.py` to launch **6 separate `analyzer.exe` processes simultaneously across 6 CPU cores**, cutting wall-clock time from hours down to ~8–12 minutes!

---

## 3. Helper Scripts Created in This Folder

### A. [split_datastage_xml.py](split_datastage_xml.py) (Streaming Profiler & Splitter)
Streams multi-gigabyte DataStage `.xml` or `.txt` exports in ~60–115 seconds using minimal RAM, and splits them into smaller valid `<DSExport>` `.xml` chunk files.

#### 1. Profile a large XML/TXT file (counts tags and JobTypes)
```cmd
uv run split_datastage_xml.py EFBI_PRD_for_Sequence_Job_Only.txt --profile-only
```

#### 2. Extract ONLY Sequence Jobs (`JobType="2"`) into smaller XML chunks
```cmd
uv run split_datastage_xml.py EFBI_PRD_for_Sequence_Job_Only.txt --out-dir ./seq_only_chunks --jobs-per-file 500 --only-job-type 2
```
* **JobType codes in DataStage XML:**
  * `2` = **Sequence Job** (Orchestration / Workflow)
  * `3` = **Parallel Job** (PX ETL Job)
  * `0` = **Server Job**

#### 3. Extract All Non-Sequence (Parallel & Server ETL) Jobs (`--exclude-job-type 2`)
```cmd
uv run split_datastage_xml.py EFBI_PRD_for_Sequence_Job_Only.txt --out-dir ./etl_only_chunks --jobs-per-file 500 --exclude-job-type 2 --skip-table-defs
```

---

### B. [analyze_parallel.py](analyze_parallel.py) (Multi-Core Parallel Runner — Fastest!)
Because `analyzer.exe` is single-threaded (`Started thread # 1`), `analyze_parallel.py` runs each XML chunk in its own isolated worker process and isolated temporary directory across multiple CPU cores simultaneously with `debug=False`.
- **Why isolated temp directories matter:** `analyzer.exe` writes `*.tmp` files into the output report folder and globs `*<timestamp>.tmp` during its consolidation phase. Isolating both input and output directories per worker prevents parallel workers from colliding on `*.tmp` files.
- **Benchmark:** Analyzed all **2,717 Sequence Jobs + 35 Routines (6 chunks, 282 MB)** in **501 seconds (~8.3 minutes)**!

#### 1. Run parallel analysis on Sequence Job chunks (auto-merges at the end!)
```cmd
uv run analyze_parallel.py --chunks-dir ./seq_only_chunks --out-dir ./seq_reports --merge-prefix ./consolidated_sequence_jobs --workers 6 --generate-json
```

#### 2. Run parallel analysis on Parallel/Server ETL Job chunks (auto-merges at the end!)
```cmd
uv run analyze_parallel.py --chunks-dir ./etl_only_chunks --out-dir ./etl_reports --merge-prefix ./consolidated_etl_jobs --workers 6 --generate-json
```

---

### C. [merge_reports.py](merge_reports.py) (Consolidate Chunk Reports into Single JSON & Excel)
Merges all `report_chunk_*.json` and `report_chunk_*.xlsx` files from one or more directories into a single deduplicated `.json` report and multi-tab `.xlsx` workbook (including `Embedded SQL Summary` and `Embedded SQL Programs`), and prints a summary to the console. Note: `analyze_parallel.py` now calls this automatically at the end of a run, or you can run it standalone to combine multiple directories:

#### 1. Merge Sequence Job + ETL Job chunk reports into a single Master Project Report (`consolidated_all_jobs`)
```cmd
uv run merge_reports.py --reports-dir ./seq_reports --reports-dir ./etl_reports --out-prefix ./consolidated_all_jobs
```

#### 2. Merge Sequence or ETL directories individually
```cmd
uv run merge_reports.py --reports-dir ./seq_reports --out-prefix ./consolidated_sequence_jobs
uv run merge_reports.py --reports-dir ./etl_reports --out-prefix ./consolidated_etl_jobs
```

---

### D. [build_hierarchy.py](build_hierarchy.py) (Build End-to-End Sequence $\rightarrow$ Sub-Sequence $\rightarrow$ ETL Job Hierarchy)
Joins the Sequence Job call graph (`subJobInfo`) with the Parallel/Server ETL Job inventory (`inventory`) from `consolidated_all_jobs.json` to build a multi-tier execution hierarchy (`Root Master Sequence -> Mid-Tier Sub-Sequence -> Leaf Parallel/Server ETL Job`).

```cmd
uv run build_hierarchy.py --input-json ./consolidated_all_jobs.json --out-prefix ./job_hierarchy
```
Generates [job_hierarchy.xlsx](job_hierarchy.xlsx) (`0.85 MB`) and [job_hierarchy.json](job_hierarchy.json) (`2.91 MB`) with 5 tabs:
1. **`Hierarchy Summary`**: Role breakdown & call-depth distribution (`Depth 1`, `Depth 2`, `Depth 3`).
2. **`Root Master Sequences`**: All **1,539 top-level Sequence Jobs** with counts of their downstream Sub-Sequences and Leaf ETL Jobs.
3. **`End-to-End Lineage Paths`**: All **8,913 root-to-leaf execution paths** (`Level 0 Root Sequence -> Level 1 -> Level 2 -> Level 3 -> Leaf ETL Job`) with leaf job type, complexity, node count, and folder.
4. **`All Jobs Classified`**: Every one of the **5,699 items** classified into its exact hierarchy role (`1_ROOT_SEQUENCE`, `2_SUB_SEQUENCE`, `3_LEAF_SEQUENCE`, `4_ACTIVE_ETL_JOB`, `5_ORPHAN_SEQUENCE`, `6_ORPHAN_ETL_JOB`, `7_ROUTINE`, `8_MISSING_EXTERNAL_CHILD_REF`), plus parent/child lists and root ancestors.
5. **`Orphan & Backup Jobs`**: The **494 unreferenced ETL jobs** + **2 unreferenced Sequence jobs** (never called by any Sequence Job) and backup-named jobs (`CopyOf*`, `*_orig`, `*_old`, `*_bak`) that can be descoped before transpilation!

---

### E. [plan_migration_batch.py](plan_migration_batch.py) & [transpile_batch.py](transpile_batch.py) (Lineage- & History-Aware Batch Planner & Local Transpiler)

Built following the architecture in [Retail-Store-Data-Modeling-and-Analysis-using-DataStage/migration/README.md](Retail-Store-Data-Modeling-and-Analysis-using-DataStage/migration/README.md):

#### Two Transpilation Options for Extracted Batches
Once a batch is planned and its standalone `.xml` files are extracted (`migrated_batches/batch_XXX/xml/*.xml`), you have **two ways to transpile DataStage jobs to Databricks**:

| Option | Tool / Script | Engine | Best For |
| :--- | :--- | :--- | :--- |
| **Option 1: Local Deterministic Compiler** | `transpile_batch.py` (`ds2spark.py`) | Built-in Python AST/XML compiler | Fast batch transpilation, Lakeflow Workflow YAML bundle generation (`bundle/`), and Switch LLM specs (`switch_input/`). |
| **Option 2: Offline BladeBridge Engine** | `run_dbxconv.ps1` / `run_dbxconv.py` | Official BladeBridge `dbxconv.exe` binary | Official BladeBridge conversion (`PYSPARK`, `SPARKSQL`, or `DBSQL`) with `dbutils.widgets` on standalone `.xml` files (see **Section 3F**). |

#### Workflow Flowcharts

##### Plan Migration Batch Workflow (`plan_migration_batch.py`)
```mermaid
flowchart TD
    A["Start: uv run plan_migration_batch.py"] --> B{"Load job_hierarchy.json"}
    B --> C["Sort Root Trees (Complexity Priority)"]
    C --> D["Filter Roots (backups, already migrated)"]
    D --> E["Select Roots up to --num-jobs"]
    E --> F["Collect Lineage Closure"]
    F --> G["Partition Reused vs New Jobs"]
    G --> H["Topological Sort (Bottom-Up DFS)"]
    H --> I["Print Report & Append Manifest to migration_state.json"]
    I --> J["End"]
```

##### Transpile Batch Workflow (`transpile_batch.py`)
```mermaid
flowchart TD
    A["Start: uv run transpile_batch.py"] --> B["Parse Arguments"]
    B --> C["Load Migration State"]
    C --> D["Plan Batch: plan_migration_batch.py"]
    D --> E["Extract XML Chunks"]
    E --> F["Transpile Jobs to Python"]
    F --> G["Write Notebooks & Assets"]
    G --> H["Update migration_state.json"]
    H --> I["End"]
```

1. **Job Selection Logic & Guarantees:**
   - **Candidate Root Filtering:** Iterates through `rootTrees` in `job_hierarchy.json`. Automatically excludes backup roots (`CopyOf*`, `*_orig`, `*_old`, `*_bak`, `*_bkp`, `*_test`) and roots already migrated in `migration_state.json`. Supports folder/name substring filtering via `--filter <substring>` (e.g., `--filter ARBI`).
   - **Atomic Lineage Closure (No Orphan Jobs):** For each selected root sequence, `collect_tree_closure()` recursively traverses all child Sub-Sequences and leaf ETL jobs. Lineage trees are **never cut in half**: the entire closure of at least 1 root tree is always migrated together even if its job count exceeds `--num-jobs N`.
   - **History-Aware Deduplication (`REUSE_FROM_HISTORY`):** Downstream shared jobs (e.g. `PXJ_ETL_AUDIT_10_ld_stg` and `XJ_ETL_AUDIT`) already migrated in earlier batches are partitioned into `candidate_reused`:
     - They do **not** count against the `--num-jobs N` budget.
     - They are **not** re-extracted or re-transpiled.
     - In the generated Lakeflow Workflow DAG, the task is wired directly to the existing notebook path (`../../batch_001/notebooks/<job>.py`).
   - **Topological Bottom-Up Ordering:** A post-order DFS sorts the batch so dependencies are built first:
     1. Active Leaf ETL Jobs (`PXJ_*`, `XJ_*`) compiled to PySpark notebooks.
     2. Mid-Tier Sub-Sequences (`SEQX_*`) compiled to task groups.
     3. Root Master Sequence (`MSEQX_*`) compiled to the top-level Lakeflow DAG.
   - **Automatic Descoping of Dead Code:** Unreferenced orphan ETL jobs (494 jobs never called by any sequence) and orphan sequences (2 jobs) are never selected because they do not exist in any `rootTrees`.

2. **Where Manifests, Reports, and Outputs Are Saved:**
   - **`plan_migration_batch.py` (Standalone Planner):** Does **not** create standalone `.xml`, `.xlsx`, or per-batch `.json` files. It prints the formatted batch plan report directly to standard output (`stdout`) and appends the `batch_manifest` dictionary into **`./migration_state.json`** (unless `--dry-run` is passed).
   - **`transpile_batch.py` (End-to-End Batch Transpiler):** Updates **`./migration_state.json`** and writes all batch artifacts into **`migrated_batches/batch_XXX/`**:
     - `notebooks/*.py`: Deterministic compiled PySpark / Spark SQL notebooks (1 temp view per link, embedded SQL extracted from `TeradataConnectorPX`/`OracleConnectorPX` `<XMLProperties>`, `PxChangeCapture` FULL OUTER JOIN logic, `CTransformerStage` derivations & constraints, and positional/non-nullable string rules).
     - `bundle/databricks.yml` & `bundle/resources/workflow_batch_XXX.yml`: Ready-to-deploy Databricks Asset Bundle & Lakeflow Job DAG matching the Root Sequence and Sub-Sequence dependencies.
     - `switch_input/*.txt` & `xml/*.xml`: Condensed human-readable job specs for Lakebridge Switch LLM transpilation (`llm-transpile`) and standalone per-job `.xml` files ready for **Option 2 (`run_dbxconv`)**.

#### Commands to Plan & Transpile Batches:

```cmd
:: Preview next batch of ~12 new jobs without modifying migration_state.json
uv run plan_migration_batch.py --num-jobs 12 --dry-run

:: Run Batch #1 (resets history and transpiles first closed lineage tree of <=12 jobs to migrated_batches/batch_001/)
uv run transpile_batch.py --num-jobs 12 --reset-history

:: Run Batch #2 (automatically skips Batch #1 jobs and reuses shared audit jobs!)
uv run transpile_batch.py --num-jobs 10

:: Check cumulative migration history across all batches
uv run plan_migration_batch.py --status
```

#### Exporting a Batch Summary `.csv` or `.xlsx` from `migration_state.json`:

Because `transpile_batch.py` tracks all batch manifests inside `migration_state.json` (and does not generate `.xlsx` files natively), use either Windows `cmd.exe`-safe one-liner below to export a batch progress summary:

**1. Export to CSV (`migration_batches_summary.csv` — zero dependencies, runs offline in <1s):**
```cmd
uv run --offline python -c "import json, csv; s = json.load(open('migration_state.json', encoding='utf-8')); f = open('migration_batches_summary.csv', 'w', newline='', encoding='utf-8'); w = csv.writer(f); w.writerow(['Batch_ID', 'Job_Name', 'Job_Type', 'Complexity', 'Folder', 'Action', 'Root_Sequence', 'Total_Nodes', 'Source_Chunk', 'Migrated_At']); [w.writerow([b['batch_id'], j['name'], j['type'], j['complexity'], j['folder'], j['action'], j['root_sequence'], j['total_nodes'], j['source_chunk'], b['created_at']]) for b in s['batches'] for j in b['new_jobs_in_bottom_up_order']]; [w.writerow([b['batch_id'], r['name'], r['type'], r['complexity'], r['folder'], r['action'], ', '.join(b['selected_root_sequences']), '', 'Batch #' + str(r['from_batch_id']), b['created_at']]) for b in s['batches'] for r in b['reused_jobs_from_history']]; f.close(); print('Wrote migration_batches_summary.csv')"
```

**2. Export to Multi-Tab Excel (`migration_batches_summary.xlsx` — `Batches Overview` & `Batch Job Details`):**
```cmd
uv run --with pandas --with openpyxl python -c "import json, pandas as pd; s = json.load(open('migration_state.json', encoding='utf-8')); batches = [{'Batch_ID': b['batch_id'], 'Created_At': b['created_at'], 'Requested_Num_Jobs': b['requested_num_jobs'], 'New_Jobs': b['actual_new_jobs_count'], 'Reused_Jobs': b['reused_jobs_from_history_count'], 'Root_Sequences': ', '.join(b['selected_root_sequences'])} for b in s['batches']]; jobs = [{'Batch_ID': b['batch_id'], 'Order': i, 'Job_Name': j['name'], 'Type': j['type'], 'Complexity': j['complexity'], 'Folder': j['folder'], 'Action': j['action'], 'Root_Sequence': j['root_sequence'], 'Total_Nodes': j['total_nodes'], 'Source_Chunk': j['source_chunk']} for b in s['batches'] for i, j in enumerate(b['new_jobs_in_bottom_up_order'], 1)] + [{'Batch_ID': b['batch_id'], 'Order': None, 'Job_Name': r['name'], 'Type': r['type'], 'Complexity': r['complexity'], 'Folder': r['folder'], 'Action': r['action'], 'Root_Sequence': ', '.join(b['selected_root_sequences']), 'Total_Nodes': None, 'Source_Chunk': 'Batch #' + str(r['from_batch_id'])} for b in s['batches'] for r in b['reused_jobs_from_history']]; w = pd.ExcelWriter('migration_batches_summary.xlsx', engine='openpyxl'); pd.DataFrame(batches).to_excel(w, sheet_name='Batches Overview', index=False); pd.DataFrame(jobs).to_excel(w, sheet_name='Batch Job Details', index=False); w.close(); print('Wrote migration_batches_summary.xlsx')"
```

---

### F. [run_dbxconv.ps1](run_dbxconv.ps1) & [run_dbxconv.py](Retail-Store-Data-Modeling-and-Analysis-using-DataStage/migration/run_dbxconv.py) (Offline BladeBridge Transpilation on Windows)

As an alternative or complement to the deterministic compiler (`transpile_batch.py` / `ds2spark.py`), DataStage `.xml` files—including the standalone XMLs extracted into `migrated_batches/batch_XXX/xml/`—can be transpiled offline using BladeBridge's official `dbxconv.exe` engine via [run_dbxconv.py](Retail-Store-Data-Modeling-and-Analysis-using-DataStage/migration/run_dbxconv.py) and the PowerShell wrapper [run_dbxconv.ps1](run_dbxconv.ps1).

#### 1. Windows Prerequisites & Environment Setup
1. **Python Version (Must be Python 3.10 to 3.14 via `uv`):**
   - BladeBridge uses PyArmor-protected binaries for its `-H` security handshake hasher under `databricks/labs/bladebridge/hasher/windows_x86_64/pyarmor_runtime_000000/`.
   - Runtimes are only provided for Python **3.10 through 3.14**. Python 3.15+ is **not supported** and will fail with `ImportError`.
   - Always run with `uv run --python 3.12` (no need to hardcode paths or manage venvs manually).
2. **BladeBridge Plugin Directory (`bb_plugin`):**
   - Unpack the wheel `bb_wheel/databricks_bb_plugin-0.3.0-py3-none-any.whl` into `bb_plugin/`. The binary is at:
     ```text
     bb_plugin\databricks\labs\bladebridge\Converter\bin\Windows\dbxconv.exe
     ```
3. **Offline Handshake & `psutil` Shim:**
   - `dbxconv.exe` enforces a caller handshake by verifying the parent process command line using `psutil.Process(os.getpid()).cmdline()`.
   - To run completely offline without extra package dependencies, `bb_plugin/psutil.py` provides this shim directly:
     ```python
     import os, sys
     class Process:
         def __init__(self, pid=None):
             self.pid = pid if pid is not None else os.getpid()
         def cmdline(self):
             return [sys._base_executable] + sys.argv
         def name(self):
             return "python.exe"
         def parent(self):
             return None
     ```

#### 2. How to Run Using PowerShell ([run_dbxconv.ps1](run_dbxconv.ps1))
The PowerShell script automatically detects Python 3.12, verifies `bb_plugin`, handles single XML staging, and outputs converted notebooks:

```powershell
# 1. Transpile a single DataStage XML file to PySpark:
.\run_dbxconv.ps1 -XmlPath "<path\to\job.xml>" -OutputDir "<output_dir>" -Target PYSPARK

# 2. Transpile to SparkSQL or DBSQL:
.\run_dbxconv.ps1 -XmlPath "<path\to\job.xml>" -Target SPARKSQL
.\run_dbxconv.ps1 -XmlPath "<path\to\job.xml>" -Target DBSQL

# 3. Transpile an entire batch XML folder (e.g., extracted by transpile_batch.py):
.\run_dbxconv.ps1 -XmlPath ".\migrated_batches\batch_001\xml" -OutputDir ".\migrated_batches\batch_001\bladebridge_pyspark" -Target PYSPARK
```

#### 3. How to Run Directly via `uv run` (Folder & Single File)
`run_dbxconv.py` natively processes an entire folder of `.xml` files using Python 3.12 via `uv` without requiring full paths to python executables or virtualenvs:

**Run an Entire Folder of DataStage XML Files (e.g., `migrated_batches/batch_001/xml`):**
`run_dbxconv.py` scans the input directory for all `*.xml` files (`src.glob("*.xml")`) and converts each job sequentially in its own isolated work folder:

```powershell
# PowerShell or CMD:
uv run --python 3.12 <path\to\run_dbxconv.py> <path\to\bb_plugin> .\migrated_batches\batch_001\xml .\migrated_batches\batch_001\bladebridge_pyspark PYSPARK
```

- **Output Structure per Job:**
  For each `.xml` file in the folder, it generates:
  - `<output_dir>/<job_name>/originals/<job_name>.xml` (staged copy)
  - `<output_dir>/<job_name>/out/` (generated Databricks notebooks)
  - `<output_dir>/<job_name>/dbxconv.log` (execution log with handshake and conversion details)
- **Console Output:**
  Prints return code and notebook paths as each job finishes:
  ```text
  <job_name>                               rc=0 notebooks=1
      Jobs/<category>/<job_name>.py
  ...
  ```

**Run a Single XML File:**
If you have a single XML file, you can either:
- **Use the PowerShell script** (which auto-stages it into a temporary folder):
  ```powershell
  .\run_dbxconv.ps1 -XmlPath "<path\to\job.xml>" -OutputDir "<output_dir>"
  ```
- **Or place the single XML into its own folder** and pass that folder to `uv run`:
  ```cmd
  uv run --python 3.12 <path\to\run_dbxconv.py> <path\to\bb_plugin> <path\to\single_xml_dir> <output_dir> PYSPARK
  ```

#### 4. Supported Target Options
- `PYSPARK` (default): Databricks notebook with PySpark DataFrame transformations and `dbutils.widgets`.
- `SPARKSQL`: Databricks notebook with `spark.sql(...)` temporary views registered per stage.
- `DBSQL`: Plain Databricks SQL script (`.sql`).

---

### G. [analyze.py](analyze.py) (Single-File or Single-Folder Runner)
Uses `uv` inline script dependencies (`databricks-bb-analyzer~=0.3.0,!=0.3.1`) and supports passing **either a directory or a single `.xml` file** via `--source-directory`.

> **Important Note on `uv run`:** Always invoke `uv run analyze.py ...` directly (do **not** put `python -u` between `uv run` and `analyze.py`, otherwise `uv` uses the system Python instead of the isolated PEP 723 script environment containing `databricks-bb-analyzer`).

```cmd
uv run analyze.py --list-tech
uv run analyze.py --source-directory ./seq_only_chunks/chunk_0001.xml --report-file ./report_chunk1.xlsx --source-tech "Datastage" --generate-json
```

---

## 4. Complete Analysis Results Summary (Unified & By Layer)

### A. Unified Project-Wide Master Report (`consolidated_all_jobs` — All 12 Chunks Combined)
- **Consolidated Master Reports in the project root directory:**
  - JSON: [consolidated_all_jobs.json](consolidated_all_jobs.json) (`16.36 MB`)
  - Excel: [consolidated_all_jobs.xlsx](consolidated_all_jobs.xlsx) (`10.69 MB`)
- **Total Unique Items Analyzed:** **5,699** (`5,664` Jobs + `35` Routines — 100% of the export!)
  - `PARALLEL`: **2,945**
  - `SEQUENCE`: **2,717**
  - `SERVER`: **2**
  - `ROUTINE`: **35**
- **Overall Complexity Breakdown:**
  - `LOW`: **1,014**
  - `MEDIUM`: **4,638**
  - `HIGH`: **46**
  - `VERY_HIGH`: **1**
- **Total Nodes Across All Jobs:** **84,269**
- **Total Subjob Links (`subJobInfo`):** **9,977**

---

### B. Sequence Jobs Summary (`JobType="2"`)
- **Consolidated Reports in the project root directory:**
  - JSON: [consolidated_sequence_jobs.json](consolidated_sequence_jobs.json) (`8.82 MB`)
  - Excel: [consolidated_sequence_jobs.xlsx](consolidated_sequence_jobs.xlsx) (`7.53 MB`)
- **Parallel Execution Time:** **501 seconds (~8.3 minutes)** across 6 workers
- **Total Unique Items Analyzed:** **2,752** (`2,717` Sequence Jobs + `35` Routines)
- **Total Subjob Links (`subJobInfo`):** **9,977** parent $\rightarrow$ child job invocations
- **Complexity Breakdown:**
  - `LOW`: **300**
  - `MEDIUM`: **2,413**
  - `HIGH`: **39**
- **Total Nodes Across Sequence Jobs:** **64,822**
  - `Annotation`: `29,028`
  - `CRoutineActivity`: `14,954`
  - `CJobActivity`: `9,978`
  - `CTerminatorActivity`: `5,176`
  - `CExecCommandActivity`: `2,807`
  - `CExceptionHandler`: `2,712`
  - `CNotificationActivity`: `45`
  - `CCondition`: `41`
  - `CSequencer`: `40`
  - `CUserVarsActivity`: `33`

---

### C. Parallel & Server ETL Jobs Summary (`JobType="3"` & `"0"`)
- **Consolidated Reports in the project root directory:**
  - JSON: [consolidated_etl_jobs.json](consolidated_etl_jobs.json) (`7.57 MB`)
  - Excel: [consolidated_etl_jobs.xlsx](consolidated_etl_jobs.xlsx) (`1.96 MB`)
- **Parallel Execution Time:** **1,735 seconds (~28.9 minutes)** across 6 workers (including embedded SQL extraction and analysis)
- **Total Unique Items Analyzed:** **2,982** (`2,945` Parallel Jobs + `2` Server Jobs + `35` Routines)
- **Complexity Breakdown:**
  - `LOW`: **714**
  - `MEDIUM`: **2,225**
  - `HIGH`: **42**
  - `VERY_HIGH`: **1**
- **Total Nodes Across ETL Jobs:** **19,447**
  - `Annotation`: `5,319`
  - `TransformerStage`: `4,574`
  - `TeradataConnectorPX`: `4,233`
  - `PxSequentialFile`: `1,957`
  - `PxDataSet`: `1,385`
  - `OracleConnectorPX`: `617`
  - `PxRowGenerator`: `563`
  - `PxChangeCapture`: `236`
  - `PxJoin`: `212`
  - `PxFunnel`: `112`
  - `ODBCConnectorPX`: `107`
  - `PxAggregator`: `38`
  - `PxCopy`: `35`
  - `PxLookup`: `23`
  - `PxSort`: `10`

---

### D. Hierarchy & Lineage Classification Summary (`job_hierarchy`)
- **Generated Hierarchy Reports in the project root directory:**
  - Excel (5 Tabs): [job_hierarchy.xlsx](job_hierarchy.xlsx) (`0.85 MB`)
  - JSON Tree: [job_hierarchy.json](job_hierarchy.json) (`2.91 MB`)
- **Total Root-to-Leaf Execution Paths:** **8,913** (`Depth 1`: `3,776` paths | `Depth 2`: `4,836` paths | `Depth 3`: `301` paths)

| Hierarchy Role | Count | Migration Action |
| :--- | :--- | :--- |
| `1_ROOT_SEQUENCE (Top-Level Orchestrator)` | **1,539** | Convert to top-level Databricks Lakeflow Workflow (`workflow_batch_XXX.yml`) |
| `2_SUB_SEQUENCE (Mid-Tier Orchestrator)` | **1,175** | Flatten into dependent task groups inside the Lakeflow Workflow |
| `3_LEAF_SEQUENCE (No Subjobs)` | **1** | Convert command/routine activities |
| `4_ACTIVE_ETL_JOB (Scheduled by Sequence)` | **2,453** | **In-Scope** — Transpile to PySpark / Spark SQL Notebooks (`notebooks/*.py`) |
| `5_ORPHAN_SEQUENCE (Unreferenced & Empty)` | **2** | **Descope / Archive** (never called, calls nothing) |
| `6_ORPHAN_ETL_JOB (Unreferenced by any Sequence)` | **494** | **Descope / Archive** (16.8% of ETL jobs are unreferenced/retired copies) |
| `7_ROUTINE` | **35** | Map to shared Python helper functions |
| `8_MISSING_EXTERNAL_CHILD_REF` | **3** | External job references not present in the XML export |

---

### E. End-to-End Batch Transpilation Test Results (`migrated_batches/`)

Running `transpile_batch.py` across two consecutive batches verified both **complete lineage closure** (zero orphans) and **history deduplication** (zero duplicate jobs across re-runs via [migration_state.json](migration_state.json)):

#### 1. **Batch #001 (`--num-jobs 12` $\rightarrow$ `migrated_batches/batch_001/`)**
- **Root Master Sequence:** `MSEQX_ARBI_AR_ADV_PAYMENT_ld_bs` (`\Jobs\ARBI\BASE\AR_ADV_PAYMENT`)
- **Jobs Selected (11 new jobs, 0 reused):**
  - **6 Compiled & Syntax-Verified PySpark/SparkSQL Notebooks (`batch_001/notebooks/*.py`):**
    1. [PXJ_ARBI_AR_ADV_PAYMENT_10_ld_ds.py](migrated_batches/batch_001/notebooks/PXJ_ARBI_AR_ADV_PAYMENT_10_ld_ds.py) (7 stages, 133 lines: Teradata source SQL $\rightarrow$ `PxChangeCapture` $\rightarrow$ 3-way `CTransformerStage` split into INS/DEL/UPD Delta tables)
    2. [PXJ_ARBI_AR_ADV_PAYMENT_20_ins_bs.py](migrated_batches/batch_001/notebooks/PXJ_ARBI_AR_ADV_PAYMENT_20_ins_bs.py) (5 stages, 91 lines)
    3. [PXJ_ARBI_AR_ADV_PAYMENT_30_upd_bs.py](migrated_batches/batch_001/notebooks/PXJ_ARBI_AR_ADV_PAYMENT_30_upd_bs.py) (5 stages, 89 lines)
    4. [PXJ_ARBI_AR_ADV_PAYMENT_40_del_bs.py](migrated_batches/batch_001/notebooks/PXJ_ARBI_AR_ADV_PAYMENT_40_del_bs.py) (5 stages, 77 lines)
    5. [PXJ_ETL_AUDIT_10_ld_stg.py](migrated_batches/batch_001/notebooks/PXJ_ETL_AUDIT_10_ld_stg.py) (8 stages, 138 lines — *Shared Audit Parallel Job*)
    6. [XJ_ETL_AUDIT.py](migrated_batches/batch_001/notebooks/XJ_ETL_AUDIT.py) (8 stages, 117 lines — *Shared Audit Server Job*)
  - **5 Sequence Jobs (`MSEQX_*` + 4 `SEQX_*` sub-sequences) Compiled into Lakeflow Bundle:**
    - [databricks.yml](migrated_batches/batch_001/bundle/databricks.yml) & [workflow_batch_001.yml](migrated_batches/batch_001/bundle/resources/workflow_batch_001.yml) (12 chained Lakeflow tasks)
  - **11 Condensed Switch LLM Specs & Standalone XMLs:**
    - `migrated_batches/batch_001/switch_input/*.txt` & `migrated_batches/batch_001/xml/*.xml`
  - **Verified Offline BladeBridge (`run_dbxconv`) Output on Batch #001 XML:**
    - Ran `run_dbxconv.ps1` / `run_dbxconv.py` on [PXJ_ARBI_AR_ADV_PAYMENT_20_ins_bs.xml](migrated_batches/batch_001/xml/PXJ_ARBI_AR_ADV_PAYMENT_20_ins_bs.xml) to generate [PXJ_ARBI_AR_ADV_PAYMENT_20_ins_bs.py](migrated_batches/batch_001/bladebridge_pyspark/PXJ_ARBI_AR_ADV_PAYMENT_20_ins_bs/out/out/Jobs/ARBI/BASE/AR_ADV_PAYMENT/PXJ_ARBI_AR_ADV_PAYMENT_20_ins_bs.py) (149 lines with `dbutils.widgets`, schema mappings, derivations, and filter rules; `dbxconv.log` confirmed `trusted transpiler handshake complete`).

#### 2. **Batch #002 (`--num-jobs 10` $\rightarrow$ `migrated_batches/batch_002/`)**
- **Root Master Sequence:** `MSEQX_ARBI_AR_BANK_ACCT_CPTY_ld_bs` (`\Jobs\ARBI\BASE\AR_BANK_ACCT_CPTY`)
- **History Deduplication Verified:**
  - Automatically skipped `MSEQX_ARBI_AR_ADV_PAYMENT_ld_bs` (already in `migration_state.json`).
  - Detected that `PXJ_ETL_AUDIT_10_ld_stg` and `XJ_ETL_AUDIT` were **already migrated in Batch #001**, marking both as `REUSE_FROM_HISTORY`.
  - Transpiled only the **9 new jobs** (`4` new `PXJ_ARBI_AR_BANK_ACCT_CPTY_*` notebooks + `5` sequences) and wired [workflow_batch_002.yml](migrated_batches/batch_002/bundle/resources/workflow_batch_002.yml) to reuse `../../batch_001/notebooks/PXJ_ETL_AUDIT_10_ld_stg.py` and `../../batch_001/notebooks/XJ_ETL_AUDIT.py` without duplicating them!

---

## 5. How to Inspect & Analyze the `.xlsx` and `.json` Reports

### A. Structure of the `.xlsx` Workbook Tabs
Each Lakebridge Analyzer Excel report (as well as the merged `consolidated_*.xlsx` workbooks) contains the following key worksheets:

| Worksheet Name | What It Contains | How to Use It for Migration Planning |
| :--- | :--- | :--- |
| **`Summary`** | Total unique jobs, total nodes, job counts by Complexity (`LOW`, `MEDIUM`, `HIGH`, `VERY_HIGH`), and counts by Job Type (`SEQUENCE`, `PARALLEL`, `SERVER`, `ROUTINE`). | High-level executive sizing and effort estimation. |
| **`Job Details`** | 1 row per job: `Job Name`, `Folder`, `Source File`, `Included`, `Job Type`, `Categorization` (complexity), `Number of Nodes`, `Categorization Metrics`, and `Checksum Extended`. | Master job inventory. Filter `Job Name` to spot backup copies (`CopyOf*`, `*_orig`, `*_old`, `*_bak`) and filter `Checksum Extended` to find exact duplicate jobs across folders. |
| **`Transformations`** | Aggregated counts of every DataStage stage/activity type (`PxLookup`, `CTransformerStage`, `OracleConnectorPX`, `CJobActivity`, etc.) and its mapped Lakebridge category. | Identify which connectors and transformation patterns exist across the project. |
| **`Jobs Transformations List`** | `(Job, Transformation, Count)` breakdown per job. | Filter for specific complex stages (e.g., `CTransformerStage`, `PxJoin`, `JSRoutineActivity`) to see which jobs use them. |
| **`Subjob Info`** | `(Parent Item, Child Item, Parent Type, Child Type, Count)` parent $\rightarrow$ child call graph. | Crucial for **dependency lineage**: join `Subjob Info` (from Sequence Jobs) against `Job Details` (from Parallel Jobs) to see which Parallel ETL jobs are actively orchestrated vs. unreferenced orphan jobs. |
| **`Item Node Info`** | Ordered list of nodes (`Node Name`, `Orig Node Type`, `Conformed Node Type`, `Node Order`) inside each job. | Step-by-step stage lineage inside each job. |
| **`Job Parameters`** | Parameter names, types (`STRING`, `PARAM_SET`, `ENCRYPTED`), default expressions, and help text per job. | Map DataStage Parameter Sets (`EFBI_BS_PARAMETER_SET`, `.env` files) to Databricks Job Parameters / Widgets / Secret Scopes. |
| **`Transformation Expressions`** | `(Job, Transformation, Field, Expression)` formulas inside Transformer / Condition stages. | Inspect custom expressions, functions, and business rules that need transpilation to Spark SQL / PySpark. |
| **`Misc Job Attributes`** | Stage-level properties such as `ROUTINE_NAME`, `CONNECTION_KEY`, `DS_TYPE`, target table names, and file paths. | Identify source/target database connections, custom routines called, and file paths. |
| **`Embedded SQL Summary` / `Programs`** | Extracted SQL statements embedded inside connector stages (`OracleConnectorPX`, `DB2ConnectorPX`, `ODBCConnectorPX`). | Analyze custom SQL overrides, joins, and DDL complexity. |

---

### B. Structure of the `.json` Report & Quick Python Queries
The `.json` report conforms to Lakebridge's `analyzer-etl-schema.json` and has three top-level keys:
- `runInfo`: Metadata about the analyzer run (`sourceTechnology`, `startTime`, `runDurationSeconds`, `reportName`).
- `inventory`: Array of job objects (`name`, `folder`, `fullyQualifiedName`, `type`, `complexityLevel`, `nodes` dict, `jobParameters` list, `sourceFile`).
- `subJobInfo`: Array of parent-child lineage edges (`parent`, `parentType`, `child`, `childType`, `count`).

#### Useful One-Liners to Query the Consolidated `.json` Report in `cmd`:

**1. Count jobs by complexity level and job type:**
```cmd
uv run --offline python -c "import json, collections; d = json.load(open('consolidated_sequence_jobs.json')); print('Types:', collections.Counter(x['type'] for x in d['inventory'])); print('Complexity:', collections.Counter(x['complexityLevel'] for x in d['inventory']))"
```

**2. Find backup / duplicate jobs (`CopyOf*`, `*_orig`, `*_old`, `*_bak`):**
```cmd
uv run --offline python -c "import json; d = json.load(open('consolidated_sequence_jobs.json')); baks = [x['fullyQualifiedName'] for x in d['inventory'] if any(k in x['name'].lower() for k in ('copyof', '_orig', '_old', '_bak'))]; print(f'Found {len(baks)} backup jobs:', baks[:15])"
```

**3. List all Parallel ETL jobs called by Sequence Jobs (Active vs. Orphan Check):**
```cmd
uv run --offline python -c "import json; seq = json.load(open('consolidated_sequence_jobs.json')); called = {s['child'] for s in seq['subJobInfo'] if s['childType'] == 'Job'}; print(f'Unique child jobs triggered by Sequence Jobs: {len(called)}')"
```

**4. Export the `Job Details` inventory from JSON to a flat `.csv` for quick filtering:**
```cmd
uv run --offline python -c "import json, csv; d = json.load(open('consolidated_sequence_jobs.json')); f = open('sequence_jobs_inventory.csv', 'w', newline='', encoding='utf-8'); w = csv.writer(f); w.writerow(['Name', 'Folder', 'Type', 'Complexity', 'TotalNodes', 'SourceFile']); [w.writerow([x['name'], x.get('folder',''), x['type'], x['complexityLevel'], sum(x.get('nodes',{}).values()), x.get('sourceFile','')]) for x in d['inventory']]; f.close(); print('Wrote sequence_jobs_inventory.csv')"
```

---

## 6. How to Check If the Analyzer Is Working or Stuck on Windows

When `analyze_parallel.py` or `analyze.py` is running, it spawns `analyzer.exe` child processes. To check live CPU and memory usage from a second `cmd.exe` window:

```cmd
tasklist /FI "IMAGENAME eq analyzer.exe"
```
Or in **PowerShell**:
```powershell
Get-Process analyzer*, python* -ErrorAction SilentlyContinue | Select-Object Id, ProcessName, CPU, @{Name="Memory_MB";Expression={[math]::Round($_.WorkingSet64 / 1MB, 1)}}
```
- When running `analyze_parallel.py --workers 6`, you will see **6 `analyzer.exe` processes** running simultaneously across your CPU cores.
- Make sure you don't left-click inside the `cmd` / PowerShell window while it runs (Windows Console **QuickEdit Mode** will pause output until you press `Esc`).
