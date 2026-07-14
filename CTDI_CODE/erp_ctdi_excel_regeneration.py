# Databricks notebook source
# MAGIC %md
# MAGIC # ERP - CTDI Excel **Regeneration** Notebook
# MAGIC Rebuilds a formatted `.xlsx` from a curated `rawstd__ctdi.<table>` Delta table - the reverse of
# MAGIC `erp_bicc_excel_ingestion.py`.
# MAGIC
# MAGIC **What this notebook does (high level):**
# MAGIC 1. Take one `input_table_name` (schema is fixed: `rawstd__ctdi`).
# MAGIC 2. Look up `bicc_table_metadata` for that table -> `file_name_prefix`, `sheet_tab_name`,
# MAGIC    `array_column_name`, PK columns, and the `load_type_delta` flag.
# MAGIC 3. Look up `bicc_process_control` for that table -> the **latest** run (`max(_az_insert_ts)`)
# MAGIC    tells us the expected record count. If that run shows **0 records**, a **headers-only** file
# MAGIC    is generated (no query against the data needed beyond column discovery).
# MAGIC 4. Read `rawstd__ctdi.<table>`. The `DATA` column (JSON string or VARIANT) is unfolded back into
# MAGIC    individual columns using the **column names stored in `DATA_KEYS`**.
# MAGIC    - `load_type_delta = true`  -> PRIMARY KEY column(s) first, then the remaining (unfolded) columns.
# MAGIC    - `load_type_delta = false` -> just the unfolded `DATA_KEYS` columns (no PK - full-load table).
# MAGIC 5. Write one `.xlsx` (bold header row + autofilter) to a Volume path. A banner row (matching the
# MAGIC    original CTDI layout) is included by default so the file is directly re-ingestible for testing.
# MAGIC
# MAGIC **Library note:** writing `.xlsx` needs a *write*-capable library - `openpyxl` was ruled out for the
# MAGIC ingestion notebook (read-performance reasons) so this uses **`xlsxwriter`** instead (lightweight,
# MAGIC write-only, and the standard choice for generating formatted Excel reports from Spark/pandas).

# COMMAND ----------

# DBTITLE 1,Install libraries (xlsxwriter - NOT openpyxl)
import sys, subprocess, pkg_resources

required = {'xlsxwriter'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

# COMMAND ----------

# DBTITLE 1,All imports
import json, os, re, uuid, datetime as dt
from datetime import datetime

import xlsxwriter
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, expr

# COMMAND ----------

# DBTITLE 1,Widgets - ALL required inputs live here at the top
# input_table_name : the ONLY thing you normally need to change - e.g. 'ohb_comparison'
# notebook_config   : JSON config (schema names / output path) - see sample below
# execution_id      : optional, only used to tag the output file / log lines
dbutils.widgets.text('input_table_name', '')
dbutils.widgets.text('notebook_config', '')
dbutils.widgets.text('execution_id', '')

# COMMAND ----------

# DBTITLE 1,Parse config + constants
# ------------------------------------------------------------------------------------------------
# Sample notebook_config:
# {
#   "validations": {"metadata_table":"drvd__app_ctdi.bicc_table_metadata",
#                   "process_control":"drvd__app_ctdi.bicc_process_control"},
#   "processing":  {"rawstd_schema":"rawstd__ctdi",
#                   "output_path":"/Volumes/edl_dev/rawstd__ela_stm/rawstd__stm/regenerated",
#                   "include_banner_row":"true",
#                   "max_rows_to_regenerate":"500000",
#                   "exit_on_finish":"false"}
# }
# ------------------------------------------------------------------------------------------------
execution_id = dbutils.widgets.get('execution_id').strip() or f"REGEN_{datetime.now().strftime('%Y%m%d%H%M%S')}"
input_table_name = dbutils.widgets.get('input_table_name').strip()
notebook_config = json.loads(dbutils.widgets.get('notebook_config'))

METADATA_TABLE   = notebook_config['validations']['metadata_table']
PROCESS_CONTROL  = notebook_config['validations']['process_control']
RAWSTD_SCHEMA    = notebook_config['processing'].get('rawstd_schema', 'rawstd__ctdi')
OUTPUT_PATH      = notebook_config['processing']['output_path']
INCLUDE_BANNER   = str(notebook_config['processing'].get('include_banner_row', 'true')).lower() == 'true'
MAX_ROWS         = int(notebook_config['processing'].get('max_rows_to_regenerate', 500000))
EXIT_ON_FINISH   = str(notebook_config['processing'].get('exit_on_finish', 'true')).lower() == 'true'

RUN_LOG = []
def log_step(msg, level='INFO'):
    tag = '' if level == 'INFO' else f"[{level}] "
    line = f"{tag}{msg}"
    RUN_LOG.append(line)
    print(line)

# COMMAND ----------

# DBTITLE 1,Validate inputs + config up-front - fail fast with a clear message
def validate_inputs():
    problems = []
    if not input_table_name:
        problems.append("widget 'input_table_name' is required, e.g. 'ohb_comparison'")
    for key in ['validations', 'processing']:
        if key not in notebook_config:
            problems.append(f"missing config section '{key}'")
    if 'output_path' not in notebook_config.get('processing', {}):
        problems.append("missing config key 'processing.output_path'")
    for t in [METADATA_TABLE, PROCESS_CONTROL]:
        try:
            spark.read.table(t).schema
        except Exception as e:
            problems.append(f"table '{t}' not readable - {str(e).splitlines()[0]}")
    if problems:
        raise Exception("Config/setup validation failed:\n  - " + "\n  - ".join(problems))
    log_step(f"STEP 1 - Validated inputs | table={input_table_name} | rawstd_schema={RAWSTD_SCHEMA}")

validate_inputs()

# COMMAND ----------

# DBTITLE 1,Util - safe Excel tab name (<=31 chars, no invalid characters)
_INVALID_SHEETNAME_CHARS = re.compile(r'[\\/*?:\[\]]')

def sanitize_sheet_name(name, fallback):
    name = _INVALID_SHEETNAME_CHARS.sub('_', str(name or fallback)).strip()
    return (name or fallback)[:31]

# COMMAND ----------

# DBTITLE 1,Util - optional boolean metadata flag reader (mirrors the ingestion notebook)
def _meta_flag(row, name, default=True):
    if name in row.__fields__ and row[name] is not None:
        return bool(row[name])
    return default

# COMMAND ----------

# DBTITLE 1,STEP 2 - Look up metadata for this table (file_name_prefix, sheet, PK cols, load_type_delta)
def get_table_metadata(table_name):
    rows = (spark.read.table(METADATA_TABLE)
            .filter(col('table_name') == lit(table_name))
            .orderBy(col('serial_number'))
            .collect())
    if not rows:
        raise Exception(f"No metadata found for table_name='{table_name}' in {METADATA_TABLE}")

    combos = {(r['file_name_prefix'], r['sheet_tab_name']) for r in rows}
    if len(combos) > 1:
        log_step(f"multiple (file_name_prefix, sheet_tab_name) combos found for '{table_name}': "
                 f"{combos} - using the first one", level='WARN')

    file_name_prefix = rows[0]['file_name_prefix']
    sheet_tab_name    = rows[0]['sheet_tab_name']
    array_col         = rows[0]['array_column_name'] or 'DATA'
    load_type_delta   = _meta_flag(rows[0], 'load_type_delta', True)
    pk_cols = ([r['original_column_name'] or r['sheet_column_name']
                for r in rows if r['is_primary_key']] if load_type_delta else [])

    log_step(f"STEP 2 - Metadata: file_name_prefix={file_name_prefix} | sheet_tab_name='{sheet_tab_name}' "
             f"| array_col={array_col} | load_type_delta={load_type_delta} | pk_cols={pk_cols}")
    return file_name_prefix, sheet_tab_name, array_col, load_type_delta, pk_cols

file_name_prefix, sheet_tab_name, array_col, load_type_delta, pk_cols = get_table_metadata(input_table_name)

# COMMAND ----------

# DBTITLE 1,STEP 3 - Look up the LATEST process-control run for this table (expected record count)
def get_latest_control_row(table_name):
    rows = (spark.read.table(PROCESS_CONTROL)
            .filter(col('table_name') == lit(table_name))
            .orderBy(col('_az_insert_ts').desc())
            .limit(1)
            .collect())
    if not rows:
        log_step(f"no process-control history found for '{table_name}' - will fall back to the "
                 f"current row count of {RAWSTD_SCHEMA}.{table_name}", level='WARN')
        return None
    r = rows[0]
    expected = r['valid_record_count'] if r['valid_record_count'] is not None else r['source_row_count']
    log_step(f"STEP 3 - Latest process-control run: _az_insert_ts={r['_az_insert_ts']} | "
             f"batch_id={r['batch_id']} | file_name={r['file_name']} | expected_record_count={expected}")
    return r

latest_ctrl = get_latest_control_row(input_table_name)
expected_count = (latest_ctrl['valid_record_count'] if latest_ctrl and latest_ctrl['valid_record_count'] is not None
                  else latest_ctrl['source_row_count'] if latest_ctrl else None)

# COMMAND ----------

# DBTITLE 1,STEP 4 - Read the curated table + detect DATA column physical type (json string vs variant)
full_table = f"{RAWSTD_SCHEMA}.{input_table_name}"
if not spark.catalog.tableExists(full_table):
    raise Exception(f"Table '{full_table}' does not exist - has it been loaded yet?")

df_full = spark.read.table(full_table)
dtypes = dict(df_full.dtypes)

if array_col not in dtypes:
    raise Exception(f"Column '{array_col}' (array_column_name from metadata) not found in {full_table}. "
                    f"Available columns: {df_full.columns}")
if 'DATA_KEYS' not in dtypes:
    raise Exception(f"Column 'DATA_KEYS' not found in {full_table} - cannot reconstruct original columns.")

IS_VARIANT = dtypes[array_col] == 'variant'
actual_count = df_full.count()
log_step(f"STEP 4 - Read {full_table} | rows={actual_count} | DATA column type={dtypes[array_col]}")

if expected_count is not None and actual_count != expected_count:
    log_step(f"rawstd row count ({actual_count}) differs from latest process-control "
             f"expected count ({expected_count}) - table may have changed since that run", level='WARN')

if actual_count > MAX_ROWS:
    raise Exception(f"{full_table} has {actual_count} rows, over the configured max_rows_to_regenerate "
                    f"({MAX_ROWS}). Raise processing.max_rows_to_regenerate if this is expected.")

# COMMAND ----------

# DBTITLE 1,STEP 5 - Discover DATA_KEYS (the original non-PK column names, in original order)
def get_data_keys(df, keys_col='DATA_KEYS'):
    """Reads DATA_KEYS from the first non-null row found (all rows normally share the same keys,
       since they come from the same sheet). Works for both string(JSON) and variant storage."""
    if dtypes[keys_col] == 'string':
        row = df.select(keys_col).where(col(keys_col).isNotNull()).limit(1).collect()
        return json.loads(row[0][keys_col]) if row else []
    else:  # variant
        row = df.select(expr(f"cast(`{keys_col}` as string)").alias('k')) \
                .where(col(keys_col).isNotNull()).limit(1).collect()
        return json.loads(row[0]['k']) if row else []

# Look up keys from the FULL table (not filtered) so a headers-only regen still knows the columns,
# even if the very latest run happened to load zero records.
data_keys = get_data_keys(df_full)
if not data_keys:
    log_step("DATA_KEYS is empty/unavailable across the whole table - the sheet may never have "
             "received non-PK data. The regenerated file will only contain PK columns (if any).",
             level='WARN')
log_step(f"STEP 5 - DATA_KEYS discovered: {data_keys}")

# COMMAND ----------

# DBTITLE 1,STEP 6 - Build the final column list + extraction expressions
def _json_path_key(key):
    return key.replace('\\', '\\\\').replace('"', '\\"')

def extract_value_expr(is_variant, array_col, key):
    """One non-PK value out of DATA, as a string - works for both storage modes."""
    path = f'$."{_json_path_key(key)}"'
    if is_variant:
        safe_path = path.replace("'", "''")
        return expr(f"try_variant_get(`{array_col}`, '{safe_path}', 'string')").alias(key)
    return F.get_json_object(col(f'`{array_col}`'), path).alias(key)

non_pk_keys = [k for k in data_keys if k not in pk_cols]

select_exprs = []
if load_type_delta:
    # PK column(s) first (kept as their real curated type), THEN the remaining unfolded columns
    for c in pk_cols:
        select_exprs.append(col(f'`{c}`').alias(c))
for k in non_pk_keys:
    select_exprs.append(extract_value_expr(IS_VARIANT, array_col, k))

final_columns = (pk_cols if load_type_delta else []) + non_pk_keys
log_step(f"STEP 6 - Final Excel column order ({len(final_columns)}): {final_columns}")

if not select_exprs:
    raise Exception("No columns could be resolved (no PK columns and no DATA_KEYS) - nothing to write.")

# COMMAND ----------

# DBTITLE 1,STEP 7 - Decide data vs headers-only, then materialize to pandas
if expected_count == 0:
    log_step("STEP 7 - Latest process-control run shows 0 records -> generating a HEADERS-ONLY file "
             "(no data rows).", level='WARN')
    df_final = df_full.select(*select_exprs).limit(0)
else:
    df_final = df_full.select(*select_exprs)

pdf = df_final.toPandas()
row_count = len(pdf)
log_step(f"STEP 7 - Collected {row_count} row(s) x {len(final_columns)} column(s) for regeneration")

# COMMAND ----------

# DBTITLE 1,STEP 8 - Write the formatted .xlsx (bold header + autofilter; xlsxwriter, not openpyxl)
def write_regenerated_excel(pdf, sheet_tab_name, fallback_name, include_banner):
    local_path = f"/tmp/{uuid.uuid4().hex}_{fallback_name}.xlsx"
    wb = xlsxwriter.Workbook(local_path)
    ws = wb.add_worksheet(sanitize_sheet_name(sheet_tab_name, fallback_name))

    header_fmt = wb.add_format({'bold': True, 'bg_color': '#D9D9D9', 'border': 1, 'text_wrap': True})
    date_fmt   = wb.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss'})

    header_row = 0
    if include_banner:
        banner = f"{sheet_tab_name} - {datetime.now().strftime('%m/%d/%Y %I:%M %p')} (REGENERATED)"
        ws.write(0, 0, banner)
        header_row = 1

    headers = list(pdf.columns)
    for c_idx, h in enumerate(headers):
        ws.write(header_row, c_idx, h, header_fmt)
        ws.set_column(c_idx, c_idx, max(12, min(40, len(str(h)) + 4)))

    for r_idx, row in enumerate(pdf.itertuples(index=False), start=header_row + 1):
        for c_idx, val in enumerate(row):
            if val is None or pd.isna(val):          # covers None, NaN, and NaT alike
                ws.write_blank(r_idx, c_idx, None)
            elif isinstance(val, (dt.date, dt.datetime, pd.Timestamp)):
                ws.write_datetime(r_idx, c_idx, val, date_fmt)
            else:
                ws.write(r_idx, c_idx, val)

    last_row = header_row + max(len(pdf), 0)  # header-only range when there is no data
    if headers:
        ws.autofilter(header_row, 0, last_row, len(headers) - 1)
    ws.freeze_panes(header_row + 1, 0)

    wb.close()
    return local_path

local_xlsx = write_regenerated_excel(pdf, sheet_tab_name, input_table_name, INCLUDE_BANNER)
log_step(f"STEP 8 - Local xlsx built -> {local_xlsx} ({os.path.getsize(local_xlsx)} bytes)")

# COMMAND ----------

# DBTITLE 1,STEP 9 - Copy to the Volume output path
def resolve_regen_output_path(file_name_prefix):
    now = datetime.now()
    dated = now.strftime('%Y/%m/%d/%H')
    file_name = f"{file_name_prefix}_REGEN_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
    folder = f"{OUTPUT_PATH.rstrip('/')}/{input_table_name}/{dated}"
    return folder, f"{folder}/{file_name}"

target_folder, target_file = resolve_regen_output_path(file_name_prefix)
dbutils.fs.mkdirs(target_folder)
dbutils.fs.cp(f"file:{local_xlsx}", target_file, True)
os.remove(local_xlsx)
log_step(f"STEP 9 - Uploaded to Volume -> {target_file}")

# COMMAND ----------

# DBTITLE 1,STEP 10 - Summary + exit
summary = {
    'status': 0,
    'execution_id': execution_id,
    'input_table': full_table,
    'file_name_prefix': file_name_prefix,
    'sheet_tab_name': sheet_tab_name,
    'load_type_delta': load_type_delta,
    'pk_columns': pk_cols,
    'data_columns': non_pk_keys,
    'expected_record_count': expected_count,
    'actual_rawstd_row_count': actual_count,
    'rows_written': row_count,
    'headers_only': row_count == 0,
    'output_file': target_file,
}
log_step('=' * 90)
log_step(f"REGENERATION SUMMARY: {json.dumps(summary, indent=1, default=str)}")
log_step('=' * 90)

if EXIT_ON_FINISH:
    dbutils.notebook.exit(json.dumps(summary, indent=1, default=str))
else:
    print(json.dumps(summary, indent=1, default=str))

# COMMAND ----------

# DBTITLE 1,NEXT CELL - step-by-step run log (only runs when exit_on_finish=false)
print("================  STEP-BY-STEP RUN LOG  ================")
for line in RUN_LOG:
    print(line)
print("\n================  OUTPUT  ================")
print(f"File: {target_file}")
display(pdf.head(20))
