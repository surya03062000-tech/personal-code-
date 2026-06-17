# Databricks notebook source
# MAGIC %md
# MAGIC # ERP - BICC **Excel** File Ingestion into YAAF
# MAGIC 200_databricks pre-processing for BICC **Excel (.xlsx)** files (CTDI feeds).
# MAGIC
# MAGIC **What this notebook does (high level):**
# MAGIC 1. (Prod) Decrypt `*.xlsx.gpg` files  →  (Test) read a plain `.xlsx` directly (`decrypt_flag`).
# MAGIC 2. Read **every sheet (tab)** of the workbook with **python-calamine** (fast, no `openpyxl`).
# MAGIC 3. Land each sheet **as-is** as a raw parquet (lineage).
# MAGIC 4. Use `bicc_table_metadata` to drive, **per tab**, a **Primary-Key-only** validation:
# MAGIC    duplicate PK, NOT-NULL PK, datatype PK.  (column-count / record-count / non-PK checks are removed)
# MAGIC 5. Build the **curated parquet**: keep PK column(s) as-is, fold every non-PK column into one
# MAGIC    JSON `DATA` column, add the standard metadata columns.
# MAGIC 6. Bad records  →  `bicc_ingestion_err_table`.  One control row per tab  →  `bicc_process_control`.
# MAGIC 7. Any tab failing  →  failure e-mail. A failure in one tab does **not** stop the other tabs.

# COMMAND ----------

# DBTITLE 1,Install libraries (python-gnupg for decrypt, python-calamine for excel - NOT openpyxl)
import sys, subprocess, pkg_resources

required = {'python-gnupg', 'python-calamine'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

# COMMAND ----------

# DBTITLE 1,All imports
import gnupg
import json, os, re, base64, uuid, smtplib
from datetime import datetime
from pprint import pprint

import pandas as pd
from python_calamine import CalamineWorkbook

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lit, when, expr, count, concat, concat_ws, current_timestamp,
    to_json, struct, regexp_replace, sum as _sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, ArrayType, LongType
)
from pyspark.storagelevel import StorageLevel

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid

# COMMAND ----------

# DBTITLE 1,Widgets - ALL required inputs live here at the top
# execution_id    : run id from the orchestrator (auto-generated if blank, e.g. for local testing)
# notebook_config : the JSON config (mirrors the config table) - see next cell for the schema
dbutils.widgets.text('execution_id', '')
dbutils.widgets.text('notebook_config', '')

# COMMAND ----------

# DBTITLE 1,Parse config + global constants (everything configurable lives in the config table / JSON)
# ------------------------------------------------------------------------------------------------
# Sample notebook_config (store this in the config table, same pattern as the old CSV job):
# {
#   "storage":    {"adls_container":"raw","folder":"erp","source":"bicc_INT_680","frequency":"hourly"},
#   "decryption": {"private_key":"${erp_private_key}","scope_nm":"${erp_keyvault_scope_nm}",
#                  "passphrase_key":"${erp_passphrase_key}"},
#   "processing": {"decrypt_flag":"false",
#                  "test_excel_path":"dbfs:/FileStore/erp_bicc_test/Rogers_Shaw_STB_OHB_Comparison_2026.06.10.xlsx",
#                  "header_row_index":"1", "data_start_row_index":"2"},
#   "validations":{"metadata_table":"drvd__app_bicc.bicc_table_metadata",
#                  "error_table":"drvd__app_bicc.bicc_ingestion_err_table",
#                  "process_control":"drvd__app_bicc.bicc_process_control"},
#   "email":      {"sender":"${erp_email_sender}","receivers":"${erp_email_receivers}",
#                  "server":"${erp_email_server}","port":"25"}   # NOTE: subject is fixed in code (EMAIL_SUBJECT)
# }
# ------------------------------------------------------------------------------------------------
execution_id = dbutils.widgets.get('execution_id').strip() or f"LOCAL_{datetime.now().strftime('%Y%m%d%H%M%S')}"
notebook_config = json.loads(dbutils.widgets.get('notebook_config'))

# decrypt_flag = "false"  -> TEST mode  : read test_excel_path directly (no GPG)
# decrypt_flag = "true"   -> PROD mode  : GPG-decrypt the dated folder, then read decrypted xlsx
DECRYPT_FLAG     = str(notebook_config['processing'].get('decrypt_flag', 'false')).lower() == 'true'
TEST_EXCEL_PATH  = notebook_config['processing'].get('test_excel_path', '')
# PROD: if source_path is set, the *.xlsx.gpg files are read from EXACTLY this folder (deterministic).
#       if left blank, the code auto-discovers the latest dated folder under storage.source.
SOURCE_PATH      = notebook_config['processing'].get('source_path', '')
# OUTPUT base for parquet. Set this to write DIRECTLY (no dbutils.fs.mounts(), which is blocked on
# Unity Catalog shared/standard clusters). Examples:
#   abfss://raw@<account>.dfs.core.windows.net/erp     (prod, via external location)
#   dbfs:/FileStore/erp_bicc_test/output               (local test, no ADLS creds)
# If left blank, the code falls back to the mount-based fn_getLocation (only works on no-isolation clusters).
OUTPUT_PATH      = notebook_config['processing'].get('output_path', '')
HEADER_ROW_IDX   = int(notebook_config['processing'].get('header_row_index', 1))     # 0-based: 1 = 2nd row
DATA_START_IDX   = int(notebook_config['processing'].get('data_start_row_index', 2)) # 0-based: 2 = 3rd row
# The non-PK "data" column type. "variant" (default) absorbs schema drift (5 cols today, 10 tomorrow)
# without changing the table. "json" = a JSON STRING fallback if the runtime can't write VARIANT to parquet.
DATA_COLUMN_TYPE = notebook_config['processing'].get('data_column_type', 'variant').lower()

METADATA_TABLE   = notebook_config['validations']['metadata_table']
ERROR_TABLE      = notebook_config['validations']['error_table']
PROCESS_CONTROL  = notebook_config['validations']['process_control']

# Curated parquet layout (your change #1):
#   PK_DERIVED (FIRST) , <pk columns...> , <DATA json> , then these trailing metadata columns.
#   Removed from the parquet: BATCH_ID, SHEET_TAB_NAME, PROCESS_ID, EXECUTION_ID
#   (they still live in the control / error tables for traceability).
CURATED_META_COLS = ['FILE_DTTM', 'SOURCE_FILE_NAME', '_AZ_INSERT_TS']

# Arrow makes pandas <-> Spark conversion (the excel read, #7) much faster.
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# E-mail subject + signature are FIXED in code now (removed from the widget/config per request).
EMAIL_SUBJECT = 'ALERT | CTDI Excel Ingestion Failure'   # table name(s) get appended at send time
EMAIL_TEAM    = 'Data & AI Team'
EMAIL_CONTACT = 'prodsuppazure@rci.rogers.com'

# exit_on_finish = "true"  (PROD/job): call dbutils.notebook.exit() so the orchestrator gets the JSON.
# exit_on_finish = "false" (TEST)    : DON'T exit, so the *next cell* runs and can show the step log/summary.
EXIT_ON_FINISH = str(notebook_config['processing'].get('exit_on_finish', 'true')).lower() == 'true'

# Step-by-step run log: every milestone is appended here AND printed live.
# The "next cell" reads RUN_LOG / RUN_CONTROL_DF (populated only when exit_on_finish=false).
RUN_LOG = []
RUN_CONTROL_DF = None
def log_step(msg, level='INFO'):
    tag = '' if level == 'INFO' else f"[{level}] "       # no timestamp; WARN/SKIP keep a tag
    line = f"{tag}{msg}"
    RUN_LOG.append(line)
    print(line)

print(f"Mode={'PROD (decrypt)' if DECRYPT_FLAG else 'TEST (direct xlsx)'} | execution_id={execution_id} "
      f"| exit_on_finish={EXIT_ON_FINISH}")

# COMMAND ----------

# DBTITLE 1,Validate config + tables up-front (#10) - fail fast with a clear message, not a raw KeyError
def validate_config():
    problems = []
    # required config keys
    required = {
        'storage': ['adls_container', 'folder', 'source', 'frequency'],
        'processing': ['decrypt_flag'],
        'validations': ['metadata_table', 'error_table', 'process_control'],
        'email': ['sender', 'receivers', 'server'],   # subject is fixed in code now
    }
    for section, keys in required.items():
        if section not in notebook_config:
            problems.append(f"missing config section '{section}'"); continue
        for k in keys:
            if k not in notebook_config[section]:
                problems.append(f"missing config key '{section}.{k}'")

    # mode-specific keys
    if DECRYPT_FLAG:
        if not (SOURCE_PATH or notebook_config['storage'].get('source')):
            problems.append("PROD mode needs processing.source_path or storage.source")
    else:
        if not TEST_EXCEL_PATH:
            problems.append("TEST mode needs processing.test_excel_path")

    # the three control tables must exist and be readable
    for t in [METADATA_TABLE, ERROR_TABLE, PROCESS_CONTROL]:
        try:
            spark.read.table(t).schema
        except Exception as e:
            problems.append(f"table '{t}' not readable - {str(e).splitlines()[0]}")

    if problems:
        raise Exception("Config/setup validation failed:\n  - " + "\n  - ".join(problems))
    print("Config validation passed.")

validate_config()

# COMMAND ----------

# DBTITLE 1,Util - dbfs:/ <-> /dbfs path helpers (calamine reads from the local FUSE path)
def to_local_path(path):
    """abfss is not directly readable by calamine; dbfs:/ and /mnt are exposed under /dbfs."""
    if path.startswith('dbfs:/'):
        return path.replace('dbfs:/', '/dbfs/', 1)
    if path.startswith('/dbfs/') or path.startswith('/'):
        return path
    return path

# COMMAND ----------

# DBTITLE 1,Util - get mounted abfss / dbfs location for a container+folder (unchanged from CSV job)
def fn_getLocation(adls_container, folder, sub_folder=None):
    try:
        status, errMsg = 0, 'Success'
        mount_point      = f'/mnt/{adls_container}'
        mount_point_prod = f'/mnt/{adls_container}/'
        mount_data = dbutils.fs.mounts()
        mountSchema = StructType([
            StructField('mountPoint', StringType(), True),
            StructField('source', StringType(), True),
            StructField('encryptionType', StringType(), True)
        ])
        df_mount = spark.createDataFrame(data=mount_data, schema=mountSchema)
        abfs_location = df_mount.filter(
            (col('mountPoint') == lit(mount_point)) | (col('mountPoint') == lit(mount_point_prod))
        ).select('source').collect()[0][0]

        if sub_folder is not None:
            dbfs_api_location = f'dbfs:/mnt/{adls_container}/{folder}/{sub_folder}'
            dbfs_location     = f'/dbfs/mnt/{adls_container}/{folder}/{sub_folder}'
            abfs_location     = abfs_location + folder + '/' + sub_folder
        else:
            dbfs_api_location = f'dbfs:/mnt/{adls_container}/{folder}'
            dbfs_location     = f'/dbfs/mnt/{adls_container}/{folder}'
            abfs_location     = abfs_location + folder
    except Exception as e:
        status, abfs_location, dbfs_api_location, dbfs_location = 1, None, None, None
        errMsg = f'Failed to get location (fn_getLocation). Error - {str(e)}'
    finally:
        return status, abfs_location, dbfs_api_location, dbfs_location, errMsg

# COMMAND ----------

# DBTITLE 1,Util - latest dated directory (PROD only)
def get_dir_content(ls_path, level=0, max_depth=2):
    dir_paths = dbutils.fs.ls(ls_path)
    if max_depth is None or level < max_depth:
        subdir_paths = [get_dir_content(p.path, level + 1, max_depth) for p in dir_paths if p.isDir() and p.path != ls_path]
        flat = [p for sub in subdir_paths for p in sub]
    else:
        flat = []
    return list(map(lambda p: p.path, dir_paths)) + flat

def get_latest_directory(file_path, frequency='hourly'):
    depth = {'monthly': 1, 'daily': 2, 'hourly': 3, 'minutely': 4, 'minutes': 4}
    md = depth.get(frequency.lower(), 2)
    paths = get_dir_content(file_path, max_depth=md)
    paths = sorted(list(set([i[:i.rfind('/')] for i in paths])), reverse=True)
    latest = paths[0]
    print(f'****** Latest directory: {latest}')
    return latest

# COMMAND ----------

# DBTITLE 1,Util - decrypt all *.xlsx.gpg files in a folder (PROD only)
def decrypt_gpg_files(src_path, scope_nm, decryption_key, passphrase_key):
    os.system('export GPG_TTY=$(tty)')
    gpg = gnupg.GPG(); gpg.encoding = 'utf-8'

    private_key = base64.b64decode(dbutils.secrets.get(scope=scope_nm, key=decryption_key)).decode()
    passphrase  = dbutils.secrets.get(scope=scope_nm, key=passphrase_key)
    gpg.import_keys(private_key)

    encrypted = dbutils.fs.ls(src_path)
    encrypted = [f.path.replace('dbfs:', '/dbfs', 1) for f in encrypted if f.name.endswith('.gpg')]
    print('encrypted files: ' + json.dumps(encrypted))

    for idx, enc in enumerate(encrypted):
        # NonconformDisposition.2026.06.01.12.32.29.xlsx.gpg -> strip the trailing .gpg only
        dec = enc[:-4] if enc.endswith('.gpg') else enc
        with open(enc, 'rb') as fh:
            status = gpg.decrypt_file(fh, output=dec, passphrase=passphrase)
        print(json.dumps({"progress": f"{idx+1}/{len(encrypted)}", "ok": status.ok, "status": status.status}))
        assert status.ok, f"Decryption failed for {enc}. Error: {status.stderr}"
        # archive the encrypted original
        final = os.path.join(os.path.dirname(enc), '.encrypted', os.path.basename(enc))
        dbutils.fs.mv(enc.replace('/dbfs', 'dbfs:', 1), final.replace('/dbfs', 'dbfs:', 1))

# COMMAND ----------

# DBTITLE 1,Util - derive file_name_prefix + batch_id + file_dttm from the file name
# CTDI names: <Prefix>.YYYY.MM.DD.HH.MM.SS.xlsx  or  <Prefix>_YYYY.MM.DD.xlsx
_DT_RE = re.compile(r'[._](\d{4})\.(\d{2})\.(\d{2})(?:\.(\d{2})\.(\d{2})\.(\d{2}))?')

def derive_file_meta(file_name):
    base = os.path.basename(file_name)
    m = _DT_RE.search(base)
    prefix = base[:m.start()] if m else os.path.splitext(base)[0]
    if m:
        y, mo, d, hh, mm, ss = m.groups()
        hh, mm, ss = hh or '00', mm or '00', ss or '00'
        batch_id = f"{y}{mo}{d}{hh}{mm}{ss}"
        file_dttm = datetime.strptime(f"{y}-{mo}-{d} {hh}:{mm}:{ss}", "%Y-%m-%d %H:%M:%S")
    else:
        batch_id, file_dttm = None, None
    return prefix, batch_id, file_dttm

# COMMAND ----------

# DBTITLE 1,Util - notebook exit
def fn_exitFinalDatabricks(start_time, status, message):
    name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]
    out = {"name": name, "jobStartDate": start_time,
           "messageDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
           "status": status, "message": message}
    if status == 0:
        dbutils.notebook.exit(json.dumps(out, indent=1))
    else:
        pprint(out)
        raise Exception('Job failed. Please debug...')

# COMMAND ----------

# DBTITLE 1,Cache the metadata table once (performance)
# One read of the metadata table for the whole run.
df_metadata_all = spark.read.table(METADATA_TABLE).persist(StorageLevel.MEMORY_AND_DISK)
df_metadata_all.count()  # materialize

def get_sheet_metadata(file_prefix, sheet_tab_name):
    """Return the PK-only metadata rows for one (file_prefix, sheet_tab). [] if not configured.
       Metadata now holds ONLY the primary-key columns; order by serial_number keeps the
       composite PK_DERIVED deterministic across runs."""
    rows = (df_metadata_all
            .filter((col('file_name_prefix') == lit(file_prefix)) &
                    (col('sheet_tab_name')   == lit(sheet_tab_name)))
            .orderBy(col('serial_number'))
            .collect())
    return rows

# COMMAND ----------

# DBTITLE 1,Read ALL sheets of one workbook with calamine -> list of (sheet_name, spark_df_as_string)
def cell_to_str(v):
    """Stringify a calamine cell safely.
       Excel numbers come back as float -> an integer-valued float like 12345.0 must become '12345',
       otherwise a numeric PK would fail the bigint datatype check."""
    if v is None:
        return None
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else repr(v)
    s = str(v)
    return None if s == '' else s

def read_excel_sheets(local_path):
    """
    Reads every tab. Header is taken from HEADER_ROW_IDX (2nd row for CTDI files, which have a
    title banner on row 1). Everything is read as STRING - we cast later only for PK datatype checks.
    Returns: list of (sheet_name, df_string_or_None, raw_row_count)
    """
    wb = CalamineWorkbook.from_path(local_path)
    results = []
    for sheet_name in wb.sheet_names:
        rows = wb.get_sheet_by_name(sheet_name).to_python(skip_empty_area=True)
        if len(rows) <= HEADER_ROW_IDX:
            results.append((sheet_name, None, 0))
            continue

        # ---- build clean, unique header names ----
        header, seen = [], {}
        for i, h in enumerate(rows[HEADER_ROW_IDX]):
            name = str(h).strip() if h not in (None, '') else f'col_{i}'
            if name in seen:
                seen[name] += 1
                name = f'{name}_{seen[name]}'
            else:
                seen[name] = 0
            header.append(name)
        ncol = len(header)

        # ---- normalise data rows to header width, stringify cells ----
        data = rows[DATA_START_IDX:] if len(rows) > DATA_START_IDX else []
        norm = []
        for r in data:
            r = list(r)
            r = (r + [None] * (ncol - len(r))) if len(r) < ncol else r[:ncol]
            norm.append([cell_to_str(v) for v in r])

        # #7 perf: go via pandas + Arrow (faster + lower-overhead than createDataFrame(list))
        schema = StructType([StructField(c, StringType(), True) for c in header])
        pdf = pd.DataFrame(norm, columns=header, dtype=object)
        pdf = pdf.where(pd.notnull(pdf), None)            # NaN -> None so Arrow writes proper nulls
        df = spark.createDataFrame(pdf, schema=schema)
        results.append((sheet_name, df, len(norm)))
    return results

# COMMAND ----------

# DBTITLE 1,Rename sheet columns -> original (final) column names from metadata
def apply_column_mapping(df_sheet, meta_rows):
    """Metadata now holds ONLY the PK columns. Rename each PK sheet column to its final
       (original) name; keep EVERY other sheet column as-is (string) for the VARIANT bucket.
       Returns (df, pk_final_cols, non_pk_cols)."""
    rename, pk_final = {}, []
    for r in meta_rows:
        if not r['is_primary_key']:                 # only PK rows are renamed/validated
            continue
        s_name = r['sheet_column_name']
        f_name = r['original_column_name'] or s_name
        rename[s_name] = f_name
        pk_final.append(f_name)
    select_exprs = [col(f'`{c}`').alias(rename.get(c, c)) for c in df_sheet.columns]
    df = df_sheet.select(*select_exprs)
    non_pk = [c for c in df.columns if c not in pk_final]
    return df, pk_final, non_pk

# COMMAND ----------

# DBTITLE 1,Primary-Key-only validations: duplicate + NOT NULL + datatype  (NO count / non-PK checks)
def run_pk_validations(df, pk_cols, pk_types):
    """
    ALL three PK checks run for EVERY record - no early exit, reasons accumulate (your change #3).
    Each check is an explicit boolean flag column (#4) so the counts never depend on message text.
    Returns (df_with_COMMENTS, dq_check_validation_array, counts_dict).
    """
    # 1) Derived PK (md5 of all PK columns) - composite-PK safe
    parts = []
    for c in pk_cols:
        parts.append(f"nvl(cast(`{c}` as string), '~')"); parts.append("'!@~'")
    pk_expr = "md5(concat(" + ", ".join(parts) + ", '~'))" if pk_cols else "md5('~')"
    df = df.withColumn('PK_DERIVED', expr(pk_expr))

    # 2) NOT-NULL flag  (any PK column null/blank)
    null_cond = " OR ".join([f"(`{c}` IS NULL OR trim(`{c}`) = '')" for c in pk_cols]) if pk_cols else "false"
    df = df.withColumn('_F_NULL', expr(null_cond))

    # 3) DATATYPE flag (non-string PK fails try_cast)
    dtype_conds = [
        f"(`{c}` IS NOT NULL AND trim(`{c}`) <> '' AND try_cast(`{c}` AS {t}) IS NULL)"
        for c, t in zip(pk_cols, pk_types) if t.lower() not in ('string', 'varchar', 'char')
    ]
    df = df.withColumn('_F_DTYPE', expr(" OR ".join(dtype_conds)) if dtype_conds else lit(False))

    # 4) DUPLICATE flag
    df = df.withColumn('_F_DUP', count(lit(1)).over(Window.partitionBy('PK_DERIVED')) > 1)

    # COMMENTS built from the flags (all reasons accumulate)
    df = df.withColumn('COMMENTS', concat_ws('',
            when(col('_F_NULL'),  lit('Mandatory PK field is NULL || ')).otherwise(lit('')),
            when(col('_F_DTYPE'), lit('PK datatype mismatch || ')).otherwise(lit('')),
            when(col('_F_DUP'),   lit('Duplicate Primary Key || ')).otherwise(lit('')))) \
           .withColumn('_F_BAD', col('COMMENTS') != lit(''))
    df.persist(StorageLevel.DISK_ONLY)

    # counts in a single pass, summing the boolean flags (cast to int)
    agg = df.agg(
        count(lit(1)).alias('total'),
        _sum(col('_F_DUP').cast('int')).alias('dup'),
        _sum(col('_F_NULL').cast('int')).alias('nul'),
        _sum(col('_F_DTYPE').cast('int')).alias('dty'),
        _sum(col('_F_BAD').cast('int')).alias('bad'),
    ).collect()[0]
    counts = {k: (agg[k] or 0) for k in ['total', 'dup', 'nul', 'dty', 'bad']}

    df = df.drop('_F_NULL', '_F_DTYPE', '_F_DUP', '_F_BAD')

    dq_array = [
        f"Duplicate PK Check : {'PASS' if counts['dup'] == 0 else 'FAIL'} | duplicate_records={counts['dup']}",
        f"Not Null PK Check : {'PASS' if counts['nul'] == 0 else 'FAIL'} | null_records={counts['nul']}",
        f"Data Type Check : {'PASS' if counts['dty'] == 0 else 'FAIL'} | mismatch_records={counts['dty']}",
    ]
    return df, dq_array, counts

# COMMAND ----------

# DBTITLE 1,Write bad records into the error table (same process_id for the whole tab)
def insert_error_records(df_bad, process_id, file_name, table_name, sheet_tab_name, file_path, pk_cols):
    """Writes one detailed row per rejected record.  error_category = the failed check(s),
       error_description = a full human-readable sentence, pk_value = the offending PK value(s),
       err_record = the entire bad row as JSON."""
    err_cols = spark.read.table(ERROR_TABLE).columns

    category = regexp_replace(col('COMMENTS'), r'\s*\|\|\s*$', '')          # trim trailing " || "
    pk_value = concat_ws(' | ', *[concat(lit(f'{c}='), col(f'`{c}`').cast('string')) for c in pk_cols]) \
        if pk_cols else lit('(no PK configured)')

    df_err = (df_bad
              .withColumn('err_record', to_json(struct(col('*'))))          # full bad row first
              .withColumn('error_category', category)
              .withColumn('error_description',
                          concat(lit('Record rejected during primary-key validation. Reason(s): '),
                                 category,
                                 lit('. Primary key -> '), pk_value,
                                 lit('. Derived key = '), col('PK_DERIVED'),
                                 lit('. This record was NOT ingested into the curated dataset.')))
              .withColumn('pk_value', pk_value)
              .withColumn('pk_derived', col('PK_DERIVED'))
              .withColumn('process_id', lit(process_id))
              .withColumn('execution_id', lit(execution_id))
              .withColumn('file_name', lit(file_name))
              .withColumn('table_name', lit(table_name))
              .withColumn('sheet_tab_name', lit(sheet_tab_name))
              .withColumn('file_path', lit(file_path))
              .withColumn('comments', col('COMMENTS'))
              .withColumn('az_insert_ts', current_timestamp()))
    df_err.select(*err_cols).write.insertInto(ERROR_TABLE, overwrite=False)

# COMMAND ----------

# DBTITLE 1,Build the curated DF: PK as-is + all non-PK folded into one JSON column + metadata columns
def build_curated_df(df_good, non_pk_cols, pk_cols, pk_types, array_col, file_dttm, source_file_name):
    # PK columns kept as-is but cast to their declared datatype
    pk_select = [col(f'`{c}`').cast(t).alias(c) for c, t in zip(pk_cols, pk_types)]

    # every non-PK column (kept as string) -> ONE semi-structured column.
    #   VARIANT (default): absorbs schema drift - new/removed columns need no DDL or metadata change.
    #   JSON   (fallback): a JSON string, for runtimes that can't write VARIANT to parquet.
    if non_pk_cols:
        struct_sql = "struct(" + ", ".join(f"`{c}`" for c in non_pk_cols) + ")"
        data_col = (expr(f"parse_json(to_json({struct_sql}))") if DATA_COLUMN_TYPE == 'variant'
                    else expr(f"to_json({struct_sql})")).alias(array_col)
    else:
        data_col = (expr("parse_json('{}')") if DATA_COLUMN_TYPE == 'variant'
                    else lit('{}')).alias(array_col)

    # PK_DERIVED FIRST, then PK cols, then the VARIANT/JSON data col, then trailing metadata cols
    df = df_good.select(col('PK_DERIVED'), *pk_select, data_col) \
                .withColumn('FILE_DTTM', lit(file_dttm).cast(TimestampType())) \
                .withColumn('SOURCE_FILE_NAME', lit(source_file_name)) \
                .withColumn('_AZ_INSERT_TS', current_timestamp())

    final_order = ['PK_DERIVED'] + pk_cols + [array_col] + CURATED_META_COLS
    return df.select(*final_order)

# COMMAND ----------

# DBTITLE 1,Write a dataframe out as a single .parquet file
def write_parquet(target_folder, table_name, df):
    tmp = f"{target_folder}/{table_name}_tmp"
    final = f"{target_folder}/{table_name}.parquet"
    df.coalesce(1).write.mode('overwrite').parquet(tmp)   # #8: coalesce avoids a full shuffle
    part = [f.path for f in dbutils.fs.ls(tmp) if f.path.endswith('.parquet')][0]
    try:
        dbutils.fs.rm(final)          # idempotent for re-runs in the same dated folder
    except Exception:
        pass
    dbutils.fs.mv(part, final)
    dbutils.fs.rm(tmp, recurse=True)
    return final

# COMMAND ----------

# DBTITLE 1,Resolve the curated/raw output folder for a table (container/folder/table/yyyy/mm/dd/HH)
def resolve_output_folder(table_name, kind='curated'):
    frequency = notebook_config['storage']['frequency']
    now = datetime.now()
    parts = {'monthly': ['%Y', '%m'], 'daily': ['%Y', '%m', '%d'],
             'hourly': ['%Y', '%m', '%d', '%H'], 'minutes': ['%Y', '%m', '%d', '%H', '%M']}
    dated = '/'.join(now.strftime(p) for p in parts.get(frequency.lower(), ['%Y', '%m', '%d']))
    sub = table_name if kind == 'curated' else f"{table_name}/_raw_asis"

    if OUTPUT_PATH:
        # direct write - NO dbutils.fs.mounts() (works on Unity Catalog shared/standard clusters)
        return f"{OUTPUT_PATH.rstrip('/')}/{sub}/{dated}"

    # fallback: mount-based (needs dbutils.fs.mounts() -> only no-isolation clusters)
    adls_container = notebook_config['storage']['adls_container']
    folder         = notebook_config['storage']['folder']
    status, abfs_location, _, _, errMsg = fn_getLocation(adls_container, folder, sub)
    if status != 0:
        raise Exception(errMsg)
    return f"{abfs_location}/{dated}"

# COMMAND ----------

# DBTITLE 1,E-mail helpers
def fn_sendEmail(sender, server, receivers, subject, html, port=25):
    """Returns refused_dict ({} == relay accepted for all recipients)."""
    msg = MIMEMultipart('alternative', None, [MIMEText('Please view this e-mail in HTML.'),
                                              MIMEText(html, 'html')])
    msg['Subject'] = subject
    msg['From'] = sender
    rcpts = receivers if isinstance(receivers, list) else [r.strip() for r in str(receivers).split(',') if r.strip()]
    msg['To'] = ', '.join(rcpts)
    msg['Date'] = formatdate(localtime=True)                      # proper headers reduce spam filtering
    msg['Message-ID'] = make_msgid(domain=sender.split('@')[-1])

    s = smtplib.SMTP(server, int(port), timeout=30)              # timeout so a bad host fails fast, not hangs
    s.ehlo()
    refused = s.sendmail(sender, rcpts, msg.as_string())
    s.quit()
    return refused

def _esc(v):
    """Minimal HTML escaping."""
    return (str(v) if v is not None else '-').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def _build_failure_html(failed_rows, exec_id):
    """High-standard corporate alert e-mail (no emoji): accent strip -> dark header with severity pill
       -> callout -> run-info panel -> zebra error table -> query box -> footer."""
    wrap = 'word-break:break-word;overflow-wrap:anywhere;'
    th  = f'border:1px solid #9a1b2e;background:#b00020;color:#ffffff;padding:9px 10px;text-align:center;font-weight:600;{wrap}'
    tdl = f'border:1px solid #e4e4e4;padding:8px 10px;text-align:left;vertical-align:top;{wrap}'
    tdc = f'border:1px solid #e4e4e4;padding:8px 10px;text-align:center;vertical-align:top;{wrap}'
    tdr = f'border:1px solid #e4e4e4;padding:8px 10px;text-align:center;vertical-align:top;color:#b00020;font-weight:700;{wrap}'

    rows_html = ""
    for i, r in enumerate(failed_rows):
        bg = '#ffffff' if i % 2 == 0 else '#f7f8f9'
        # short, clean reason for the e-mail (drop the table-name / path tail that overflows)
        reason = (r['comments'] or '').split(' -> ')[0].split('. Sheet columns')[0].strip()
        rows_html += (
            f"<tr style='background:{bg};'>"
            f"<td style='{tdl}'>{_esc(r['file_name'])}</td>"
            f"<td style='{tdl}'><b>{_esc(r['table_name'])}</b>"
            f"<br><span style='font-size:11px;color:#888;'>{_esc(r['sheet_tab_name'])}</span></td>"
            f"<td style='{tdl}color:#b00020;'>{_esc(reason)}</td>"
            f"<td style='{tdc}'>{r['source_row_count']}</td>"
            f"<td style='{tdr}'>{r['error_record_count']}</td>"
            f"<td style='{tdc}'>{r['valid_record_count']}</td>"
            "</tr>"
        )

    query = f"SELECT * FROM {ERROR_TABLE} WHERE execution_id = '{exec_id}';"
    run_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def info(label, value, vcolor='#1f2a36'):
        return (f"<tr><td style='padding:7px 12px;border-bottom:1px solid #ededed;font-size:12px;color:#8a8a8a;width:140px;'>{label}</td>"
                f"<td style='padding:7px 12px;border-bottom:1px solid #ededed;font-size:13px;color:{vcolor};font-weight:600;'>{value}</td></tr>")

    return f"""
    <div style="font-family:Segoe UI,Arial,sans-serif;color:#222;width:100%;max-width:860px;border:1px solid #e0e0e0;border-radius:6px;overflow:hidden;">

      <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
        <tr><td bgcolor="#b00020" style="height:4px;line-height:4px;font-size:0;">&nbsp;</td></tr>
      </table>

      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" bgcolor="#1f2a36">
        <tr>
          <td bgcolor="#1f2a36" style="padding:16px 22px;">
            <div style="color:#ffffff;font-size:19px;font-weight:600;letter-spacing:.3px;">Data Ingestion Alert</div>
            <div style="color:#9fb0c0;font-size:12px;margin-top:3px;">ERP &middot; CTDI Excel Ingestion &middot; Automated Notification</div>
          </td>
          <td bgcolor="#1f2a36" align="right" style="padding:16px 22px;">
            <span style="background:#b00020;color:#ffffff;font-size:11px;font-weight:700;letter-spacing:.6px;padding:5px 12px;border-radius:3px;">CRITICAL</span>
          </td>
        </tr>
      </table>

      <div style="padding:18px 22px;">
        <div style="background:#fdecea;border-left:4px solid #b00020;padding:12px 16px;color:#7a1c12;font-size:13px;">
          <b>Data validation irregularities have been detected during ingestion of one or more CTDI Excel feeds.</b>
          This condition may impact downstream data reliability and requires immediate attention.
        </div>

        <table role="presentation" cellpadding="0" cellspacing="0" style="margin:18px 0 4px;border-collapse:collapse;">
          {info('Execution ID', _esc(exec_id))}
          {info('Run Timestamp', run_date + ' UTC')}
          {info('Failed Feeds', str(len(failed_rows)), '#b00020')}
          {info('Control Table', PROCESS_CONTROL)}
          {info('Error Table', ERROR_TABLE)}
        </table>

        <div style="font-weight:600;margin:20px 0 8px;font-size:14px;color:#1f2a36;">Error summary</div>
        <table cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-size:13px;width:100%;table-layout:fixed;">
          <tr>
            <th style="{th}width:24%;">File Name</th>
            <th style="{th}width:18%;">Table / Sheet</th>
            <th style="{th}width:28%;">Error Reason</th>
            <th style="{th}width:10%;">Total</th>
            <th style="{th}width:10%;">Rejected</th>
            <th style="{th}width:10%;">Ingested</th>
          </tr>
          {rows_html}
        </table>

        <div style="font-weight:600;margin:22px 0 6px;font-size:14px;color:#1f2a36;">Next step</div>
        <div style="font-size:13px;color:#444;margin-bottom:8px;">For detailed record-level errors, query the ingestion error table:</div>
        <div style="background:#0f1b2a;color:#e6edf3;border-radius:4px;padding:11px 14px;font-family:Consolas,'Courier New',monospace;font-size:13px;">{_esc(query)}</div>

        <div style="border-top:1px solid #e2e2e2;margin:24px 0 12px;"></div>
        <div style="font-size:13px;color:#222;">Thanks,<br><b>{EMAIL_TEAM}</b></div>
        <div style="font-size:11px;color:#8a8a8a;margin-top:14px;line-height:1.6;">
          This is an automated notification from the ED&amp;A ingestion framework. Please do not reply to this email.<br>
          For assistance, contact <b>Prod Support &ndash; Azure</b> &mdash;
          <a href="mailto:{EMAIL_CONTACT}" style="color:#1f6feb;text-decoration:none;">{EMAIL_CONTACT}</a>
        </div>
      </div>
    </div>"""

def send_failure_email(df_control):
    """ONE consolidated mail for ALL failed sheets/files in this run. Returns a status string."""
    cfg = notebook_config['email']
    # collect every failed tab/file across the whole run -> a single e-mail
    failed_rows = df_control.filter(col('final_ingestion_status') != lit('Succeeded')) \
                            .select('file_name', 'table_name', 'sheet_tab_name',
                                    'source_row_count', 'valid_record_count', 'error_record_count',
                                    'comments').collect()
    if not failed_rows:
        return 'No failures -> no e-mail sent'

    # subject includes the failed table name(s) (your request)
    tables  = sorted({(r['table_name'] or '-') for r in failed_rows})
    tbl_str = ', '.join(tables[:4]) + (' …' if len(tables) > 4 else '')
    subject = f"{EMAIL_SUBJECT} | Table(s): {tbl_str} | {len(failed_rows)} failed | " \
              f"Run date - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC"
    html = _build_failure_html(failed_rows, execution_id)
    server = cfg.get('server', '')
    # "email not sent" fix: only an unresolved ${placeholder} or blank host is skipped, and we say so loudly.
    if (not server) or server.startswith('${'):
        return ("EMAIL NOT SENT: email.server is blank/placeholder - "
                "set config.email.server to a reachable SMTP host (e.g. 10.9.40.62)")
    try:
        refused = fn_sendEmail(cfg['sender'], server, cfg['receivers'], subject, html, cfg.get('port', 25))
        if refused:
            return f'EMAIL NOT DELIVERED: relay {server} refused {refused}'
        return f'Failure e-mail sent to {cfg["receivers"]} ({len(failed_rows)} failed sheet/file)'
    except Exception as e:
        # do NOT crash the job for an e-mail problem - report it concisely
        return f'EMAIL NOT SENT: {e}'

# COMMAND ----------

# DBTITLE 1,Build the list of workbooks to process (TEST = direct path | PROD = decrypted dated folder)
def list_workbooks():
    """Returns list of (abfss_or_dbfs_path, local_path, file_name)."""
    if not DECRYPT_FLAG:
        # TEST MODE - read the provided xlsx directly, no GPG.
        p = TEST_EXCEL_PATH
        return [(p, to_local_path(p), os.path.basename(p))]

    # PROD MODE - decrypt *.xlsx.gpg, then list *.xlsx
    adls_container = notebook_config['storage']['adls_container']
    folder         = notebook_config['storage']['folder']
    source         = notebook_config['storage']['source']
    frequency      = notebook_config['storage']['frequency']
    dec            = notebook_config['decryption']

    if SOURCE_PATH:
        # deterministic: read exactly from the folder you placed the file in (must be a dbfs:/ path)
        latest_dbfs = SOURCE_PATH
        print(f'Using explicit source_path: {latest_dbfs}')
    else:
        # auto-discover the latest dated folder under /mnt/<container>/<folder>/<source>
        _, abfs_loc, dbfs_api_loc, _, errMsg = fn_getLocation(adls_container, folder, source)
        latest_dbfs = get_latest_directory(dbfs_api_loc, frequency)
    decrypt_gpg_files(latest_dbfs, dec['scope_nm'], dec['private_key'], dec['passphrase_key'])

    files = dbutils.fs.ls(latest_dbfs)
    wbs = []
    for f in files:
        if f.name.lower().endswith('.xlsx'):
            wbs.append((f.path, f.path.replace('dbfs:/', '/dbfs/', 1), f.name))
    return wbs

# COMMAND ----------

# DBTITLE 1,MAIN - per workbook, per tab: validate -> curate -> parquet -> control row (isolated)
NOT_RUN_DQ = ['Duplicate PK Check : NOT RUN', 'Not Null PK Check : NOT RUN', 'Data Type Check : NOT RUN']

def write_control_rows(control_rows):
    control_schema = StructType([
        StructField('execution_id', StringType()),  StructField('process_id', StringType()),
        StructField('batch_id', StringType()),       StructField('file_name', StringType()),
        StructField('sheet_tab_name', StringType()), StructField('table_name', StringType()),
        StructField('source_file_path', StringType()), StructField('raw_parquet_path', StringType()),
        StructField('final_parquet_source_raw', StringType()),
        StructField('dq_check_validation', ArrayType(StringType())),
        StructField('source_row_count', LongType()), StructField('valid_record_count', LongType()),
        StructField('error_record_count', LongType()),
        StructField('final_ingestion_status', StringType()), StructField('comments', StringType()),
        StructField('file_dttm', TimestampType()),
    ])
    df_control = spark.createDataFrame(control_rows, control_schema) \
                      .withColumn('_az_insert_ts', current_timestamp())
    ctrl_cols = spark.read.table(PROCESS_CONTROL).columns
    df_control.select(*ctrl_cols).write.insertInto(PROCESS_CONTROL, overwrite=False)
    return df_control


if __name__ == '__main__':
    final_status, final_message = 0, 'Success'
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    control_rows = []

    try:
        log_step(f'STEP 1 - Job start ({start_time}) | mode={"PROD" if DECRYPT_FLAG else "TEST"}')
        workbooks = list_workbooks()
        log_step(f'STEP 2 - Workbooks to process: {[w[2] for w in workbooks]}')

        for src_path, local_path, file_name in workbooks:
            file_prefix, batch_id, file_dttm = derive_file_meta(file_name)
            log_step(f'STEP 3 - Reading workbook: {file_name} | prefix={file_prefix} | batch_id={batch_id}')

            # #1 FILE-LEVEL isolation: a corrupt/unreadable workbook becomes ONE failed control row,
            #    we e-mail it and keep going - it never wipes other workbooks' results.
            try:
                sheets = read_excel_sheets(local_path)
            except Exception as e:
                msg = f'Workbook could not be read - {str(e)}'
                print(f'  !! {msg}')
                control_rows.append((execution_id, str(uuid.uuid4()), batch_id, file_name, None, None,
                                     src_path, None, None, NOT_RUN_DQ, 0, 0, 0, 'Failed', msg, file_dttm))
                continue

            for sheet_name, df_sheet, raw_cnt in sheets:
                meta_rows = get_sheet_metadata(file_prefix, sheet_name)
                if not meta_rows:
                    log_step(f"tab '{sheet_name}' : no metadata configured -> skipped", level='SKIP')
                    continue

                process_id = str(uuid.uuid4())
                table_name = (meta_rows[0]['table_name'] or '').lower()
                array_col  = meta_rows[0]['array_column_name'] or 'DATA'
                # only PK rows are validated (works whether metadata is PK-only or still has all columns)
                pk_meta       = [r for r in meta_rows if r['is_primary_key']]
                pk_sheet_cols = [r['sheet_column_name'] for r in pk_meta]
                pk_cols       = [r['original_column_name'] or r['sheet_column_name'] for r in pk_meta]
                pk_types      = [r['data_type'] for r in pk_meta]

                log_step(f"STEP 4 - tab '{sheet_name}' -> table '{table_name}' | rows={raw_cnt} | PK={pk_cols}")

                raw_path, final_path, status_txt, comments = None, None, 'Failed', ''
                dq_array, valid_cnt, err_cnt = list(NOT_RUN_DQ), 0, 0
                df_sheet_persisted, df_val = None, None

                try:
                    if df_sheet is None or raw_cnt == 0:
                        raise Exception('No data rows found in the sheet.')

                    # #5: configured PK column must actually exist in the sheet - fail with a CLEAR reason
                    sheet_cols = set(df_sheet.columns)
                    missing_pk = [c for c in pk_sheet_cols if c not in sheet_cols]
                    if missing_pk:
                        raise Exception(f"Configured PK column(s) {missing_pk} not found in sheet "
                                        f"'{sheet_name}'. Sheet columns: {sorted(sheet_cols)}")

                    # #9: persist the sheet once - it is used by the raw write, mapping and validation
                    df_sheet_persisted = df_sheet.persist(StorageLevel.DISK_ONLY)

                    # (a) land the sheet AS-IS as raw parquet (lineage)
                    raw_folder = resolve_output_folder(table_name, kind='raw')
                    raw_path = write_parquet(raw_folder, table_name, df_sheet_persisted)
                    log_step(f"   STEP 4a - raw as-is parquet  -> {raw_path}")

                    # (b) rename PK -> final names; every other sheet column stays for the VARIANT bucket
                    df_mapped, pk_cols, non_pk_cols = apply_column_mapping(df_sheet_persisted, meta_rows)
                    log_step(f"   STEP 4b - {len(pk_cols)} PK col(s) + {len(non_pk_cols)} data col(s) -> VARIANT")

                    # (c) PK-only validations (all checks run for every record)
                    df_val, dq_array, counts = run_pk_validations(df_mapped, pk_cols, pk_types)
                    valid_cnt = counts['total'] - counts['bad']
                    err_cnt   = counts['bad']
                    log_step(f"   STEP 4c - PK checks done | dup={counts['dup']} null={counts['nul']} "
                             f"dtype={counts['dty']} -> bad={err_cnt}, good={valid_cnt}")

                    # (d) bad records -> error table (detailed)
                    if err_cnt > 0:
                        insert_error_records(df_val.where(col('COMMENTS') != lit('')),
                                             process_id, file_name, table_name, sheet_name, src_path, pk_cols)
                        log_step(f"   STEP 4d - {err_cnt} bad record(s) -> {ERROR_TABLE}", level='WARN')

                    # (e) good records -> curated parquet (PK_DERIVED first + PK + VARIANT data + meta cols)
                    df_good = df_val.where(col('COMMENTS') == lit('')).drop('COMMENTS')
                    df_curated = build_curated_df(df_good, non_pk_cols, pk_cols, pk_types,
                                                  array_col, file_dttm, file_name)
                    cur_folder = resolve_output_folder(table_name, kind='curated')
                    final_path = write_parquet(cur_folder, table_name, df_curated)
                    log_step(f"   STEP 4e - curated parquet    -> {final_path}")

                    if err_cnt == 0:
                        status_txt = 'Succeeded'
                        comments   = 'All primary-key checks passed.'
                    else:
                        status_txt = 'Failed'   # reported + e-mailed, but the JOB still succeeds (your change #2)
                        comments   = (f"{err_cnt} record(s) failed PK validation -> {ERROR_TABLE}. "
                                      f"Good records ({valid_cnt}) written to {final_path}.")

                except Exception as e:
                    status_txt = 'Failed'
                    comments   = f'Tab processing failed - {str(e)}'
                    print(f"    !! {comments}")
                finally:
                    if df_val is not None:
                        df_val.unpersist()
                    if df_sheet_persisted is not None:
                        df_sheet_persisted.unpersist()

                control_rows.append((
                    execution_id, process_id, batch_id, file_name, sheet_name, table_name,
                    src_path, raw_path, final_path, dq_array,
                    int(raw_cnt), int(valid_cnt), int(err_cnt), status_txt, comments, file_dttm
                ))

    except Exception as e:
        # only batch-level / infra problems (e.g. list_workbooks) reach here -> the job genuinely fails
        final_status, final_message = 1, 'Failed | Please debug - Error - ' + str(e)

    finally:
        # always persist whatever we processed + e-mail any failures (your change #2: no job failure on DQ issues)
        try:
            if control_rows:
                df_control = write_control_rows(control_rows)
                globals()['RUN_CONTROL_DF'] = df_control          # expose to the next cell
                log_step(f'STEP 5 - {len(control_rows)} control row(s) written to {PROCESS_CONTROL}')

                # tuple layout: 3=file_name 4=sheet 5=table 12=err_cnt 13=status 14=comments
                failed = [r for r in control_rows if r[13] != 'Succeeded']
                n_ok, n_failed = len(control_rows) - len(failed), len(failed)

                email_status = send_failure_email(df_control)     # ONE mail for ALL failures
                log_step(f'STEP 6 - {email_status}')

                # ---- human-readable summary (also captured in RUN_LOG for the next cell) ----
                log_step('=' * 90)
                log_step(f'RUN SUMMARY : {n_ok} succeeded | {n_failed} FAILED')
                for r in control_rows:
                    log_step(f"  [{r[13]:9}] file={r[3]} | tab={r[4]} | table={r[5]} | "
                             f"rows={r[10]} valid={r[11]} err={r[12]} | {r[14]}")
                log_step('=' * 90)

                # ---- surface the failures in the EXIT message (job still SUCCESS) ----
                if final_status == 0:
                    if n_failed:
                        details = " ;; ".join(
                            f"{r[3]} -> {r[5] or r[4] or '-'} : {(r[14] or '').strip()}" for r in failed[:15])
                        final_message = (f"Completed with FAILURES | {n_ok} ok, {n_failed} failed | "
                                         f"{details} | EMAIL: {email_status}")
                    else:
                        final_message = f"Success | {n_ok} sheet(s) ingested, 0 failed | EMAIL: {email_status}"
            else:
                if final_status == 0:
                    final_message = 'Success | No configured tabs were found to process.'
        except Exception as e:
            final_status, final_message = 1, 'Failed writing control table / e-mail - ' + str(e)

        df_metadata_all.unpersist()
        globals()['RUN_FINAL'] = {'status': final_status, 'message': final_message}

        if EXIT_ON_FINISH:
            # PROD/job: hand the JSON to the orchestrator (this STOPS the notebook - no later cell runs).
            fn_exitFinalDatabricks(start_time, final_status, final_message)
        else:
            # TEST: do NOT exit, so the next cell can show the step-by-step log + summary.
            log_step("exit_on_finish=false -> notebook NOT exited; run the NEXT cell for the step log & summary.")
            print(json.dumps({'status': final_status, 'message': final_message}, indent=1))

# COMMAND ----------

# DBTITLE 1,NEXT CELL - step-by-step run log + summary (runs only when exit_on_finish=false)
# This cell only executes when the main cell did NOT call dbutils.notebook.exit()
# (i.e. processing.exit_on_finish = "false"). In PROD the notebook exits above and this cell is skipped.

print("================  STEP-BY-STEP RUN LOG  ================")
for line in RUN_LOG:
    print(line)

print("\n================  FINAL RESULT  ================")
print(json.dumps(RUN_FINAL, indent=1))

# Per-tab control rows for this run (status, counts, dq array, parquet paths)
if RUN_CONTROL_DF is not None:
    print("\n================  PROCESS CONTROL (this run)  ================")
    display(RUN_CONTROL_DF.select(
        'file_name', 'sheet_tab_name', 'table_name', 'final_ingestion_status',
        'source_row_count', 'valid_record_count', 'error_record_count',
        'dq_check_validation', 'comments', 'final_parquet_source_raw'))

    # Show the bad records written to the error table for this execution_id
    print("\n================  ERROR RECORDS (this run)  ================")
    display(spark.read.table(ERROR_TABLE).where(col('execution_id') == lit(execution_id)))
else:
    print("No control rows were produced (no configured tabs matched the metadata).")
