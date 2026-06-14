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

from python_calamine import CalamineWorkbook

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lit, when, expr, count, concat_ws, current_timestamp,
    to_json, struct, sum as _sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, ArrayType, LongType
)
from pyspark.storagelevel import StorageLevel

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
#                  "server":"${erp_email_server}","subject":"Failed | ERP - BICC Excel File INT 680"}
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

METADATA_TABLE   = notebook_config['validations']['metadata_table']
ERROR_TABLE      = notebook_config['validations']['error_table']
PROCESS_CONTROL  = notebook_config['validations']['process_control']

# Fixed metadata-column names written into every curated parquet (the "as usual" audit columns)
META_COLS = ['PK_DERIVED', 'BATCH_ID', 'FILE_DTTM', 'SHEET_TAB_NAME',
             'SOURCE_FILE_NAME', 'PROCESS_ID', 'EXECUTION_ID', '_AZ_INSERT_TS']

print(f"Mode={'PROD (decrypt)' if DECRYPT_FLAG else 'TEST (direct xlsx)'} | execution_id={execution_id}")

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
    """Return ordered list of metadata rows for one (file_prefix, sheet_tab). [] if not configured."""
    rows = (df_metadata_all
            .filter((col('file_name_prefix') == lit(file_prefix)) &
                    (col('sheet_tab_name')   == lit(sheet_tab_name)))
            .orderBy(col('column_order'))
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

        schema = StructType([StructField(c, StringType(), True) for c in header])
        df = spark.createDataFrame(norm, schema) if norm else spark.createDataFrame([], schema)
        results.append((sheet_name, df, len(norm)))
    return results

# COMMAND ----------

# DBTITLE 1,Rename sheet columns -> original (final) column names from metadata
def apply_column_mapping(df_sheet, meta_rows):
    """sheet_column_name -> original_column_name. Keeps only configured columns, in column_order."""
    select_exprs, ordered_final = [], []
    sheet_cols = set(df_sheet.columns)
    for r in meta_rows:
        s_name = r['sheet_column_name']
        f_name = r['original_column_name'] or r['sheet_column_name']
        if s_name in sheet_cols:
            select_exprs.append(col(f'`{s_name}`').alias(f_name))
        else:
            # configured column missing in the sheet -> create as NULL so downstream is stable
            select_exprs.append(lit(None).cast('string').alias(f_name))
        ordered_final.append(f_name)
    return df_sheet.select(*select_exprs), ordered_final

# COMMAND ----------

# DBTITLE 1,Primary-Key-only validations: duplicate + NOT NULL + datatype  (NO count / non-PK checks)
def run_pk_validations(df, pk_cols, pk_types):
    """
    Adds PK_DERIVED + COMMENTS. COMMENTS is '' for a clean record, else pipe-joined failure reasons.
    Returns (df_with_comments, dq_check_validation_array, counts_dict).
    """
    # 1) Derived PK (md5 of all PK columns) - composite-PK safe
    parts = []
    for c in pk_cols:
        parts.append(f"nvl(cast(`{c}` as string), '~')"); parts.append("'!@~'")
    pk_expr = "md5(concat(" + ", ".join(parts) + ", '~'))" if pk_cols else "md5('~')"
    df = df.withColumn('PK_DERIVED', expr(pk_expr))

    # 2) NOT NULL check on every PK column
    if pk_cols:
        null_cond = " OR ".join([f"(`{c}` IS NULL OR trim(`{c}`) = '')" for c in pk_cols])
        df = df.withColumn('CHK_NULL', when(expr(null_cond), lit('Mandatory PK field is NULL || ')).otherwise(lit('')))
    else:
        df = df.withColumn('CHK_NULL', lit(''))

    # 3) Datatype check on non-string PK columns (try_cast fails -> mismatch)
    dtype_conds = [
        f"(`{c}` IS NOT NULL AND trim(`{c}`) <> '' AND try_cast(`{c}` AS {t}) IS NULL)"
        for c, t in zip(pk_cols, pk_types) if t.lower() not in ('string', 'varchar', 'char')
    ]
    if dtype_conds:
        df = df.withColumn('CHK_DTYPE', when(expr(" OR ".join(dtype_conds)), lit('PK datatype mismatch || ')).otherwise(lit('')))
    else:
        df = df.withColumn('CHK_DTYPE', lit(''))

    # 4) Duplicate PK check
    df = df.withColumn('cnt_chk', count(lit(1)).over(Window.partitionBy('PK_DERIVED')))
    df = df.withColumn('CHK_DUP', when(col('cnt_chk') > 1, lit('Duplicate Primary Key || ')).otherwise(lit(''))).drop('cnt_chk')

    # combine into a single COMMENTS column
    df = df.withColumn('COMMENTS', concat_ws('', col('CHK_NULL'), col('CHK_DTYPE'), col('CHK_DUP'))) \
           .drop('CHK_NULL', 'CHK_DTYPE', 'CHK_DUP')
    df.persist(StorageLevel.DISK_ONLY)

    # one pass to get all the counts (cheaper than several .count() calls)
    agg = df.agg(
        count(lit(1)).alias('total'),
        _sum(when(col('COMMENTS').contains('Duplicate'), 1).otherwise(0)).alias('dup'),
        _sum(when(col('COMMENTS').contains('Mandatory PK'), 1).otherwise(0)).alias('nul'),
        _sum(when(col('COMMENTS').contains('datatype'), 1).otherwise(0)).alias('dty'),
        _sum(when(col('COMMENTS') != '', 1).otherwise(0)).alias('bad')
    ).collect()[0]

    counts = {'total': agg['total'] or 0, 'dup': agg['dup'] or 0, 'nul': agg['nul'] or 0,
              'dty': agg['dty'] or 0, 'bad': agg['bad'] or 0}

    dq_array = [
        f"Duplicate PK Check : {'PASS' if counts['dup'] == 0 else 'FAIL'} | duplicate_records={counts['dup']}",
        f"Not Null PK Check : {'PASS' if counts['nul'] == 0 else 'FAIL'} | null_records={counts['nul']}",
        f"Data Type Check : {'PASS' if counts['dty'] == 0 else 'FAIL'} | mismatch_records={counts['dty']}",
    ]
    return df, dq_array, counts

# COMMAND ----------

# DBTITLE 1,Write bad records into the error table (same process_id for the whole tab)
def insert_error_records(df_bad, process_id, file_name, table_name, sheet_tab_name, file_path):
    err_cols = spark.read.table(ERROR_TABLE).columns  # written in the table's own column order
    df_err = (df_bad
              .select(col('COMMENTS'), to_json(struct(col('*'))).alias('err_record'))
              .withColumn('process_id', lit(process_id))
              .withColumn('execution_id', lit(execution_id))
              .withColumn('file_name', lit(file_name))
              .withColumn('table_name', lit(table_name))
              .withColumn('sheet_tab_name', lit(sheet_tab_name))
              .withColumn('file_path', lit(file_path))
              .withColumn('az_insert_ts', current_timestamp()))
    df_err.select(*err_cols).write.insertInto(ERROR_TABLE, overwrite=False)

# COMMAND ----------

# DBTITLE 1,Build the curated DF: PK as-is + all non-PK folded into one JSON column + metadata columns
def build_curated_df(df_good, ordered_final, pk_cols, pk_types, array_col,
                     batch_id, file_dttm, sheet_tab_name, source_file_name, process_id):
    non_pk = [c for c in ordered_final if c not in pk_cols]

    # PK columns kept as-is but cast to their declared datatype
    pk_select = [col(f'`{c}`').cast(t).alias(c) for c, t in zip(pk_cols, pk_types)]

    # every non-PK column -> a single JSON object column (the "data"/array column from metadata)
    json_col = to_json(struct(*[col(f'`{c}`').alias(c) for c in non_pk])).alias(array_col) if non_pk \
        else lit('{}').alias(array_col)

    df = df_good.select(*pk_select, json_col, col('PK_DERIVED')) \
                .withColumn('BATCH_ID', lit(batch_id)) \
                .withColumn('FILE_DTTM', lit(file_dttm).cast(TimestampType())) \
                .withColumn('SHEET_TAB_NAME', lit(sheet_tab_name)) \
                .withColumn('SOURCE_FILE_NAME', lit(source_file_name)) \
                .withColumn('PROCESS_ID', lit(process_id)) \
                .withColumn('EXECUTION_ID', lit(execution_id)) \
                .withColumn('_AZ_INSERT_TS', current_timestamp())

    final_order = pk_cols + [array_col] + META_COLS
    return df.select(*final_order)

# COMMAND ----------

# DBTITLE 1,Write a dataframe out as a single .parquet file
def write_parquet(target_folder, table_name, df):
    tmp = f"{target_folder}/{table_name}_tmp"
    final = f"{target_folder}/{table_name}.parquet"
    df.repartition(1).write.mode('overwrite').parquet(tmp)
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
def fn_sendEmail(sender, server, receivers, subject, html):
    msg = MIMEMultipart('alternative', None, [MIMEText(html), MIMEText(html, 'html')])
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(receivers) if isinstance(receivers, list) else receivers
    s = smtplib.SMTP(server); s.ehlo()
    s.sendmail(sender, receivers, msg.as_string()); s.quit()

def send_failure_email(df_control):
    cfg = notebook_config['email']
    subject = cfg['subject'] + ' | Run date - ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' UTC'
    failed = df_control.filter(col('final_ingestion_status') != lit('Succeeded')) \
                       .select('execution_id', 'process_id', 'file_name', 'sheet_tab_name',
                               'table_name', 'final_ingestion_status', 'comments')
    if failed.count() == 0:
        print('No failures - no e-mail sent.')
        return
    html = f"""
    <html><body><p>Dear Team,<br><br>
    The below ERP-BICC <b>Excel</b> feeds failed during ingestion.<br>
    Process control table - {PROCESS_CONTROL} (filter by the execution_id / process_id below).<br>
    Errored records are in - {ERROR_TABLE}.<br><br>
    Thanks,<br>ED&amp;A Auto Email Alerts<br>
    (auto-generated, do not reply - contact prodsuppazure@rci.rogers.com)<br></p>
    {failed.toPandas().to_html(index=False)}
    </body></html>"""
    # best-effort: a missing/unreachable SMTP server must not mask the real ingestion status
    try:
        if cfg.get('server') and not cfg['server'].startswith('${'):
            fn_sendEmail(cfg['sender'], cfg['server'], cfg['receivers'], subject, html)
            print('Failure e-mail sent.')
        else:
            print('SMTP server not configured - skipping e-mail (test mode).')
    except Exception as e:
        print(f'WARN: failure e-mail could not be sent ({e}). Continuing.')

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
if __name__ == '__main__':
    final_status, final_message = 0, 'Success'
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f'Job starts at {start_time}')
    control_rows = []

    try:
        workbooks = list_workbooks()
        print(f'Workbooks to process: {[w[2] for w in workbooks]}')

        for src_path, local_path, file_name in workbooks:
            file_prefix, batch_id, file_dttm = derive_file_meta(file_name)
            print('=' * 110)
            print(f'Workbook: {file_name} | prefix={file_prefix} | batch_id={batch_id}')

            sheets = read_excel_sheets(local_path)

            for sheet_name, df_sheet, raw_cnt in sheets:
                meta_rows = get_sheet_metadata(file_prefix, sheet_name)
                if not meta_rows:
                    print(f"  - tab '{sheet_name}' : no metadata configured -> skipped")
                    continue

                process_id = str(uuid.uuid4())
                table_name = (meta_rows[0]['table_name'] or '').lower()
                array_col  = meta_rows[0]['array_column_name'] or 'DATA'
                pk_cols  = [r['original_column_name'] or r['sheet_column_name'] for r in meta_rows if r['is_primary_key']]
                pk_types = [r['data_type'] for r in meta_rows if r['is_primary_key']]

                print(f"  - tab '{sheet_name}' -> table '{table_name}' | rows={raw_cnt} | PK={pk_cols}")

                raw_path, final_path, status_txt, comments = None, None, 'Failed', ''
                dq_array = []
                valid_cnt = err_cnt = 0

                try:
                    if df_sheet is None or raw_cnt == 0:
                        raise Exception('No data rows found in the sheet.')

                    # (a) land the sheet AS-IS as raw parquet (lineage)
                    raw_folder = resolve_output_folder(table_name, kind='raw')
                    raw_path = write_parquet(raw_folder, table_name, df_sheet)

                    # (b) map sheet columns -> final names
                    df_mapped, ordered_final = apply_column_mapping(df_sheet, meta_rows)

                    # (c) PK-only validations
                    df_val, dq_array, counts = run_pk_validations(df_mapped, pk_cols, pk_types)
                    valid_cnt = counts['total'] - counts['bad']
                    err_cnt   = counts['bad']

                    # (d) bad records -> error table
                    if err_cnt > 0:
                        insert_error_records(df_val.where(col('COMMENTS') != lit('')),
                                             process_id, file_name, table_name, sheet_name, src_path)

                    # (e) good records -> curated parquet (PK as-is + JSON data column + metadata cols)
                    df_good = df_val.where(col('COMMENTS') == lit('')).drop('COMMENTS')
                    df_curated = build_curated_df(df_good, ordered_final, pk_cols, pk_types, array_col,
                                                  batch_id, file_dttm, sheet_name, file_name, process_id)
                    cur_folder = resolve_output_folder(table_name, kind='curated')
                    final_path = write_parquet(cur_folder, table_name, df_curated)

                    df_val.unpersist()

                    if err_cnt == 0:
                        status_txt = 'Succeeded'
                        comments   = 'All primary-key checks passed.'
                    else:
                        status_txt = 'Failed'
                        comments   = (f"{err_cnt} record(s) failed PK validation -> {ERROR_TABLE}. "
                                      f"Good records ({valid_cnt}) written to {final_path}.")

                except Exception as e:
                    status_txt = 'Failed'
                    comments   = f'Tab processing failed - {str(e)}'
                    if not dq_array:
                        dq_array = ['Duplicate PK Check : NOT RUN', 'Not Null PK Check : NOT RUN', 'Data Type Check : NOT RUN']
                    print(f"    !! {comments}")

                control_rows.append((
                    execution_id, process_id, batch_id, file_name, sheet_name, table_name,
                    src_path, raw_path, final_path, dq_array,
                    int(raw_cnt), int(valid_cnt), int(err_cnt), status_txt, comments, file_dttm
                ))

        # ---- write all control rows in one shot ----
        if control_rows:
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

            # ---- failure e-mail (and fail the job if anything failed) ----
            send_failure_email(df_control)
            if df_control.filter(col('final_ingestion_status') != lit('Succeeded')).count() > 0:
                raise Exception('One or more tabs failed ingestion - see process control / error table / e-mail.')
        else:
            print('No configured tabs were found to process.')

    except Exception as e:
        final_status, final_message = 1, 'Failed | Please debug - Error - ' + str(e)
    finally:
        df_metadata_all.unpersist()
        fn_exitFinalDatabricks(start_time, final_status, final_message)
