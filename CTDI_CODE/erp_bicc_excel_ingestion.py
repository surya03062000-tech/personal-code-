# Databricks notebook source
# MAGIC %md
# MAGIC # ERP - BICC **Excel** File Ingestion into YAAF
# MAGIC <p>200_databricks custom code to pre-process <b>multi-sheet Excel (.xlsx)</b> BICC files.</p>
# MAGIC
# MAGIC **What this notebook does (high level)**
# MAGIC 1. (Optional) GPG-decrypts the encrypted Excel files. Controlled by the `decrypt_process` flag so you can point straight at a sample `.xlsx` for testing.
# MAGIC 2. Reads **every sheet/tab** of each Excel file using `com.crealytics.spark.excel` (Apache POI based — **no openpyxl**).
# MAGIC 3. Writes each sheet **as-is** to a raw Parquet (landing copy).
# MAGIC 4. Looks up the **metadata table** to drive column names, data types and primary keys.
# MAGIC 5. Runs **primary-key checks only**: duplicate PK, NULL PK, PK data-type. (No record-count check, no non-PK column checks.)
# MAGIC 6. Builds the **final Parquet** = primary key column(s) kept as-is + all other columns collapsed into a single JSON `DATA` column + standard metadata columns.
# MAGIC 7. Writes one row **per tab/table** into `bicc_process_control`, bad records into `bicc_ingestion_err_table`, and emails a summary of any failures.
# MAGIC
# MAGIC > One file with 15 tabs = 15 independent table validations. If one tab fails, only that table's records go to the error table; the other tabs still succeed.
# MAGIC
# MAGIC **Cluster requirement:** install the Maven library `com.crealytics:spark-excel_2.12:<spark_ver>_<lib_ver>`
# MAGIC (match your cluster Scala/Spark, e.g. `com.crealytics:spark-excel_2.12:3.5.1_0.20.4`).

# COMMAND ----------

# DBTITLE 1,Install libraries
# Only python-gnupg is pip-installable. spark-excel is a JVM library and MUST be attached
# to the cluster as a Maven coordinate (see header). We do not use openpyxl anywhere.

import sys, subprocess, pkg_resources

required = {'python-gnupg'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

# COMMAND ----------

# DBTITLE 1,All imports
# Single place for every import used in the notebook.

import gnupg
import json, os, re, base64, uuid, time
from datetime import datetime
from pprint import pprint

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lit, when, concat, concat_ws, current_timestamp, input_file_name,
    to_json, struct, count, expr, to_timestamp, regexp_substr, split,
    row_number, md5, lower, coalesce, sum as _sum, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    TimestampType, ArrayType
)
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

# DBTITLE 1,INPUT PARAMETERS (all required inputs at the top)
# --------------------------------------------------------------------------------------
# ALL inputs the notebook needs are defined here, at the top, as widgets.
#
#   execution_id      : pipeline run id (passed by the orchestrator).
#   notebook_config   : JSON config (same shape as the previous project - storage,
#                       decryption, validations table names, feed_file options, email).
#   decrypt_process   : "true"  -> production path: GPG-decrypt files then process.
#                       "false" -> test path: skip decryption, read test_excel_path directly.
#   test_excel_path   : full path to a sample .xlsx used ONLY when decrypt_process == false.
#   output_base_path  : where final/raw parquet is written in TEST mode. In prod mode the
#                       path is derived from notebook_config['storage'] (mounted container).
# --------------------------------------------------------------------------------------

dbutils.widgets.text('execution_id', '')
dbutils.widgets.text('notebook_config', '')
dbutils.widgets.dropdown('decrypt_process', 'false', ['true', 'false'])
dbutils.widgets.text('test_excel_path', '')
dbutils.widgets.text('output_base_path', '')

# COMMAND ----------

# DBTITLE 1,Config defaults (mirrors the config in the metadata/control project)
# These match the previous project's notebook_config. The orchestrator normally passes
# notebook_config as JSON; the fallback below lets the notebook run standalone in TEST mode.

DEFAULT_CONFIG = {
    "storage": {"adls_container": "raw", "folder": "erp", "source": "bicc_INT_680", "frequency": "hourly"},
    "decryption": {"private_key": "${erp_private_key}", "scope_nm": "${erp_keyvault_scope_nm}", "passphrase_key": "${erp_passphrase_key}"},
    "validations": {
        "metadata_table": "drvd__app_bicc.bicc_table_metadata",
        "error_table": "drvd__app_bicc.bicc_ingestion_err_table",
        "process_control": "drvd__app_bicc.bicc_process_control"
        # NOTE: load_type table removed - no longer required.
    },
    # data_start_row = the 1-based row that holds the column HEADER. CTDI report exports put a
    # title in row 1 and the real header in row 2, so default = 2. Set to 1 for plain sheets.
    "feed_file": {"excel_header": "true", "infer_schema": "false", "data_start_row": 2},
    "email": {
        "sender": "${erp_email_sender}", "receivers": "${erp_email_receivers}",
        "server": "${erp_email_server}", "subject": "Failed | ERP - BICC Excel File INT 680"
    }
}

# COMMAND ----------

# DBTITLE 1,Exit from Databricks
# Clean exit wrapper - returns JSON on success, raises on failure (so the job is marked failed).

def fn_exitFinalDatabricks(start_time, status, message):
    name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]
    json_output_dict = {
        "name": name,
        "jobStartDate": start_time,
        "messageDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        "message": message
    }
    if status == 0:
        dbutils.notebook.exit(json.dumps(json_output_dict, indent=1))
    else:
        print('-' * 120)
        pprint(json_output_dict)
        raise Exception('Job failed. Please debug...')

# COMMAND ----------

# DBTITLE 1,Get latest directory (production path discovery)
# Walks the mounted raw path to the latest dated folder based on frequency.
# Only used when decrypt_process == true.

def get_dir_content(ls_path, level=0, max_depth=2):
    dir_paths = dbutils.fs.ls(ls_path)
    if max_depth is None:
        subdir_paths = [get_dir_content(p.path, level + 1, max_depth) for p in dir_paths if p.isDir() and p.path != ls_path]
        flat = [p for sub in subdir_paths for p in sub]
    elif level < max_depth:
        subdir_paths = [get_dir_content(p.path, level + 1, max_depth) for p in dir_paths if p.isDir() and p.path != ls_path]
        flat = [p for sub in subdir_paths for p in sub]
    else:
        flat = []
    return list(map(lambda p: p.path, dir_paths)) + flat


def get_latest_directory(file_path, frequency="daily"):
    frequency_depth = {"monthly": 1, "daily": 2, "hourly": 3, "minutely": 4, "minutes": 4}
    max_depth = frequency_depth.get(frequency.lower(), 2)
    paths = get_dir_content(file_path, max_depth=max_depth)
    paths = list(set([i[:i.rfind('/')] for i in paths]))
    paths.sort(reverse=True)
    if not paths:
        raise IndexError(f"No dated directories found under {file_path}")
    latest = paths[0]
    print(f'****** Latest directory is: {latest}')
    return latest

# COMMAND ----------

# DBTITLE 1,Get mounted ADLS location (abfss + dbfs)
# Resolves a mounted ADLS container/folder to its abfss + dbfs paths. Used in prod mode.

def fn_getLocation(adls_container, folder, sub_folder=None):
    try:
        status, errMsg = 0, 'Success'
        mount_point = f'/mnt/{adls_container}'
        mount_point_prod = f'/mnt/{adls_container}/'
        mount_data = dbutils.fs.mounts()
        mountSchema = StructType([
            StructField('mountPoint', StringType(), True),
            StructField('source', StringType(), True),
            StructField('encryptionType', StringType(), True)
        ])
        df_mount = spark.createDataFrame(data=mount_data, schema=mountSchema)
        abfs_location = df_mount.filter((col('mountPoint') == lit(mount_point)) | (col('mountPoint') == lit(mount_point_prod))).select('source').collect()[0][0]
        if sub_folder is not None:
            dbfs_api_location = f'dbfs:/mnt/{adls_container}/{folder}/{sub_folder}'
            dbfs_location = f'/dbfs/mnt/{adls_container}/{folder}/{sub_folder}'
            abfs_location = abfs_location + folder + '/' + sub_folder
        else:
            dbfs_api_location = f'dbfs:/mnt/{adls_container}/{folder}'
            dbfs_location = f'/dbfs/mnt/{adls_container}/{folder}'
            abfs_location = abfs_location + folder
    except Exception as e:
        status, abfs_location, dbfs_api_location, dbfs_location = 1, None, None, None
        errMsg = f'Failed to get location - fn_getLocation. Error - {str(e)}'
    finally:
        return status, abfs_location, dbfs_api_location, dbfs_location, errMsg

# COMMAND ----------

# DBTITLE 1,GPG decryption (production path only)
# Decrypts every *.gpg in src_path, moves the encrypted originals to .encrypted/.
# Returns the list of decrypted file paths (we only care about .xlsx outputs).

def decrypt_gpg_files(src_path, scope_nm, decryption_key, passphrase_key):
    os.system('export GPG_TTY=$(tty)')
    gpg = gnupg.GPG()
    gpg.encoding = 'utf-8'

    private_key = base64.b64decode(dbutils.secrets.get(scope=scope_nm, key=decryption_key)).decode()
    passphrase = dbutils.secrets.get(scope=scope_nm, key=passphrase_key)
    print(json.dumps(gpg.import_keys(private_key).results))

    encrypted_files = dbutils.fs.ls(src_path)
    encrypted_files = [f.path.replace("dbfs:", "/dbfs", 1) for f in encrypted_files if f.name.endswith(".gpg")]
    print("encrypted files: " + json.dumps(encrypted_files))

    decrypted_paths = []
    for idx, enc in enumerate(encrypted_files):
        # foo.xlsx.gpg -> foo.xlsx  (keep the real extension, not just first token)
        dec = os.path.join(os.path.dirname(enc), os.path.basename(enc)[:-4])
        enc_final = os.path.join(os.path.dirname(enc), ".encrypted", os.path.basename(enc))
        with open(enc, 'rb') as ef:
            status = gpg.decrypt_file(ef, output=dec, passphrase=passphrase)
        print(json.dumps({"progress": f"{idx+1}/{len(encrypted_files)}", "ok": status.ok, "status": status.status}))
        assert status.ok, f"Decryption failed for {enc}. Error: {status.stderr}"
        dbutils.fs.mv(enc.replace("/dbfs", "dbfs:", 1), enc_final.replace("/dbfs", "dbfs:", 1))
        decrypted_paths.append(dec.replace("/dbfs", "dbfs:", 1))
    return decrypted_paths

# COMMAND ----------

# DBTITLE 1,File-name helpers (prefix / batch_id / file_dttm)
# Derive the metadata key (file_name_prefix) and batch attributes from the file name.
# Supports TWO naming conventions:
#   (A) CTDI report exports : <Prefix>[._]YYYY.MM.DD[.HH.MM.SS]
#         e.g. Rogers_Shaw_STB_OHB_Comparison_2026.06.10        -> prefix=Rogers_Shaw_STB_OHB_Comparison
#              IssueTrackerDetails.2026.06.10.11.00.44           -> prefix=IssueTrackerDetails
#   (B) BICC extract files  : <Prefix>-batch<n>-<YYYYMMDD_HHMMSS> (kept for backward-compat)
# The prefix is the stable metadata key (date is stripped so it matches every run).

def fn_parseFileName(file_path):
    file_name = os.path.basename(file_path)
    stem = os.path.splitext(file_name)[0]
    batch_id, file_dttm = None, None

    # (A) CTDI date suffix
    m = re.search(r'[._](\d{4}\.\d{2}\.\d{2}(?:\.\d{2}\.\d{2}\.\d{2})?)$', stem)
    if m:
        date_token = m.group(1)
        file_name_prefix = stem[:m.start()]
        batch_id = date_token.replace('.', '')                  # 20260610 or 20260610110044
        parts = date_token.split('.')
        try:
            if len(parts) >= 6:
                file_dttm = datetime(*[int(p) for p in parts[:6]])
            else:
                file_dttm = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
        except Exception:
            file_dttm = None
        return file_name, file_name_prefix, batch_id, file_dttm

    # (B) BICC -batch suffix (fallback)
    file_name_prefix = re.sub(r'-batch[0-9]*-[0-9]*_[0-9]*', '', stem)
    mb = re.search(r'(?<=batch)[0-9]+-[0-9]+_[0-9]+', file_name)
    if mb:
        batch_part, dttm_part = mb.group(0).split('-')
        batch_id = batch_part
        try:
            file_dttm = datetime.strptime(dttm_part, '%Y%m%d_%H%M%S')
        except Exception:
            file_dttm = None
    return file_name, file_name_prefix, batch_id, file_dttm

# COMMAND ----------

# DBTITLE 1,Metadata lookups (sheet list + per-sheet column metadata)
# The metadata table is the single source of truth: which tabs to read, how each column
# maps from the sheet header to the final name, its data type and whether it is a PK.

def fn_getSheetList(file_prefix, metadata_table):
    rows = (spark.read.table(metadata_table)
            .filter(lower(col('file_name_prefix')) == lit(file_prefix.lower()))
            .select('sheet_tab_name', 'table_name').distinct()
            .orderBy('sheet_tab_name').collect())
    return [(r['sheet_tab_name'], r['table_name']) for r in rows]


def fn_getSheetMetadata(file_prefix, sheet_name, metadata_table):
    return (spark.read.table(metadata_table)
            .filter((lower(col('file_name_prefix')) == lit(file_prefix.lower())) &
                    (lower(col('sheet_tab_name')) == lit(sheet_name.lower())))
            .orderBy('column_order').collect())

# COMMAND ----------

# DBTITLE 1,Read one Excel sheet (spark-excel, no openpyxl)
# Reads a single tab with com.crealytics.spark.excel. All columns are read as String
# (inferSchema=false) so we keep the data exactly as it appears (as-is). We then rename
# sheet_column_name -> original_column_name using the metadata and keep column_order.

def fn_readExcelSheet(file_path, sheet_name, meta_rows, header, infer_schema, data_start_row=2):
    try:
        status, errMsg = 0, 'Success'
        # dataAddress 'Sheet'!A<row> => that row is the HEADER (CTDI reports put a title in row 1,
        # so default row 2). With header=true, data begins on the following row.
        df = (spark.read.format("com.crealytics.spark.excel")
              .option("header", header)
              .option("inferSchema", infer_schema)
              .option("dataAddress", f"'{sheet_name}'!A{data_start_row}")
              .option("treatEmptyValuesAsNulls", "true")
              .option("usePlainNumberFormat", "true")     # avoid 1.0 / scientific notation on ids
              .option("addColorColumns", "false")
              .load(file_path))

        # Rename sheet header -> final/original column name, force string, keep metadata order.
        select_exprs = []
        data_cols = []
        for m in meta_rows:
            src = m['sheet_column_name']
            tgt = m['original_column_name']
            select_exprs.append(col(f"`{src}`").cast('string').alias(tgt))
            data_cols.append(tgt)
        df = df.select(*select_exprs)

        # Drop FULLY-EMPTY rows. CTDI report exports end with a blank trailing row; without
        # this, every file would raise a spurious NULL-PK failure (and a daily error email).
        non_empty = None
        for c in data_cols:
            cond = col(c).isNotNull() & (col(c) != lit(''))
            non_empty = cond if non_empty is None else (non_empty | cond)
        if non_empty is not None:
            df = df.filter(non_empty)

        df = df.withColumn('AZ_INPUT_FILE_NAME', lit(os.path.basename(file_path)))
    except Exception as e:
        status, df = 1, None
        errMsg = f"Failed to read sheet '{sheet_name}' from {file_path} - fn_readExcelSheet. Error - {str(e)}"
    finally:
        return status, df, errMsg

# COMMAND ----------

# DBTITLE 1,Write a single Parquet file to a target folder
# Repartition to a single file and move it out of the spark output folder, so downstream
# (100_databricks) gets one clean .parquet per table. Reused for both raw and final writes.

def fn_writeSingleParquet(target_folder, file_label, df):
    try:
        status, errMsg, finalTgt = 0, 'Success', ''
        if df is not None:
            tempTgt = f"{target_folder}/{file_label}"
            finalTgt = tempTgt + '.parquet'
            df.repartition(1).write.mode('overwrite').parquet(tempTgt)
            data_files = dbutils.fs.ls(tempTgt)
            data_file = [f.path for f in data_files if f.path.endswith(".parquet")][0]
            dbutils.fs.mv(data_file, finalTgt)
            dbutils.fs.rm(tempTgt, recurse=True)
    except Exception as e:
        status, finalTgt = 1, None
        errMsg = f'Failed to write parquet {file_label} - fn_writeSingleParquet. Error - {str(e)}'
    finally:
        return status, finalTgt, errMsg

# COMMAND ----------

# DBTITLE 1,Build target base folder (prod vs test)
# Decides where parquet for a given table lands. Prod = mounted ADLS dated path,
# Test = output_base_path passed via widget (or alongside the test file).

def fn_buildTargetBase(table_name, decrypt_process, notebook_config, output_base_path, test_excel_path):
    try:
        status, errMsg = 0, 'Success'
        if decrypt_process:
            adls_container = notebook_config['storage']['adls_container']
            folder = notebook_config['storage']['folder']
            frequency = notebook_config['storage']['frequency']
            status, abfs_location, _, _, errMsg = fn_getLocation(adls_container, folder, table_name)
            if status != 0:
                raise Exception(errMsg)
            now = datetime.now()
            depth = {'monthly': ['%Y', '%m'], 'daily': ['%Y', '%m', '%d'],
                     'hourly': ['%Y', '%m', '%d', '%H'], 'minutes': ['%Y', '%m', '%d', '%H', '%M'],
                     'minutely': ['%Y', '%m', '%d', '%H', '%M']}.get(frequency.lower(), ['%Y', '%m', '%d'])
            dated = '/'.join(now.strftime(p) for p in depth)
            target_base = f"{abfs_location}/{dated}"
        else:
            base = output_base_path.strip() or (os.path.dirname(test_excel_path) + '/_output')
            target_base = f"{base.rstrip('/')}/{table_name}"
    except Exception as e:
        status, target_base = 1, None
        errMsg = f'Failed to build target base for {table_name} - fn_buildTargetBase. Error - {str(e)}'
    finally:
        return status, target_base, errMsg

# COMMAND ----------

# DBTITLE 1,Add derived primary key (PK_DERIVED)
# md5 over the PK columns. NULL-safe (nvl to '~') so composite blank PKs still hash
# deterministically. If a table has no PK defined, fall back to a constant hash.

def fn_addDerivedPK(df, pk_cols):
    if pk_cols:
        pieces = ", ".join([f"nvl(`{c}`, '~'), '!@~'" for c in pk_cols]) + ", '~'"
        pk_expr = f"md5(concat({pieces}))"
    else:
        pk_expr = "md5('~')"
    return df.withColumn('PK_DERIVED', expr(pk_expr))

# COMMAND ----------

# DBTITLE 1,Primary-key validations (duplicate + not-null + data type)
# This is the ONLY validation set now (count check + non-PK checks were removed).
# Adds a single COMMENTS column. Rows with COMMENTS != '' are errors.
#   - Duplicate PK : count(PK_DERIVED) over the table > 1
#   - NULL PK      : any PK column is NULL
#   - Data type    : casting a (non-null) PK value to its metadata data_type yields NULL

def fn_validatePrimaryKey(df, meta_rows):
    try:
        status, errMsg = 0, 'Success'
        pk_meta = [m for m in meta_rows if m['is_primary_key']]
        pk_cols = [m['original_column_name'] for m in pk_meta]

        df = fn_addDerivedPK(df, pk_cols)

        # 1) Duplicate flag
        w = Window.partitionBy('PK_DERIVED')
        df = df.withColumn('FLAG_DUP', when(count(lit(1)).over(w) > lit(1), lit(1)).otherwise(lit(0)))

        # 2) NULL PK flag
        if pk_cols:
            null_cond = None
            for c in pk_cols:
                cond = col(c).isNull()
                null_cond = cond if null_cond is None else (null_cond | cond)
            df = df.withColumn('FLAG_NULL', when(null_cond, lit(1)).otherwise(lit(0)))
        else:
            df = df.withColumn('FLAG_NULL', lit(0))

        # 3) Data-type flag (PK columns only)
        dtype_cond = None
        for m in pk_meta:
            c, dt = m['original_column_name'], m['data_type']
            cond = col(c).isNotNull() & col(c).cast(dt).isNull()
            dtype_cond = cond if dtype_cond is None else (dtype_cond | cond)
        df = df.withColumn('FLAG_DTYPE', when(dtype_cond, lit(1)).otherwise(lit(0))) if dtype_cond is not None \
               else df.withColumn('FLAG_DTYPE', lit(0))

        # Build the human-readable COMMENTS from the three flags.
        df = df.withColumn('COMMENTS', concat_ws('',
                when(col('FLAG_DUP') == 1, lit('Duplicate Primary Key || ')).otherwise(lit('')),
                when(col('FLAG_NULL') == 1, lit('Mandatory Primary Key field is NULL || ')).otherwise(lit('')),
                when(col('FLAG_DTYPE') == 1, lit('Primary Key data type mismatch || ')).otherwise(lit(''))))
    except Exception as e:
        status, df = 1, None
        errMsg = f'Failed PK validation - fn_validatePrimaryKey. Error - {str(e)}'
    finally:
        return status, df, pk_cols, errMsg

# COMMAND ----------

# DBTITLE 1,Insert error records into the error table
# Only rows with COMMENTS != '' are written. The whole bad row is captured as JSON in
# ERR_RECORD. PROCESS_ID/TABLE_NAME are shared for all error rows of this tab.

def fn_insertErrRecords(execution_id, process_id, file_path, file_name, table_name, df_err, error_table):
    try:
        status, errMsg = 0, 'Success'
        df_bad = df_err.where(col('COMMENTS') != lit(''))
        df_bad = (df_bad
                  .withColumn('ERR_RECORD', to_json(struct([c for c in df_err.columns
                                                            if c not in ('FLAG_DUP', 'FLAG_NULL', 'FLAG_DTYPE')])))
                  .withColumn('EXECUTION_ID', lit(execution_id))
                  .withColumn('PROCESS_ID', lit(process_id))
                  .withColumn('FILE_NAME', lit(file_name))
                  .withColumn('TABLE_NAME', lit(table_name))
                  .withColumn('FILE_PATH', lit(file_path))
                  .withColumn('AZ_INSERT_TS', current_timestamp()))
        cols_to_write = spark.read.table(error_table).columns
        df_bad.select(*cols_to_write).write.insertInto(error_table, overwrite=False)
    except Exception as e:
        status = 1
        errMsg = f'Failed to insert error records - fn_insertErrRecords. Error - {str(e)}'
    finally:
        return status, errMsg

# COMMAND ----------

# DBTITLE 1,Build the FINAL parquet (PK as-is + JSON DATA + metadata)
# Final shape per the requirement:
#   PK_DERIVED, <pk columns kept as-is>, DATA (json of all non-PK cols keyed by
#   array_column_name), BATCH_ID, FILE_DTTM, AZ_INPUT_FILE_NAME, _az_insert_ts.
# Only good rows (COMMENTS == '') are written.

def fn_buildFinalDf(df_good, meta_rows, pk_cols, batch_id, file_dttm):
    pk_meta = [m for m in meta_rows if m['is_primary_key']]
    non_pk_meta = [m for m in meta_rows if not m['is_primary_key']]

    # JSON DATA column: keys = array_column_name, values = the non-PK column values.
    data_struct = [col(m['original_column_name']).alias(m['array_column_name']) for m in non_pk_meta]
    df_final = df_good.withColumn('DATA', to_json(struct(*data_struct)) if data_struct else lit(None).cast('string'))

    # Keep PK columns as-is but cast to their declared data type for clean storage.
    final_cols = [col('PK_DERIVED')]
    for m in pk_meta:
        final_cols.append(col(m['original_column_name']).cast(m['data_type']).alias(m['original_column_name']))
    final_cols.append(col('DATA'))

    df_final = (df_final.select(*final_cols)
                .withColumn('BATCH_ID', lit(batch_id))
                .withColumn('FILE_DTTM', lit(file_dttm).cast(TimestampType()))
                .withColumn('AZ_INPUT_FILE_NAME', lit(df_good.select('AZ_INPUT_FILE_NAME').first()[0]))
                .withColumn('_az_insert_ts', current_timestamp()))
    return df_final

# COMMAND ----------

# DBTITLE 1,Write the process-control rows
# Writes one row per tab/table into bicc_process_control. dq_check_validation is an
# array<string> ([duplicate, not-null, data-type] results).

def fn_writeProcessControl(control_rows, process_control):
    try:
        status, errMsg = 0, 'Success'
        control_schema = StructType([
            StructField('execution_id', StringType(), True),
            StructField('process_id', StringType(), True),
            StructField('batch_id', StringType(), True),
            StructField('file_name', StringType(), True),
            StructField('sheet_tab_name', StringType(), True),
            StructField('table_name', StringType(), True),
            StructField('raw_parquet_path', StringType(), True),
            StructField('dq_check_validation', ArrayType(StringType()), True),
            StructField('record_count', LongType(), True),
            StructField('error_count', LongType(), True),
            StructField('final_parquet_source_raw', StringType(), True),
            StructField('final_ingestion_status', StringType(), True),
            StructField('comments', StringType(), True),
            StructField('file_dttm', TimestampType(), True),
        ])
        df_ctrl = spark.createDataFrame(control_rows, schema=control_schema) \
                       .withColumn('_az_insert_ts', current_timestamp())
        cols_to_write = spark.read.table(process_control).columns
        df_ctrl.select(*cols_to_write).write.insertInto(process_control, overwrite=False)
    except Exception as e:
        status = 1
        errMsg = f'Failed to write process control - fn_writeProcessControl. Error - {str(e)}'
    finally:
        return status, errMsg

# COMMAND ----------

# DBTITLE 1,Email functions (send + compose)
# Same behaviour as the previous project: build an HTML table of failed tables and email it.

def fn_sendEmail(sender, server, receivers, subject, msgtext, msghtml, attachmentPath=None):
    try:
        status, errMsg = 0, f"Email sent to {receivers} | subject - {subject}"
        message = MIMEMultipart("alternative", None, [MIMEText(msgtext), MIMEText(msghtml, "html")])
        message["Subject"] = subject
        message["From"] = sender
        message["To"] = ", ".join(receivers) if isinstance(receivers, list) else receivers
        if attachmentPath is not None:
            with open(attachmentPath, "rb") as a:
                part = MIMEApplication(a.read(), Name=os.path.basename(attachmentPath))
            part["Content-Disposition"] = 'attachment; filename="%s"' % os.path.basename(attachmentPath)
            message.attach(part)
        s = smtplib.SMTP(server)
        s.ehlo()
        s.sendmail(sender, receivers, message.as_string())
        s.quit()
    except Exception as e:
        status = 1
        errMsg = f"Failed to send email to {receivers} - fn_sendEmail. Error - {str(e)}"
    finally:
        return status, errMsg


def fn_composeEmail(control_rows, sender, server, receivers, subject, notebook_config):
    try:
        status, errMsg = 0, 'Success'
        failed = [r for r in control_rows if r['final_ingestion_status'] != 'Succeeded']
        if not failed:
            print('No email will be sent - no failures.')
            return status, errMsg

        rows_html = "".join(
            f"<tr><td>{r['execution_id']}</td><td>{r['process_id']}</td><td>{r['file_name']}</td>"
            f"<td>{r['sheet_tab_name']}</td><td>{r['table_name']}</td>"
            f"<td>{'; '.join(r['dq_check_validation'] or [])}</td><td>{r['comments']}</td></tr>"
            for r in failed
        )
        html = f"""
        <html><body>
        <p>Dear Team,<br><br>
        The below ERP - BICC Excel tabs failed during ingestion.<br>
        Check the process control table <b>{notebook_config['validations']['process_control']}</b> and the error
        table <b>{notebook_config['validations']['error_table']}</b> using the execution_id / process_id below.<br><br>
        Thanks,<br>ED&amp;A Auto Email Alerts<br>
        ------------------------------------------------------------<br>
        Auto-generated email - do not reply.<br></p>
        <table border="1" cellpadding="4" cellspacing="0">
        <tr><th>execution_id</th><th>process_id</th><th>file_name</th><th>sheet_tab_name</th>
        <th>table_name</th><th>dq_check_validation</th><th>comments</th></tr>
        {rows_html}
        </table></body></html>
        """
        status, errMsg = fn_sendEmail(sender, server, receivers, subject, html, html)
        if status != 0:
            raise Exception(errMsg)
        # Surface as a failure so the job is flagged (same as previous project).
        raise Exception('One or more Excel tabs failed DQ. Please connect with the source team.')
    except Exception as e:
        status, errMsg = 1, f'Error in fn_composeEmail. Error - {str(e)}'
    finally:
        return status, errMsg

# COMMAND ----------

# DBTITLE 1,Process one sheet/tab end-to-end
# Encapsulates everything for a single tab so a failure in one tab does NOT stop the others.
# Returns a control-row dict (always) so process_control gets an entry per tab.

def fn_processSheet(execution_id, file_name, file_path, file_name_prefix, batch_id, file_dttm,
                    sheet_tab_name, table_name, notebook_config, decrypt_process,
                    output_base_path, test_excel_path):
    process_id = str(uuid.uuid4())                      # unique per tab/table (shared by its error rows)
    metadata_table = notebook_config['validations']['metadata_table']
    error_table = notebook_config['validations']['error_table']
    header = notebook_config['feed_file'].get('excel_header', 'true')
    infer_schema = notebook_config['feed_file'].get('infer_schema', 'false')
    data_start_row = notebook_config['feed_file'].get('data_start_row', 2)

    control = {
        'execution_id': execution_id, 'process_id': process_id, 'batch_id': batch_id,
        'file_name': file_name, 'sheet_tab_name': sheet_tab_name, 'table_name': table_name,
        'raw_parquet_path': None, 'dq_check_validation': [], 'record_count': 0, 'error_count': 0,
        'final_parquet_source_raw': None, 'final_ingestion_status': 'Failed',
        'comments': None, 'file_dttm': file_dttm
    }
    try:
        print('-' * 100)
        print(f"Processing tab '{sheet_tab_name}' -> table '{table_name}' (process_id={process_id})")

        # 1) Metadata for this sheet
        meta_rows = fn_getSheetMetadata(file_name_prefix, sheet_tab_name, metadata_table)
        if not meta_rows:
            control['comments'] = f"No metadata found for prefix '{file_name_prefix}', tab '{sheet_tab_name}'."
            return control

        # 2) Read the sheet (as-is, all string)
        status, df_sheet, errMsg = fn_readExcelSheet(file_path, sheet_tab_name, meta_rows, header, infer_schema, data_start_row)
        if status != 0:
            control['comments'] = errMsg
            return control

        status, raw_base, errMsg = fn_buildTargetBase(table_name, decrypt_process, notebook_config, output_base_path, test_excel_path)
        if status != 0:
            control['comments'] = errMsg
            return control

        # 3) Convert sheet -> raw parquet AS-IS (landing copy)  [process_control.csv_file_name <=> raw_parquet_path]
        status, raw_path, errMsg = fn_writeSingleParquet(raw_base + '/_raw', table_name + '_raw', df_sheet)
        if status != 0:
            control['comments'] = errMsg
            return control
        control['raw_parquet_path'] = raw_path

        # 4) PK validations (the only checks)
        status, df_val, pk_cols, errMsg = fn_validatePrimaryKey(df_sheet, meta_rows)
        if status != 0:
            control['comments'] = errMsg
            return control
        df_val.persist(StorageLevel.DISK_ONLY)

        # 5) Single-pass aggregation for counts -> dq_check_validation array
        stats = df_val.agg(
            _sum('FLAG_DUP').alias('dup'), _sum('FLAG_NULL').alias('nul'),
            _sum('FLAG_DTYPE').alias('dt'), count(lit(1)).alias('total')
        ).collect()[0]
        total = stats['total'] or 0
        dup, nul, dt = (stats['dup'] or 0), (stats['nul'] or 0), (stats['dt'] or 0)
        error_count = df_val.where(col('COMMENTS') != lit('')).count()

        dq_array = [
            f"Duplicate Primary Key Check: {'PASS' if dup == 0 else f'FAIL ({dup} rows)'}",
            f"Not Null Primary Key Check: {'PASS' if nul == 0 else f'FAIL ({nul} rows)'}",
            f"Data Type Check: {'PASS' if dt == 0 else f'FAIL ({dt} rows)'}",
        ]
        control['dq_check_validation'] = dq_array
        control['record_count'] = int(total)
        control['error_count'] = int(error_count)

        # 6) Write bad rows to the error table (only this tab's records)
        if error_count > 0:
            status, errMsg = fn_insertErrRecords(execution_id, process_id, raw_path, file_name, table_name, df_val, error_table)
            if status != 0:
                control['comments'] = errMsg
                df_val.unpersist()
                return control
            control['comments'] = (f"{error_count} of {total} records failed PK checks. "
                                   f"See error table {error_table} (process_id={process_id}).")
            control['final_ingestion_status'] = 'Failed'
            df_val.unpersist()
            return control

        # 7) All good -> build + write FINAL parquet (PK as-is + JSON DATA + metadata)
        df_good = df_val.where(col('COMMENTS') == lit('')).drop('FLAG_DUP', 'FLAG_NULL', 'FLAG_DTYPE', 'COMMENTS')
        df_final = fn_buildFinalDf(df_good, meta_rows, pk_cols, batch_id, file_dttm)
        status, final_path, errMsg = fn_writeSingleParquet(raw_base, table_name, df_final)
        df_val.unpersist()
        if status != 0:
            control['comments'] = errMsg
            return control

        control['final_parquet_source_raw'] = final_path
        control['final_ingestion_status'] = 'Succeeded'
        control['comments'] = 'Succeeded'
        print(f"Tab '{sheet_tab_name}' succeeded -> {final_path}")
    except Exception as e:
        control['comments'] = f'Failed processing tab {sheet_tab_name} - fn_processSheet. Error - {str(e)}'
    return control

# COMMAND ----------

# DBTITLE 1,MAIN - orchestrates the whole run
if __name__ == '__main__':
    try:
        final_status, final_message = 0, 'Success'
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Job starts at {start_time}')

        # ---- Read inputs ----
        execution_id = dbutils.widgets.get('execution_id') or str(uuid.uuid4())
        raw_cfg = dbutils.widgets.get('notebook_config')
        notebook_config = json.loads(raw_cfg) if raw_cfg.strip() else DEFAULT_CONFIG
        decrypt_process = dbutils.widgets.get('decrypt_process').strip().lower() == 'true'
        test_excel_path = dbutils.widgets.get('test_excel_path').strip()
        output_base_path = dbutils.widgets.get('output_base_path').strip()

        metadata_table = notebook_config['validations']['metadata_table']
        process_control = notebook_config['validations']['process_control']

        # ---- Discover the Excel files ----
        if decrypt_process:
            # PRODUCTION: walk to latest dated folder, GPG-decrypt, pick the .xlsx outputs.
            print('decrypt_process=true -> production path (decrypt then read).')
            adls_container = notebook_config['storage']['adls_container']
            folder = notebook_config['storage']['folder']
            sub_folder = notebook_config['storage']['source']
            frequency = notebook_config['storage']['frequency']
            scope_nm = notebook_config['decryption']['scope_nm']
            key_name = notebook_config['decryption']['private_key']
            passphrase_key = notebook_config['decryption']['passphrase_key']

            _, _, dbfs_api_location, _, errMsg = fn_getLocation(adls_container, folder, sub_folder)
            latest_dbfs = get_latest_directory(dbfs_api_location, frequency)
            decrypt_gpg_files(latest_dbfs, scope_nm, key_name, passphrase_key)
            excel_files = [f.path for f in dbutils.fs.ls(latest_dbfs) if f.name.lower().endswith(('.xlsx', '.xls'))]
        else:
            # TEST: read the sample Excel directly. No decryption.
            print('decrypt_process=false -> TEST path (read sample .xlsx directly).')
            if not test_excel_path:
                raise Exception('test_excel_path is required when decrypt_process=false.')
            excel_files = [test_excel_path]

        if not excel_files:
            raise Exception('No Excel files found to process.')
        print(f'Excel files to process: {excel_files}')

        # ---- Process every file -> every tab ----
        control_rows = []
        for file_path in excel_files:
            file_name, file_name_prefix, batch_id, file_dttm = fn_parseFileName(file_path)
            print(f"\nFile: {file_name} | prefix: {file_name_prefix} | batch_id: {batch_id} | dttm: {file_dttm}")

            sheets = fn_getSheetList(file_name_prefix, metadata_table)
            if not sheets:
                raise Exception(f"No metadata/sheets configured for prefix '{file_name_prefix}'.")
            print(f"Tabs to validate: {[s[0] for s in sheets]}")

            for sheet_tab_name, table_name in sheets:
                control = fn_processSheet(
                    execution_id, file_name, file_path, file_name_prefix, batch_id, file_dttm,
                    sheet_tab_name, table_name, notebook_config, decrypt_process,
                    output_base_path, test_excel_path
                )
                control_rows.append(control)

        # ---- Write all control rows ----
        print('\nWriting process control rows...')
        status, errMsg = fn_writeProcessControl(control_rows, process_control)
        if status != 0:
            raise Exception(errMsg)

        # ---- Email summary for failures ----
        sender = notebook_config['email']['sender']
        server = notebook_config['email']['server']
        receivers = notebook_config['email']['receivers']
        subject = notebook_config['email']['subject'] + ' | Run date - ' + start_time + ' UTC'
        status, errMsg = fn_composeEmail(control_rows, sender, server, receivers, subject, notebook_config)
        if status != 0:
            raise Exception(errMsg)

    except Exception as e:
        final_status, final_message = 1, 'Failed | Please debug - Error - ' + str(e)
    finally:
        fn_exitFinalDatabricks(start_time, final_status, final_message)
