# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 oracle_edl_engine  (shared library — do NOT edit to run)
# MAGIC
# MAGIC This notebook only **defines functions**. It never runs a load by itself.
# MAGIC The user-facing config notebooks call it with `%run ./oracle_edl_engine`
# MAGIC and then invoke `run_standard(CONFIG)` or `run_type1(CONFIG)`.
# MAGIC
# MAGIC ### Key behaviour
# MAGIC - **Row-count windows**: windows are sized by `rows_per_window` (e.g. 40M),
# MAGIC   split on the numeric PK — *not* by fixed days. Even-sized, predictable.
# MAGIC - **Resumable**: every window is checkpointed. If a run fails, the next run
# MAGIC   **re-processes only the unfinished windows** of the same cycle.
# MAGIC - **Type 1 (large date slice)**: pass `start_date`/`end_date`; only that date
# MAGIC   range is windowed & loaded — ideal for splitting 33B across many notebooks.
# MAGIC - **Standard (incremental)**: watermark-driven; advances only when the whole
# MAGIC   cycle matches Oracle.
# MAGIC - 3-level naming `catalog.schema.table`; idempotent writes; Liquid Clustering;
# MAGIC   count-from-metrics; reconciliation.

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 1 — Imports + defaults

# COMMAND ----------
import oracledb, math, time, traceback
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, FloatType,
    TimestampType, IntegerType, BooleanType, DecimalType
)
from delta.tables import DeltaTable

DEFAULTS = {
    "engine": "oracledb",                 # "oracledb" (no jar) | "jdbc"
    "oracle_host": "exa013mdc1-scan.rci.rogers.com",
    "oracle_port": 1531,
    "oracle_service": "IFRSDWPR_SN",
    "secret_scope": "oracle-scope",
    "secret_user_key": "oracle-user",
    "secret_pwd_key": "oracle-pwd",
    "oracle_schema": None,
    "oracle_table": None,
    "primary_keys": [],
    "watermark_col": "",
    "edl_catalog": "edl_dev",
    "target_schema": None,                 # defaults to oracle_schema
    "drop_columns": ["GG_BIGDATA_SCN", "GG_BIGDATA_LOG_POSITION"],
    "num_partitions": 64,                  # parallel readers PER window
    "fetch_size": 100_000,
    "rows_per_window": 40_000_000,         # ★ each window ≈ this many rows
    "full_backfill": True,                 # True=append, False=MERGE upsert
    "recon_numeric_col": "",
    "oracle_parallel_hint": 0,             # 0=off; e.g. 2 for /*+ PARALLEL(t,2) */
    "oracledb_all_string": False,          # True=safe slow path, False=typed fast
    "window_retry_max": 3,
    "window_retry_wait": 20,
    # Type 1 only:
    "start_date": None,                    # "YYYY-MM-DD"
    "end_date": None,                      # "YYYY-MM-DD"
    "date_filter_col": None,               # defaults to watermark_col
}
print("✅ engine: imports + defaults")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 2 — Config object + Spark tuning

# COMMAND ----------
class Cfg:
    """Merges user CONFIG over DEFAULTS and exposes attributes + derived names."""
    def __init__(self, user_cfg, mode):
        m = {**DEFAULTS, **(user_cfg or {})}
        self.mode = mode
        for k, v in m.items():
            setattr(self, k, v)
        if not self.oracle_schema or not self.oracle_table:
            raise ValueError("oracle_schema and oracle_table are required")
        self.target_schema = self.target_schema or self.oracle_schema
        self.date_filter_col = self.date_filter_col or self.watermark_col
        self.pk_col = self.primary_keys[0] if self.primary_keys else None
        self.target_table = f"{self.oracle_table}_BKG"
        self.fq_table = f"{self.edl_catalog}.{self.target_schema}.{self.target_table}"
        self.control_schema = f"{self.edl_catalog}.{self.target_schema}"
        self.window_control = f"{self.control_schema}.edl_window_control"
        self.watermark_table = f"{self.control_schema}.edl_watermark_control"
        self.load_log = f"{self.control_schema}.edl_load_log"
        sc = dbutils.secrets
        self.user = sc.get(self.secret_scope, self.secret_user_key)
        self.pwd  = sc.get(self.secret_scope, self.secret_pwd_key)
        self.dsn  = f"{self.oracle_host}:{self.oracle_port}/{self.oracle_service}"
        self.jdbc_url = f"jdbc:oracle:thin:@//{self.oracle_host}:{self.oracle_port}/{self.oracle_service}"

def apply_spark_tuning(cfg):
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled",
                   "false" if cfg.full_backfill else "true")
    spark.conf.set("spark.sql.files.maxRecordsPerFile", "5000000")
    spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
    spark.conf.set("spark.network.timeout", "1800s")
    spark.conf.set("spark.executor.heartbeatInterval", "60s")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "100000")

print("✅ engine: Cfg + tuning")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 3 — Oracle helpers

# COMMAND ----------
def _conn(cfg):
    return oracledb.connect(user=cfg.user, password=cfg.pwd, dsn=cfg.dsn)

def ora_exec(cfg, sql):
    c = _conn(cfg); cur = c.cursor(); cur.execute(sql)
    rows = cur.fetchall(); cur.close(); c.close()
    return rows

def ora_row(cfg, sql):
    r = ora_exec(cfg, sql); return r[0] if r else (None,)

def ora_scalar(cfg, sql):
    return ora_row(cfg, sql)[0]

def get_oracle_columns(cfg):
    sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID
        FROM ALL_TAB_COLUMNS
        WHERE OWNER='{cfg.oracle_schema.upper()}' AND TABLE_NAME='{cfg.oracle_table.upper()}'
        ORDER BY COLUMN_ID
    """
    return [{"name": r[0], "data_type": r[1], "precision": r[2],
             "scale": r[3], "nullable": r[4]} for r in ora_exec(cfg, sql)]

print("✅ engine: Oracle helpers")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 4 — Target DDL (Liquid Clustering) + control tables

# COMMAND ----------
def oracle_col_to_ddl(c):
    dt = (c["data_type"] or "").upper().strip(); p, s = c["precision"], c["scale"]
    if dt == "NUMBER":
        return "DOUBLE" if p is None else f"DECIMAL({p},{s if s is not None else 0})"
    if dt in ("BINARY_FLOAT",): return "FLOAT"
    if dt in ("BINARY_DOUBLE", "FLOAT"): return "DOUBLE"
    if dt in ("VARCHAR2","NVARCHAR2","CHAR","NCHAR","VARCHAR","NVARCHAR"): return "STRING"
    if dt == "DATE" or dt.startswith("TIMESTAMP"): return "TIMESTAMP"
    return "STRING"

AUDIT_COLS = [("GG_LOAD_TO_TDAT_TS","TIMESTAMP"), ("GG_DATALAKE_LOAD_TS","TIMESTAMP"),
              ("_BATCH_ID","STRING"), ("_SOURCE_TABLE","STRING")]

def ensure_namespace(cfg):
    # Catalog is assumed to already exist (CREATE CATALOG needs metastore admin).
    # Only try to create the schema; if there's no privilege, assume it exists.
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.control_schema}")
    except Exception as e:
        print(f"  ℹ️ schema create skipped (assuming it exists): {str(e).splitlines()[0]}")

def ensure_control_tables(cfg):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cfg.window_control} (
        source_schema STRING, source_table STRING, cycle_id STRING,
        where_clause STRING, advance_wm BOOLEAN, wm_start TIMESTAMP, wm_end TIMESTAMP,
        window_idx INT, pk_lo LONG, pk_hi LONG,
        oracle_count LONG, edl_count LONG, status STRING, updated_ts TIMESTAMP
    ) USING DELTA""")
    # migrate older control tables that predate wm_start
    try:
        spark.sql(f"ALTER TABLE {cfg.window_control} ADD COLUMNS (wm_start TIMESTAMP)")
    except Exception:
        pass
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cfg.watermark_table} (
        source_schema STRING, source_table STRING, watermark_col STRING,
        last_processed_end TIMESTAMP, updated_ts TIMESTAMP
    ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cfg.load_log} (
        batch_id STRING, source_schema STRING, source_table STRING, target_table STRING,
        cycle_id STRING, window_idx INT, pk_lo LONG, pk_hi LONG,
        oracle_count LONG, edl_count LONG, count_match BOOLEAN, attempt_num INT,
        num_partitions INT, engine STRING, duration_sec DOUBLE, rows_per_sec LONG,
        status STRING, error_msg STRING, load_ts TIMESTAMP
    ) USING DELTA""")

def build_and_create_target(cfg):
    if spark.catalog.tableExists(cfg.fq_table):
        print(f"  ℹ️ target exists: {cfg.fq_table}"); return False
    ora_cols = get_oracle_columns(cfg)
    if not ora_cols: raise ValueError(f"No columns for {cfg.oracle_schema}.{cfg.oracle_table}")
    drop_set = {c.upper() for c in cfg.drop_columns}
    defs = [f"  {c['name']} {oracle_col_to_ddl(c)}{'' if c['nullable']=='Y' else ' NOT NULL'}"
            for c in ora_cols if c["name"].upper() not in drop_set]
    defs += [f"  {a} {t}" for a, t in AUDIT_COLS]
    cl_cols = [cfg.pk_col] + ([cfg.watermark_col] if cfg.watermark_col else [])
    cluster = f" CLUSTER BY ({', '.join(cl_cols)})" if cl_cols else ""
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg.fq_table} (\n" + ",\n".join(defs) + f"\n) USING DELTA{cluster}")
    except Exception as e:
        print(f"  ⚠️ clustered DDL failed ({e}) — plain delta");
        spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg.fq_table} (\n" + ",\n".join(defs) + "\n) USING DELTA")
    print(f"  ✅ created target: {cfg.fq_table}"); return True

print("✅ engine: DDL + control tables")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 5 — Cast map + audit

# COMMAND ----------
def build_cast_map(cfg):
    try: return [(f.name, f.dataType) for f in spark.table(cfg.fq_table).schema]
    except Exception: return []

def apply_cast_map(df, cast_map):
    if not cast_map: return df
    cols = {c.upper(): c for c in df.columns}
    for name, tgt in cast_map:
        if name.upper() not in cols: continue
        col = cols[name.upper()]; src = df.schema[col].dataType
        if type(src) == type(tgt):
            if not isinstance(tgt, DecimalType): continue
            if src.precision == tgt.precision and src.scale == tgt.scale: continue
        if isinstance(tgt, TimestampType):
            if isinstance(src, StringType):
                # try_to_timestamp → unparseable/garbage dates become NULL instead of
                # failing the whole window (dirty Oracle data tolerance).
                df = df.withColumn(col,
                    F.expr(f"try_to_timestamp(`{col}`, 'yyyy-MM-dd HH:mm:ss.SSSSSS')"))
            elif not isinstance(src, TimestampType):
                df = df.withColumn(col, F.col(col).cast(TimestampType()))
        else:
            df = df.withColumn(col, F.col(col).cast(tgt))
    return df

def add_audit(df, cfg, batch_id):
    for c in cfg.drop_columns:
        if c in df.columns: df = df.drop(c)
    return (df.withColumn("GG_LOAD_TO_TDAT_TS", F.current_timestamp())
              .withColumn("GG_DATALAKE_LOAD_TS", F.current_timestamp())
              .withColumn("_BATCH_ID", F.lit(batch_id))
              .withColumn("_SOURCE_TABLE", F.lit(f"{cfg.oracle_schema}.{cfg.oracle_table}")))

print("✅ engine: cast + audit")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 6 — Date bound + cycle resolution (Type 1 vs Standard)

# COMMAND ----------
# ⚠️ COLON-FREE date format (YYYYMMDDHH24MISS).
# python-oracledb's thin driver parses ":name" as bind placeholders; a colon in
# an inlined time literal (e.g. '08:03:00') can be misread as a bind → DPY-4010.
# Using a colon-free format removes that risk entirely (valid for oracledb + JDBC).
def date_where(col, ds, de):
    if not col or (not ds and not de): return ""
    p = []
    if ds: p.append(f"{col} >= TO_DATE('{ds.replace('-','')}000000','YYYYMMDDHH24MISS')")
    if de: p.append(f"{col} <= TO_DATE('{de.replace('-','')}235959','YYYYMMDDHH24MISS')")
    return " AND ".join(p)

def ts_where(col, lo_dt, hi_dt):
    lo = lo_dt.strftime("%Y%m%d%H%M%S"); hi = hi_dt.strftime("%Y%m%d%H%M%S")
    return (f"{col} > TO_DATE('{lo}','YYYYMMDDHH24MISS') "
            f"AND {col} <= TO_DATE('{hi}','YYYYMMDDHH24MISS')")

def get_high_watermark(cfg):
    r = spark.sql(f"""SELECT MAX(last_processed_end) v FROM {cfg.watermark_table}
        WHERE source_schema='{cfg.oracle_schema}' AND source_table='{cfg.oracle_table}'
          AND watermark_col='{cfg.watermark_col}'""").collect()
    v = r[0]["v"] if r else None
    return v.replace(tzinfo=None) if v else None

def save_high_watermark(cfg, end_dt):
    e = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""INSERT INTO {cfg.watermark_table} VALUES (
        '{cfg.oracle_schema}','{cfg.oracle_table}','{cfg.watermark_col}',
        TIMESTAMP '{e}', current_timestamp())""")

def rebuild_where(cfg, cycle_id, wm_start, wm_end):
    """Rebuild the WHERE fresh from cycle metadata — never trust stored text.
    Returns None if it cannot be rebuilt (old rows) → caller auto-heals."""
    if cycle_id.startswith("DR::"):
        parts = cycle_id.split("::")            # DR::<col>::<ds>::<de>
        if len(parts) != 4: return None
        return date_where(parts[1], parts[2], parts[3])
    if cycle_id.startswith("INC::"):
        if not wm_start or not wm_end: return None
        return ts_where(cfg.watermark_col, wm_start, wm_end)
    if cycle_id == "FULL":
        return ""
    return None

def find_resumable_cycle(cfg, prefix):
    rows = spark.sql(f"""SELECT cycle_id, advance_wm, wm_start, wm_end
        FROM {cfg.window_control}
        WHERE source_schema='{cfg.oracle_schema}' AND source_table='{cfg.oracle_table}'
          AND status <> 'SUCCESS' AND cycle_id LIKE '{prefix}%'
        LIMIT 1""").collect()
    if not rows: return None
    r = rows[0]; cid = r["cycle_id"]
    ws = r["wm_start"].replace(tzinfo=None) if r["wm_start"] else None
    we = r["wm_end"].replace(tzinfo=None) if r["wm_end"] else None
    where = rebuild_where(cfg, cid, ws, we)
    if where is None:
        # old/un-rebuildable cycle (e.g. created before wm_start existed) → clear & restart
        print(f"  🧹 Auto-healing un-rebuildable cycle {cid} — clearing its checkpoint rows")
        spark.sql(f"""DELETE FROM {cfg.window_control}
            WHERE source_schema='{cfg.oracle_schema}' AND source_table='{cfg.oracle_table}'
              AND cycle_id='{cid}'""")
        return None
    print(f"  ♻️  Resuming incomplete cycle: {cid}")
    return {"cycle_id": cid, "where": where, "advance_wm": bool(r["advance_wm"]),
            "wm_start": ws, "wm_end": we}

def resolve_new_cycle(cfg):
    """Decide the date bound for a fresh cycle based on mode."""
    if cfg.mode == "daterange":
        ds, de = cfg.start_date, cfg.end_date
        where = date_where(cfg.date_filter_col, ds, de)
        return {"cycle_id": f"DR::{cfg.date_filter_col}::{ds}::{de}", "where": where,
                "advance_wm": False, "wm_start": None, "wm_end": None}
    # standard
    if cfg.watermark_col:
        low = get_high_watermark(cfg)
        if low is None:
            mn = ora_scalar(cfg, f"SELECT MIN({cfg.watermark_col}) FROM {cfg.oracle_schema}.{cfg.oracle_table}")
            if mn is None:
                return {"cycle_id": "EMPTY", "where": "1=0", "advance_wm": False,
                        "wm_start": None, "wm_end": None}
            low = mn.replace(hour=0, minute=0, second=0, microsecond=0)
        high = datetime.now().replace(microsecond=0)
        where = ts_where(cfg.watermark_col, low, high)
        return {"cycle_id": f"INC::{high.strftime('%Y%m%d_%H%M%S')}", "where": where,
                "advance_wm": True, "wm_start": low, "wm_end": high}
    return {"cycle_id": "FULL", "where": "", "advance_wm": False, "wm_start": None, "wm_end": None}

print("✅ engine: cycle resolution")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 7 — Row-count windowing (split PK range into ~rows_per_window chunks)

# COMMAND ----------
def pk_bounds(cfg, where):
    w = f" WHERE {where}" if where else ""
    pk_min, pk_max, cnt = ora_row(cfg, f"SELECT MIN({cfg.pk_col}),MAX({cfg.pk_col}),COUNT(*) "
                                       f"FROM {cfg.oracle_schema}.{cfg.oracle_table}{w}")
    return (int(pk_min) if pk_min is not None else None,
            int(pk_max) if pk_max is not None else None, int(cnt or 0))

def make_windows(cfg, where):
    pk_min, pk_max, total = pk_bounds(cfg, where)
    if total == 0 or pk_min is None:
        return [], total
    n = max(1, math.ceil(total / cfg.rows_per_window))
    step = max(1, math.ceil((pk_max - pk_min + 1) / n))
    wins, lo, idx = [], pk_min, 0
    while lo <= pk_max:
        hi = min(lo + step - 1, pk_max)
        wins.append({"window_idx": idx, "pk_lo": lo, "pk_hi": hi})
        lo = hi + 1; idx += 1
    print(f"  🪟 {total:,} rows → {len(wins)} windows (~{cfg.rows_per_window:,}/window)")
    return wins, total

def insert_pending(cfg, cyc, windows):
    if not windows: return
    def _ts(v): return f"TIMESTAMP '{v.strftime('%Y-%m-%d %H:%M:%S')}'" if v else "NULL"
    ws_sql, we_sql = _ts(cyc.get("wm_start")), _ts(cyc.get("wm_end"))
    adv = str(cyc["advance_wm"]).lower()
    # explicit column list → order-independent (survives ALTER ADD COLUMNS migrations).
    # where_clause intentionally left NULL; WHERE is always rebuilt from bounds.
    vals = ",\n".join(
        f"('{cfg.oracle_schema}','{cfg.oracle_table}','{cyc['cycle_id']}',{adv},"
        f"{ws_sql},{we_sql},{w['window_idx']},{w['pk_lo']},{w['pk_hi']},'PENDING',current_timestamp())"
        for w in windows)
    spark.sql(f"""INSERT INTO {cfg.window_control}
        (source_schema, source_table, cycle_id, advance_wm, wm_start, wm_end,
         window_idx, pk_lo, pk_hi, status, updated_ts)
        VALUES {vals}""")

def load_windows_from_control(cfg, cycle_id):
    rows = spark.sql(f"""SELECT window_idx, pk_lo, pk_hi, status FROM {cfg.window_control}
        WHERE source_schema='{cfg.oracle_schema}' AND source_table='{cfg.oracle_table}'
          AND cycle_id='{cycle_id}' ORDER BY window_idx""").collect()
    return [{"window_idx": r["window_idx"], "pk_lo": r["pk_lo"], "pk_hi": r["pk_hi"],
             "status": r["status"]} for r in rows]

def set_window_status(cfg, cycle_id, idx, oc, ec, status):
    spark.sql(f"""UPDATE {cfg.window_control}
        SET oracle_count={oc}, edl_count={ec}, status='{status}', updated_ts=current_timestamp()
        WHERE source_schema='{cfg.oracle_schema}' AND source_table='{cfg.oracle_table}'
          AND cycle_id='{cycle_id}' AND window_idx={idx}""")

print("✅ engine: windowing + checkpoint")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 8 — Engine A: JDBC partitioned read

# COMMAND ----------
def read_jdbc(cfg, where, pk_lo, pk_hi, num_parts):
    parts = [f"{cfg.pk_col} >= {pk_lo} AND {cfg.pk_col} <= {pk_hi}"]
    if where: parts.append(f"({where})")
    sub = f"(SELECT t.* FROM {cfg.oracle_schema}.{cfg.oracle_table} t WHERE {' AND '.join(parts)}) q"
    reader = (spark.read.format("jdbc").option("url", cfg.jdbc_url).option("dbtable", sub)
        .option("user", cfg.user).option("password", cfg.pwd)
        .option("driver", "oracle.jdbc.OracleDriver").option("fetchsize", cfg.fetch_size)
        .option("oracle.jdbc.timezoneAsRegion", "false")
        .option("partitionColumn", cfg.pk_col).option("lowerBound", str(pk_lo))
        .option("upperBound", str(pk_hi)).option("numPartitions", str(num_parts)))
    return reader.load()

print("✅ engine: JDBC reader")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 9 — Engine B: distributed python-oracledb (TYPED fast path)
# MAGIC Integers→Long, floats→Double stay **native** (no per-value `str()`); only
# MAGIC decimals / timestamps / strings are emitted as text and cast in Spark. This
# MAGIC is the main per-row speedup over the old all-string path.

# COMMAND ----------
def _oracledb_schema(cfg, ora_cols):
    """Build (spark output schema, per-column kind list)."""
    drop = {c.upper() for c in cfg.drop_columns}
    names, fields, kinds = [], [], []
    for c in ora_cols:
        if c["name"].upper() in drop: continue
        dt = (c["data_type"] or "").upper(); p, s = c["precision"], c["scale"]
        if cfg.oracledb_all_string:
            kind, st = "str", StringType()
        elif dt == "NUMBER" and p is None:
            kind, st = "dbl", DoubleType()
        elif dt == "NUMBER" and (s or 0) == 0 and p is not None and p <= 18:
            kind, st = "long", LongType()
        elif dt == "NUMBER":
            kind, st = "str", StringType()            # exact decimal → cast later
        elif dt in ("BINARY_DOUBLE", "FLOAT", "BINARY_FLOAT"):
            kind, st = "dbl", DoubleType()
        elif dt == "DATE" or dt.startswith("TIMESTAMP"):
            kind, st = "ts", StringType()
        else:
            kind, st = "str", StringType()
        names.append(c["name"]); fields.append(StructField(c["name"], st, True)); kinds.append(kind)
    return names, StructType(fields), kinds

def read_oracledb(cfg, where, pk_lo, pk_hi, num_parts, ora_cols):
    import pandas as pd
    names, out_schema, kinds = _oracledb_schema(cfg, ora_cols)
    sel = ", ".join(f"t.{n}" for n in names)
    hint = f"/*+ PARALLEL(t,{cfg.oracle_parallel_hint}) */" if cfg.oracle_parallel_hint > 0 else ""

    step = max(1, math.ceil((pk_hi - pk_lo + 1) / num_parts))
    ranges, lo = [], pk_lo
    while lo <= pk_hi:
        ranges.append((lo, min(lo + step - 1, pk_hi))); lo += step
    range_df = spark.createDataFrame(ranges, "lo long, hi long").repartition(len(ranges))

    _user, _pwd, _dsn = cfg.user, cfg.pwd, cfg.dsn
    _schema, _table, _where = cfg.oracle_schema, cfg.oracle_table, where
    _sel, _names, _kinds, _fetch, _pk, _hint = sel, names, kinds, cfg.fetch_size, cfg.pk_col, hint
    ncols = len(names)

    def fetch(pdf_iter):
        import oracledb as _odb
        conn = _odb.connect(user=_user, password=_pwd, dsn=_dsn)
        try:
            for pdf in pdf_iter:
                for r in pdf.itertuples(index=False):
                    lo_, hi_ = int(r.lo), int(r.hi)
                    cond = [f"{_pk} >= {lo_} AND {_pk} <= {hi_}"]
                    if _where: cond.append(f"({_where})")
                    q = f"SELECT {_hint} {_sel} FROM {_schema}.{_table} t WHERE " + " AND ".join(cond)
                    cur = conn.cursor(); cur.arraysize = _fetch; cur.execute(q)
                    while True:
                        batch = cur.fetchmany(_fetch)
                        if not batch: break
                        cols = [[None]*len(batch) for _ in range(ncols)]
                        for ri, row in enumerate(batch):
                            for ci in range(ncols):
                                v = row[ci]
                                if v is None: continue
                                k = _kinds[ci]
                                if k == "long": cols[ci][ri] = int(v)
                                elif k == "dbl": cols[ci][ri] = float(v)
                                elif k == "ts":
                                    # 4-digit zero-padded year — strftime("%Y") drops the
                                    # leading zeros for years < 1000 (e.g. Oracle garbage
                                    # date year 15 → "15") which then fails Spark's `yyyy`.
                                    if isinstance(v, datetime):
                                        cols[ci][ri] = (f"{v.year:04d}-{v.month:02d}-{v.day:02d} "
                                                        f"{v.hour:02d}:{v.minute:02d}:{v.second:02d}."
                                                        f"{v.microsecond:06d}")
                                    else:
                                        cols[ci][ri] = str(v)
                                else:
                                    if hasattr(v, "read"):
                                        try: cols[ci][ri] = str(v.read())
                                        except Exception: cols[ci][ri] = None
                                    elif isinstance(v, (bytes, bytearray)):
                                        cols[ci][ri] = v.decode("utf-8", "replace")
                                    else: cols[ci][ri] = str(v)
                        yield pd.DataFrame({_names[ci]: cols[ci] for ci in range(ncols)})
                    cur.close()
        finally:
            conn.close()

    return range_df.mapInPandas(fetch, schema=out_schema)

print("✅ engine: oracledb reader (typed)")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 10 — Idempotent write + count from Delta metrics

# COMMAND ----------
def _table_version(fq):
    try: return spark.sql(f"DESCRIBE HISTORY {fq} LIMIT 1").collect()[0]["version"]
    except Exception: return -1

def rows_written_since(fq, since):
    total = 0
    for r in spark.sql(f"DESCRIBE HISTORY {fq}").collect():
        if r["version"] <= since: continue
        op = (r["operation"] or "").upper(); m = r["operationMetrics"] or {}
        if op in ("WRITE","CREATE TABLE AS SELECT","REPLACE TABLE AS SELECT","CREATE OR REPLACE TABLE AS SELECT"):
            total += int(m.get("numOutputRows", 0) or 0)
        elif op == "MERGE":
            total += int(m.get("numTargetRowsInserted",0) or 0) + int(m.get("numTargetRowsUpdated",0) or 0)
    return total

def write_window(cfg, df, batch_id, cast_map):
    df = apply_cast_map(add_audit(df, cfg, batch_id), cast_map)
    if cfg.full_backfill:
        # #1 idempotent: clear any prior rows for this batch, then append
        spark.sql(f"DELETE FROM {cfg.fq_table} WHERE _BATCH_ID='{batch_id}'")
        ver = _table_version(cfg.fq_table)
        df.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(cfg.fq_table)
    else:
        ver = _table_version(cfg.fq_table)
        cond = " AND ".join(f"tgt.{pk}=src.{pk}" for pk in cfg.primary_keys)
        mb = DeltaTable.forName(spark, cfg.fq_table).alias("tgt").merge(df.alias("src"), cond)
        if cfg.watermark_col:
            mb = mb.whenMatchedUpdateAll(condition=f"src.{cfg.watermark_col} > tgt.{cfg.watermark_col}")
        else:
            mb = mb.whenMatchedUpdateAll()
        mb.whenNotMatchedInsertAll().execute()
    return rows_written_since(cfg.fq_table, ver)

print("✅ engine: idempotent writer")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 11 — Load one window (+ count gate inputs) & logging

# COMMAND ----------
def window_oracle_count(cfg, where, pk_lo, pk_hi):
    cond = [f"{cfg.pk_col} >= {pk_lo} AND {cfg.pk_col} <= {pk_hi}"]
    if where: cond.append(f"({where})")
    return int(ora_scalar(cfg, f"SELECT COUNT(*) FROM {cfg.oracle_schema}.{cfg.oracle_table} "
                               f"WHERE " + " AND ".join(cond)) or 0)

def write_log(cfg, batch_id, cyc, w, oc, ec, attempt, nparts, dur, status, err=""):
    rps = int(ec/dur) if dur > 0 else 0
    schema = StructType([
        StructField("batch_id",StringType()),StructField("source_schema",StringType()),
        StructField("source_table",StringType()),StructField("target_table",StringType()),
        StructField("cycle_id",StringType()),StructField("window_idx",IntegerType()),
        StructField("pk_lo",LongType()),StructField("pk_hi",LongType()),
        StructField("oracle_count",LongType()),StructField("edl_count",LongType()),
        StructField("count_match",BooleanType()),StructField("attempt_num",IntegerType()),
        StructField("num_partitions",IntegerType()),StructField("engine",StringType()),
        StructField("duration_sec",DoubleType()),StructField("rows_per_sec",LongType()),
        StructField("status",StringType()),StructField("error_msg",StringType()),
        StructField("load_ts",TimestampType())])
    row = [(batch_id, cfg.oracle_schema, cfg.oracle_table, cfg.fq_table, cyc["cycle_id"],
            int(w["window_idx"]), int(w["pk_lo"]), int(w["pk_hi"]), int(oc), int(ec),
            oc==ec, int(attempt), int(nparts), cfg.engine, round(dur,2), rps,
            status, str(err)[:500], datetime.now())]
    spark.createDataFrame(row, schema).write.format("delta").mode("append").saveAsTable(cfg.load_log)

def load_one_window(cfg, cyc, w, cast_map, ora_cols):
    start = datetime.now()
    oc = window_oracle_count(cfg, cyc["where"], w["pk_lo"], w["pk_hi"])
    if oc == 0:
        return 0, 0, 0.0, 0
    nparts = max(1, min(cfg.num_partitions, math.ceil(oc / 200_000)))
    batch_id = f"{cyc['cycle_id']}#W{w['window_idx']:05d}"
    if cfg.engine == "jdbc":
        df = read_jdbc(cfg, cyc["where"], w["pk_lo"], w["pk_hi"], nparts)
    else:
        df = read_oracledb(cfg, cyc["where"], w["pk_lo"], w["pk_hi"], nparts, ora_cols)
    ec = write_window(cfg, df, batch_id, cast_map)
    dur = (datetime.now() - start).total_seconds()
    return oc, ec, dur, nparts

print("✅ engine: load_one_window")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 12 — Reconciliation

# COMMAND ----------
def reconcile(cfg, where=""):
    print(f"\n  🔬 RECONCILE  {cfg.oracle_schema}.{cfg.oracle_table}  vs  {cfg.fq_table}")
    o_min, o_max, o_cnt = pk_bounds(cfg, where)
    edl = spark.table(cfg.fq_table)
    e_cnt = edl.count()
    e_min, e_max = edl.select(F.min(cfg.pk_col), F.max(cfg.pk_col)).first()
    e_min = int(e_min) if e_min is not None else None
    e_max = int(e_max) if e_max is not None else None
    ok_c, ok_lo, ok_hi = (o_cnt==e_cnt), (o_min==e_min), (o_max==e_max)
    print(f"    count : Oracle={o_cnt:,}  EDL={e_cnt:,}  {'✅' if ok_c else '❌'}")
    print(f"    PK min: {o_min} / {e_min}  {'✅' if ok_lo else '❌'}")
    print(f"    PK max: {o_max} / {e_max}  {'✅' if ok_hi else '❌'}")
    ok_s = True
    if cfg.recon_numeric_col:
        os_ = ora_scalar(cfg, f"SELECT SUM({cfg.recon_numeric_col}) FROM {cfg.oracle_schema}.{cfg.oracle_table}"
                              + (f" WHERE {where}" if where else ""))
        es_ = edl.select(F.sum(cfg.recon_numeric_col)).first()[0]
        ok_s = round(float(os_ or 0),2)==round(float(es_ or 0),2)
        print(f"    SUM({cfg.recon_numeric_col}): {os_} / {es_}  {'✅' if ok_s else '❌'}")
    passed = ok_c and ok_lo and ok_hi and ok_s
    print(f"    → {'✅ RECONCILED' if passed else '❌ MISMATCH'}")
    return passed

print("✅ engine: reconcile")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 13 — Orchestrator: run cycle with resume + auto window-chaining

# COMMAND ----------
def _run(user_cfg, mode):
    cfg = Cfg(user_cfg, mode)
    t0 = datetime.now()
    print(f"\n{'━'*65}\n  {mode.upper()} LOAD → {cfg.fq_table}\n  engine={cfg.engine} "
          f"rows/window={cfg.rows_per_window:,} parts/window={cfg.num_partitions}\n{'━'*65}")
    apply_spark_tuning(cfg)
    print(f"  ✅ Oracle SYSDATE={ora_scalar(cfg,'SELECT SYSDATE FROM DUAL')}")

    ensure_namespace(cfg); ensure_control_tables(cfg)
    build_and_create_target(cfg)
    cast_map = build_cast_map(cfg)
    ora_cols = get_oracle_columns(cfg)

    # ── Resolve cycle (resume incomplete first) ───────────────
    prefix = {"daterange": "DR::", "standard": "INC::"}.get(mode, "")
    cyc = find_resumable_cycle(cfg, prefix) if mode != "daterange" else None
    if cyc is None:
        cyc = resolve_new_cycle(cfg)
        if cyc["cycle_id"] in ("EMPTY",):
            print("  ✅ Nothing to load."); return "ALL_DONE"
        # For date-range the cycle_id is deterministic → it may already exist (resume)
        existing = load_windows_from_control(cfg, cyc["cycle_id"])
        if existing:
            print(f"  ♻️  Found existing windows for {cyc['cycle_id']} — resuming")
            windows = existing
        else:
            windows, total = make_windows(cfg, cyc["where"])
            if not windows:
                print("  ✅ 0 rows in range — nothing to load.")
                if cyc["advance_wm"] and cyc["wm_end"]: save_high_watermark(cfg, cyc["wm_end"])
                return "ALL_DONE"
            insert_pending(cfg, cyc, windows)
    else:
        windows = load_windows_from_control(cfg, cyc["cycle_id"])

    pending = [w for w in windows if w.get("status") != "SUCCESS"]
    print(f"  📦 cycle={cyc['cycle_id']} | total windows={len(windows)} | pending={len(pending)}")

    done = 0; stopped = False
    for w in pending:
        idx = w["window_idx"]
        print(f"\n  ── Window {idx+1} (pk {w['pk_lo']}→{w['pk_hi']}) ──")
        matched, attempt = False, 0
        oc = ec = nparts = 0; dur = 0.0
        while attempt < cfg.window_retry_max and not matched:
            attempt += 1
            try:
                oc, ec, dur, nparts = load_one_window(cfg, cyc, w, cast_map, ora_cols)
                rps = int(ec/dur) if dur > 0 else 0
                if oc == 0:
                    matched = True
                    print(f"    (empty)"); set_window_status(cfg, cyc["cycle_id"], idx, 0, 0, "SUCCESS")
                    write_log(cfg, f"{cyc['cycle_id']}#W{idx:05d}", cyc, w, 0,0, attempt, nparts, dur, "SUCCESS_EMPTY")
                elif oc == ec:                                   # ← count gate
                    matched = True
                    print(f"    ✅ MATCH {ec:,} | {dur:.1f}s | {rps:,} r/s")
                    set_window_status(cfg, cyc["cycle_id"], idx, oc, ec, "SUCCESS")
                    write_log(cfg, f"{cyc['cycle_id']}#W{idx:05d}", cyc, w, oc, ec, attempt, nparts, dur, "SUCCESS")
                else:
                    print(f"    ⚠️ MISMATCH Oracle={oc:,} EDL={ec:,}")
                    if attempt < cfg.window_retry_max: time.sleep(cfg.window_retry_wait)
            except Exception as e:
                print(f"    ❌ {str(e).splitlines()[0]}")   # first line only (skip JVM dump)
                write_log(cfg, f"{cyc['cycle_id']}#W{idx:05d}", cyc, w, oc, ec, attempt, nparts, dur, "FAILED", str(e))
                if attempt < cfg.window_retry_max: time.sleep(cfg.window_retry_wait)
        if matched:
            done += 1
            # auto-advance: loop continues to the next window automatically
        else:
            set_window_status(cfg, cyc["cycle_id"], idx, oc, ec, "FAILED")
            print(f"    🛑 Window {idx+1} unresolved → stop. Re-run to resume only this window.")
            stopped = True
            break

    # ── Cycle completion ──────────────────────────────────────
    remaining = spark.sql(f"""SELECT COUNT(*) c FROM {cfg.window_control}
        WHERE source_schema='{cfg.oracle_schema}' AND source_table='{cfg.oracle_table}'
          AND cycle_id='{cyc['cycle_id']}' AND status<>'SUCCESS'""").collect()[0]["c"]

    if remaining == 0:
        if cfg.full_backfill:
            print(f"\n  ⏳ OPTIMIZE {cfg.fq_table} ..."); spark.sql(f"OPTIMIZE {cfg.fq_table}")
        if cyc["advance_wm"] and cyc["wm_end"]:
            save_high_watermark(cfg, cyc["wm_end"]); print("  💾 watermark advanced")
        reconcile(cfg, cyc["where"])

    dur = (datetime.now() - t0).total_seconds()
    print(f"\n{'━'*65}\n  {'✅ CYCLE COMPLETE' if remaining==0 else '⚠️ STOPPED/PENDING'} "
          f"| windows done this run={done} | {dur/60:.1f} min\n{'━'*65}")
    return "ALL_DONE" if remaining == 0 else "MORE_PENDING"

def run_standard(user_cfg):              return _run(user_cfg, "standard")
def run_oracle_to_edl_daterange(user_cfg): return _run(user_cfg, "daterange")
run_type1 = run_oracle_to_edl_daterange   # backward-compatible alias

# COMMAND ----------
# MAGIC %md
# MAGIC ### Cell 14 — EDL STG → FINAL  (checksum-based insert/update merge)
# MAGIC Delta-to-Delta only (no Oracle). Reads ALL rows from `source_table`, computes
# MAGIC a row **checksum over the business columns** (PK + audit columns excluded),
# MAGIC and MERGEs into `target_table`:
# MAGIC - **new PK** → insert
# MAGIC - **existing PK, checksum changed** → update
# MAGIC - **existing PK, checksum same** → skipped (no rewrite)
# MAGIC
# MAGIC Inputs needed: `source_table`, `target_table`, `primary_keys`
# MAGIC (+ optional `checksum_exclude_cols` for audit columns like `AZ_INSERT_TS`).

# COMMAND ----------
def _row_checksum(df, exclude_upper):
    """sha2-256 over all columns except excluded ones, NULL-safe, order-stable."""
    hash_cols = sorted([c for c in df.columns if c.upper() not in exclude_upper])
    expr = F.sha2(F.concat_ws("‖",
        *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in hash_cols]), 256)
    return expr, hash_cols

def run_stg_to_final(cfg):
    src_name = cfg["source_table"]
    tgt_name = cfg["target_table"]
    pks      = cfg["primary_keys"]
    if not (src_name and tgt_name and pks):
        raise ValueError("source_table, target_table and primary_keys are required")
    # PKs + audit columns are excluded from the checksum (but still stored/updated)
    excl = {c.upper() for c in cfg.get("checksum_exclude_cols", [])} | {p.upper() for p in pks}
    excl.add("_CHECKSUM")

    t0 = datetime.now()
    print(f"\n{'━'*65}\n  STG → FINAL  (checksum merge)\n"
          f"  src : {src_name}\n  tgt : {tgt_name}\n  PK  : {pks}\n{'━'*65}")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")

    src = spark.table(src_name)
    chk, hash_cols = _row_checksum(src, excl)
    print(f"  🔢 checksum over {len(hash_cols)} cols | excluded {sorted(excl)}")
    src_h = src.withColumn("_CHECKSUM", chk)

    # ── First time: create the final table directly ──────────
    if not spark.catalog.tableExists(tgt_name):
        print("  🆕 target missing → initial full load")
        (src_h.write.format("delta").mode("overwrite")
              .option("overwriteSchema", "true").saveAsTable(tgt_name))
        n = spark.table(tgt_name).count()
        print(f"  ✅ initial load | rows={n:,} | {(datetime.now()-t0).total_seconds():.1f}s")
        return "INITIAL_LOAD"

    # ── Ensure target carries a _CHECKSUM column ─────────────
    if "_CHECKSUM" not in [c.upper() for c in spark.table(tgt_name).columns]:
        print("  ➕ adding _CHECKSUM column to existing target")
        spark.sql(f"ALTER TABLE {tgt_name} ADD COLUMNS (_CHECKSUM STRING)")

    ver  = _table_version(tgt_name)
    cond = " AND ".join(f"t.{pk}=s.{pk}" for pk in pks)
    (DeltaTable.forName(spark, tgt_name).alias("t")
        .merge(src_h.alias("s"), cond)
        .whenMatchedUpdateAll(condition="t._CHECKSUM IS NULL OR t._CHECKSUM <> s._CHECKSUM")
        .whenNotMatchedInsertAll()
        .execute())

    ins = upd = 0
    for r in spark.sql(f"DESCRIBE HISTORY {tgt_name}").collect():
        if r["version"] <= ver: continue
        if (r["operation"] or "").upper() == "MERGE":
            m = r["operationMetrics"] or {}
            ins += int(m.get("numTargetRowsInserted", 0) or 0)
            upd += int(m.get("numTargetRowsUpdated", 0) or 0)
    print(f"  ✅ MERGE done | inserted={ins:,} updated={upd:,} "
          f"| {(datetime.now()-t0).total_seconds():.1f}s")
    return "MERGED"

print("✅ engine ready — run_standard(CONFIG) | run_oracle_to_edl_daterange(CONFIG) | run_stg_to_final(CONFIG)")
