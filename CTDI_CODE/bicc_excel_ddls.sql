-- =====================================================================================================
-- ERP - BICC EXCEL INGESTION  |  DDLs + one-file test metadata
-- Canonical schema: edl_prod.drvd__app_bicc   (test env used: drvd__app_ctdi - swap the schema as needed)
--
-- Model summary:
--   * Metadata holds ONLY the PRIMARY-KEY columns (no column_order).
--   * Non-PK columns are read straight from the sheet, kept as STRING, and folded into ONE
--     VARIANT column ("DATA") - so 5 columns today / 10 tomorrow needs NO DDL or metadata change.
--   * Only PK columns are validated (duplicate / not-null / datatype).
-- =====================================================================================================

-- -----------------------------------------------------------------------------------------------------
-- 0) Remove load_type table - no longer required
-- -----------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS edl_prod.drvd__app_bicc.bicc_load_type;


-- -----------------------------------------------------------------------------------------------------
-- 1) METADATA TABLE  -  ONE ROW PER PRIMARY-KEY COLUMN ONLY  (column_order removed)
--    Key = (file_name_prefix, sheet_tab_name).
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS edl_prod.drvd__app_bicc.bicc_table_metadata (
  serial_number         BIGINT,        -- surrogate id; also keeps composite PK order deterministic
  batch_id              STRING,        -- feed/batch grouping label
  file_name_prefix      STRING,        -- file name with the trailing date stripped (e.g. Rogers_Shaw_STB_OHB_Comparison)
  sheet_tab_name        STRING,        -- exact Excel tab name (e.g. 'OHB Comparison')
  table_name            STRING,        -- target table for this tab
  sheet_column_name     STRING,        -- PK column header as it appears in the sheet
  original_column_name  STRING,        -- final/canonical PK name (rename target; usually = sheet name)
  array_column_name     STRING,        -- name of the VARIANT column that holds ALL non-PK columns (e.g. DATA)
  data_type             STRING,        -- PK target datatype (string, bigint, int, date, timestamp, decimal(18,2)...)
  is_primary_key        BOOLEAN,       -- always TRUE here (only PK rows are stored)
  _az_insert_ts         TIMESTAMP,
  _az_update_ts         TIMESTAMP
) USING DELTA;


-- -----------------------------------------------------------------------------------------------------
-- 2) PROCESS CONTROL  (one row per tab/table)
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS edl_prod.drvd__app_bicc.bicc_process_control (
  execution_id              STRING,
  process_id                STRING,         -- UNIQUE per tab/table (same id used in the error table for that tab)
  batch_id                  STRING,
  file_name                 STRING,
  sheet_tab_name            STRING,
  table_name                STRING,
  source_file_path          STRING,
  raw_parquet_path          STRING,
  final_parquet_source_raw  STRING,         -- curated parquet path (what 100_databricks consumes)
  dq_check_validation       ARRAY<STRING>,  -- [duplicate result, not-null result, datatype result]
  source_row_count          BIGINT,
  valid_record_count        BIGINT,
  error_record_count        BIGINT,
  final_ingestion_status    STRING,         -- Succeeded (all PK checks pass) | Failed
  comments                  STRING,
  file_dttm                 TIMESTAMP,
  _az_insert_ts             TIMESTAMP
) USING DELTA;


-- -----------------------------------------------------------------------------------------------------
-- 3) ERROR TABLE  (one DETAILED row per rejected record; all rows of a tab share its process_id)
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS edl_prod.drvd__app_bicc.bicc_ingestion_err_table (
  process_id        STRING,
  execution_id      STRING,
  file_name         STRING,
  table_name        STRING,
  sheet_tab_name    STRING,
  file_path         STRING,
  error_category    STRING,     -- which PK check(s) failed (e.g. 'Duplicate Primary Key')
  error_description STRING,     -- full human-readable sentence
  pk_value          STRING,     -- the offending PK value(s) e.g. PART_NUMBER=... | STOCK_LOCATION_ID=...
  pk_derived        STRING,     -- md5 derived key of the bad record
  err_record        STRING,     -- the ENTIRE bad row as JSON
  comments          STRING,     -- raw concatenated failure reason(s)
  az_insert_ts      TIMESTAMP
) USING DELTA;


-- =====================================================================================================
-- 4) ONE-FILE TEST METADATA  ->  Rogers_Shaw_STB_OHB_Comparison  (2 tabs)  -  PK COLUMNS ONLY
--    Tab 'OHB Comparison'          -> ohb_comparison           PK = SERIAL_NUMBER + CTDI_SERIAL_NUMBER_ID
--    Tab 'OHB Nonserial Comparison'-> ohb_nonserial_comparison PK = PART_NUMBER  + STOCK_LOCATION_ID
--    (every other column is read from the sheet and folded into the DATA VARIANT column automatically)
-- =====================================================================================================
INSERT INTO edl_prod.drvd__app_bicc.bicc_table_metadata
(serial_number, batch_id, file_name_prefix, sheet_tab_name, table_name,
 sheet_column_name, original_column_name, array_column_name, data_type, is_primary_key,
 _az_insert_ts, _az_update_ts)
VALUES
-- Tab 1 PKs
(1,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison',
   'SERIAL_NUMBER'        ,'SERIAL_NUMBER'        ,'DATA','string',true,current_timestamp(),current_timestamp()),
(2,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison',
   'CTDI_SERIAL_NUMBER_ID','CTDI_SERIAL_NUMBER_ID','DATA','bigint',true,current_timestamp(),current_timestamp()),
-- Tab 2 PKs
(3,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Nonserial Comparison','ohb_nonserial_comparison',
   'PART_NUMBER'      ,'PART_NUMBER'      ,'DATA','string',true,current_timestamp(),current_timestamp()),
(4,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Nonserial Comparison','ohb_nonserial_comparison',
   'STOCK_LOCATION_ID','STOCK_LOCATION_ID','DATA','string',true,current_timestamp(),current_timestamp());


-- =====================================================================================================
-- 5) MIGRATION (if you already created the OLD tables)
-- =====================================================================================================
-- ALTER TABLE edl_prod.drvd__app_bicc.bicc_table_metadata DROP COLUMN column_order;
-- DELETE FROM edl_prod.drvd__app_bicc.bicc_table_metadata WHERE is_primary_key = false;   -- keep PK rows only
-- ALTER TABLE edl_prod.drvd__app_bicc.bicc_ingestion_err_table
--   ADD COLUMNS (error_category STRING, error_description STRING, pk_value STRING, pk_derived STRING);


-- =====================================================================================================
-- 6) Verify
-- =====================================================================================================
-- SELECT sheet_tab_name, table_name,
--        concat_ws(' + ', collect_list(original_column_name)) AS primary_key
-- FROM edl_prod.drvd__app_bicc.bicc_table_metadata
-- WHERE file_name_prefix = 'Rogers_Shaw_STB_OHB_Comparison'
-- GROUP BY sheet_tab_name, table_name;
