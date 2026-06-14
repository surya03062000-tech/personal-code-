-- =====================================================================================================
-- ERP - BICC EXCEL INGESTION  |  DDLs + one-file test metadata
-- Catalog/schema: edl_dev.drvd__app_ctdi
-- =====================================================================================================

-- -----------------------------------------------------------------------------------------------------
-- 0) Remove load_type table - no longer required (full/delta + count checks were dropped)
-- ----------------------------------------------------------------------------------------------------


-- -----------------------------------------------------------------------------------------------------
-- 1) METADATA TABLE  (drives, per tab, the rename + PK-only validation + JSON folding)
--    Key = (file_name_prefix, sheet_tab_name).  One row per source column.
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS edl_dev.drvd__app_ctdi.bicc_table_metadata (
  serial_number         BIGINT,        -- surrogate row id (optional)
  batch_id              STRING,        -- feed/batch grouping label
  file_name_prefix      STRING,        -- file name with the trailing date stripped (e.g. Rogers_Shaw_STB_OHB_Comparison)
  sheet_tab_name        STRING,        -- NEW: exact Excel tab name (e.g. 'OHB Comparison')
  table_name            STRING,        -- target table for this tab
  sheet_column_name     STRING,        -- column header as it appears in the sheet
  original_column_name  STRING,        -- NEW: final/canonical column name (rename target; usually = sheet name)
  array_column_name     STRING,        -- NEW: name of the JSON/array column that holds all non-PK columns
  column_order          INT,           -- 1-based column order
  data_type             STRING,        -- target Spark datatype (string, int, bigint, date, timestamp, decimal(18,2)...)
  is_primary_key        BOOLEAN,       -- TRUE = kept as-is + validated (dup/null/datatype). FALSE = folded into JSON.
  _az_insert_ts         TIMESTAMP,
  _az_update_ts         TIMESTAMP
) USING DELTA;


-- -----------------------------------------------------------------------------------------------------
-- 2) PROCESS CONTROL  (one row per tab/table)
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS edl_dev.drvd__app_ctdi.bicc_process_control (
  execution_id              STRING,         -- run id
  process_id                STRING,         -- UNIQUE per tab/table (same id used in the error table for that tab)
  batch_id                  STRING,         -- yyyymmddHHMMSS parsed from the file name
  file_name                 STRING,         -- workbook file name
  sheet_tab_name            STRING,         -- excel tab
  table_name                STRING,         -- target table
  source_file_path          STRING,         -- decrypted source (xlsx) path - abfss://raw@.../erp/...
  raw_parquet_path          STRING,         -- as-is landing parquet path
  final_parquet_source_raw  STRING,         -- curated parquet path (what 100_databricks consumes)
  dq_check_validation       ARRAY<STRING>,  -- [duplicate result, not-null result, datatype result]
  source_row_count          BIGINT,
  valid_record_count        BIGINT,
  error_record_count        BIGINT,
  final_ingestion_status    STRING,         -- Succeeded (all PK checks pass) | Failed
  comments                  STRING,         -- failure reason / summary
  file_dttm                 TIMESTAMP,
  _az_insert_ts             TIMESTAMP
) USING DELTA;


-- -----------------------------------------------------------------------------------------------------
-- 3) ERROR TABLE  (bad records; all errors of one tab share that tab's process_id)
--    Column order as requested: PROCESS_ID, FILE_NAME, TABLE_NAME, FILE_PATH, ERR_RECORD, COMMENTS, AZ_INSERT_TS
--    EXECUTION_ID + SHEET_TAB_NAME added as required audit columns (tie an error back to a run/tab).
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS edl_dev.drvd__app_ctdi.bicc_ingestion_err_table (
  process_id      STRING,
  execution_id    STRING,
  file_name       STRING,
  table_name      STRING,
  sheet_tab_name  STRING,
  file_path       STRING,
  err_record      STRING,     -- full bad row as JSON
  comments        STRING,     -- which PK check failed
  az_insert_ts    TIMESTAMP
) USING DELTA;


-- =====================================================================================================
-- 4) ONE-FILE TEST METADATA  ->  Rogers_Shaw_STB_OHB_Comparison_2026.06.10.xlsx  (2 tabs)
--    Demonstrates: multi-tab -> multi-table, composite PK, numeric-PK datatype check, JSON folding.
-- =====================================================================================================

-- ---- Tab 1 : 'OHB Comparison'  ->  table ohb_comparison  (PK = SERIAL_NUMBER + CTDI_SERIAL_NUMBER_ID) ----
INSERT INTO edl_dev.drvd__app_ctdi.bicc_table_metadata
(serial_number, batch_id, file_name_prefix, sheet_tab_name, table_name,
 sheet_column_name, original_column_name, array_column_name, column_order, data_type, is_primary_key,
 _az_insert_ts, _az_update_ts)
VALUES
(1 ,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','ACTION'                ,'ACTION'                ,'DATA',1 ,'string' ,false,current_timestamp(),current_timestamp()),
(2 ,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','CTDI_COMP_DATE'        ,'CTDI_COMP_DATE'        ,'DATA',2 ,'date'   ,false,current_timestamp(),current_timestamp()),
(3 ,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','SHAW_COMP_DATE'        ,'SHAW_COMP_DATE'        ,'DATA',3 ,'date'   ,false,current_timestamp(),current_timestamp()),
(4 ,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','SERIAL_NUMBER'         ,'SERIAL_NUMBER'         ,'DATA',4 ,'string' ,true ,current_timestamp(),current_timestamp()),
(5 ,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','ADDITIONAL1'           ,'ADDITIONAL1'           ,'DATA',5 ,'string' ,false,current_timestamp(),current_timestamp()),
(6 ,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','COMPARISON'            ,'COMPARISON'            ,'DATA',6 ,'string' ,false,current_timestamp(),current_timestamp()),
(7 ,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','SHAW_STATION'          ,'SHAW_STATION'          ,'DATA',7 ,'string' ,false,current_timestamp(),current_timestamp()),
(8 ,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','SHAW_STOCK_LOCATION'   ,'SHAW_STOCK_LOCATION'   ,'DATA',8 ,'string' ,false,current_timestamp(),current_timestamp()),
(9 ,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','CTDI_STOCK_LOCATION'   ,'CTDI_STOCK_LOCATION'   ,'DATA',9 ,'string' ,false,current_timestamp(),current_timestamp()),
(10,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','INVENTORY_CLASS'       ,'INVENTORY_CLASS'       ,'DATA',10,'string' ,false,current_timestamp(),current_timestamp()),
(11,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','CTDI_SERIAL_NUMBER_ID' ,'CTDI_SERIAL_NUMBER_ID' ,'DATA',11,'bigint' ,true ,current_timestamp(),current_timestamp()),
(12,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','STATION_ID'            ,'STATION_ID'            ,'DATA',12,'string' ,false,current_timestamp(),current_timestamp()),
(13,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','PALLET_ID'             ,'PALLET_ID'             ,'DATA',13,'string' ,false,current_timestamp(),current_timestamp()),
(14,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','BIN_ID'                ,'BIN_ID'                ,'DATA',14,'string' ,false,current_timestamp(),current_timestamp()),
(15,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','PART_NUMBER'           ,'PART_NUMBER'           ,'DATA',15,'string' ,false,current_timestamp(),current_timestamp()),
(16,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','PART_TYPE'             ,'PART_TYPE'             ,'DATA',16,'string' ,false,current_timestamp(),current_timestamp()),
(17,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','RECEIVE_DATE'          ,'RECEIVE_DATE'          ,'DATA',17,'date'   ,false,current_timestamp(),current_timestamp()),
(18,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','RECEIVE_YEAR_SORT'     ,'RECEIVE_YEAR_SORT'     ,'DATA',18,'int'    ,false,current_timestamp(),current_timestamp()),
(19,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','RECEIVE_RMA'           ,'RECEIVE_RMA'           ,'DATA',19,'string' ,false,current_timestamp(),current_timestamp()),
(20,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','RECEIVE_LOCATION_ID'   ,'RECEIVE_LOCATION_ID'   ,'DATA',20,'string' ,false,current_timestamp(),current_timestamp()),
(21,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','RECEIVE_WAYBILL'       ,'RECEIVE_WAYBILL'       ,'DATA',21,'string' ,false,current_timestamp(),current_timestamp()),
(22,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','SHIP_DATE'             ,'SHIP_DATE'             ,'DATA',22,'date'   ,false,current_timestamp(),current_timestamp()),
(23,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','SHIP_YEAR_SORT'        ,'SHIP_YEAR_SORT'        ,'DATA',23,'int'    ,false,current_timestamp(),current_timestamp()),
(24,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','SHIP_WAYBILL'          ,'SHIP_WAYBILL'          ,'DATA',24,'string' ,false,current_timestamp(),current_timestamp()),
(25,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','SHIP_ORDER_NUMBER'     ,'SHIP_ORDER_NUMBER'     ,'DATA',25,'string' ,false,current_timestamp(),current_timestamp()),
(26,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','CUSTOMER_ORDER_NUMBER' ,'CUSTOMER_ORDER_NUMBER' ,'DATA',26,'string' ,false,current_timestamp(),current_timestamp()),
(27,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','ORDER_TYPE'            ,'ORDER_TYPE'            ,'DATA',27,'string' ,false,current_timestamp(),current_timestamp()),
(28,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Comparison','ohb_comparison','RECEIVE_SHIP_YEAR_SORT','RECEIVE_SHIP_YEAR_SORT','DATA',28,'int'    ,false,current_timestamp(),current_timestamp());

-- ---- Tab 2 : 'OHB Nonserial Comparison'  ->  table ohb_nonserial_comparison  (PK = PART_NUMBER + STOCK_LOCATION_ID) ----
INSERT INTO edl_dev.drvd__app_ctdi.bicc_table_metadata
(serial_number, batch_id, file_name_prefix, sheet_tab_name, table_name,
 sheet_column_name, original_column_name, array_column_name, column_order, data_type, is_primary_key,
 _az_insert_ts, _az_update_ts)
VALUES
(29,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Nonserial Comparison','ohb_nonserial_comparison','CTDI_COMP_DATE'   ,'CTDI_COMP_DATE'   ,'DATA',1,'date'  ,false,current_timestamp(),current_timestamp()),
(30,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Nonserial Comparison','ohb_nonserial_comparison','SHAW_COMP_DATE'   ,'SHAW_COMP_DATE'   ,'DATA',2,'date'  ,false,current_timestamp(),current_timestamp()),
(31,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Nonserial Comparison','ohb_nonserial_comparison','PART_NUMBER'      ,'PART_NUMBER'      ,'DATA',3,'string',true ,current_timestamp(),current_timestamp()),
(32,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Nonserial Comparison','ohb_nonserial_comparison','STOCK_LOCATION_ID','STOCK_LOCATION_ID','DATA',4,'string',true ,current_timestamp(),current_timestamp()),
(33,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Nonserial Comparison','ohb_nonserial_comparison','PARTTYPE'         ,'PARTTYPE'         ,'DATA',5,'string',false,current_timestamp(),current_timestamp()),
(34,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Nonserial Comparison','ohb_nonserial_comparison','CTDI_QUANTITY'    ,'CTDI_QUANTITY'    ,'DATA',6,'int'   ,false,current_timestamp(),current_timestamp()),
(35,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Nonserial Comparison','ohb_nonserial_comparison','SHAW_QUANTITY'    ,'SHAW_QUANTITY'    ,'DATA',7,'int'   ,false,current_timestamp(),current_timestamp()),
(36,'BICC_INT_680','Rogers_Shaw_STB_OHB_Comparison','OHB Nonserial Comparison','ohb_nonserial_comparison','DELTA_QUANTITY'   ,'DELTA_QUANTITY'   ,'DATA',8,'int'   ,false,current_timestamp(),current_timestamp());


-- =====================================================================================================
-- 5) Verify
-- =====================================================================================================
-- SELECT sheet_tab_name, table_name, count(*) cols,
--        concat_ws(',', collect_list(case when is_primary_key then original_column_name end)) pk
-- FROM edl_dev.drvd__app_ctdi.bicc_table_metadata
-- WHERE file_name_prefix='Rogers_Shaw_STB_OHB_Comparison'
-- GROUP BY sheet_tab_name, table_name;
