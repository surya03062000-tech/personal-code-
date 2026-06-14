-- =====================================================================================
-- ERP - BICC Excel Ingestion : DDLs + sample metadata for ONE file test
-- Catalog/schema: edl_prod.drvd__app_bicc
-- load_type table is intentionally REMOVED (no longer required).
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- 1) METADATA TABLE  (drives sheet list, column mapping, data types, primary keys)
--    New columns vs the old table:
--      sheet_tab_name        -> the Excel tab name to read
--      sheet_column_name     -> the header as it appears IN the sheet
--      original_column_name  -> the column name to use in the final table (can differ)
--      array_column_name     -> the key used for this column inside the JSON DATA column
--    is_nullable is kept ONLY because the NULL-PK check needs it (required column).
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS edl_prod.drvd__app_bicc.bicc_table_metadata (
    serial_number          BIGINT,
    batch_id               STRING,
    file_name_prefix       STRING,
    sheet_tab_name         STRING,
    table_name             STRING,
    sheet_column_name      STRING,
    original_column_name   STRING,
    array_column_name      STRING,
    column_order           INT,
    data_type              STRING,
    is_primary_key         BOOLEAN,
    is_nullable            BOOLEAN,
    _az_insert_ts          TIMESTAMP,
    _az_update_ts          TIMESTAMP
) USING DELTA;


-- -------------------------------------------------------------------------------------
-- 2) PROCESS CONTROL  (one row PER TAB / TABLE per file)
--      raw_parquet_path            -> as-is sheet->parquet landing path  (your "csv file name")
--      dq_check_validation         -> ARRAY of the three PK check results
--      final_parquet_source_raw    -> final parquet path/status for the table
--      final_ingestion_status      -> Succeeded only when all DQ checks pass
--      comments                    -> failure reason when not Succeeded
--    record_count / error_count / batch_id / file_dttm added as useful standards.
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS edl_prod.drvd__app_bicc.bicc_process_control (
    execution_id               STRING,
    process_id                 STRING,
    batch_id                   STRING,
    file_name                  STRING,
    sheet_tab_name             STRING,
    table_name                 STRING,
    raw_parquet_path           STRING,
    dq_check_validation        ARRAY<STRING>,
    record_count               BIGINT,
    error_count                BIGINT,
    final_parquet_source_raw   STRING,
    final_ingestion_status     STRING,
    comments                   STRING,
    file_dttm                  TIMESTAMP,
    _az_insert_ts              TIMESTAMP
) USING DELTA;


-- -------------------------------------------------------------------------------------
-- 3) ERROR TABLE  (same columns/order as before + TABLE_NAME added)
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS edl_prod.drvd__app_bicc.bicc_ingestion_err_table (
    EXECUTION_ID    STRING,
    PROCESS_ID      STRING,
    FILE_NAME       STRING,
    TABLE_NAME      STRING,
    FILE_PATH       STRING,
    ERR_RECORD      STRING,
    COMMENTS        STRING,
    AZ_INSERT_TS    TIMESTAMP
) USING DELTA;


-- =====================================================================================
-- SAMPLE METADATA ENTRIES FOR ONE FILE TEST
-- File          : file_erp_bicc_inv-batch240790827-20260614_100433.xlsx
-- file_prefix   : file_erp_bicc_inv   (derived by stripping -batch...-...)
-- Tabs (2)      : INV_HEADER -> inv_header , INV_DETAIL -> inv_detail
-- =====================================================================================
INSERT INTO edl_prod.drvd__app_bicc.bicc_table_metadata
(serial_number, batch_id, file_name_prefix, sheet_tab_name, table_name, sheet_column_name,
 original_column_name, array_column_name, column_order, data_type, is_primary_key, is_nullable,
 _az_insert_ts, _az_update_ts) VALUES
-- ---- Tab: INV_HEADER (PK = INVOICE_ID) ----
(1,  '240790827', 'file_erp_bicc_inv', 'INV_HEADER', 'inv_header', 'Invoice Id',     'INVOICE_ID',   'invoice_id',   1, 'bigint',        true,  false, current_timestamp(), current_timestamp()),
(2,  '240790827', 'file_erp_bicc_inv', 'INV_HEADER', 'inv_header', 'Invoice Number', 'INVOICE_NUM',  'invoice_num',  2, 'string',        false, true,  current_timestamp(), current_timestamp()),
(3,  '240790827', 'file_erp_bicc_inv', 'INV_HEADER', 'inv_header', 'Invoice Date',   'INVOICE_DATE', 'invoice_date', 3, 'date',          false, true,  current_timestamp(), current_timestamp()),
(4,  '240790827', 'file_erp_bicc_inv', 'INV_HEADER', 'inv_header', 'Amount',         'AMOUNT',       'amount',       4, 'decimal(18,2)', false, true,  current_timestamp(), current_timestamp()),
(5,  '240790827', 'file_erp_bicc_inv', 'INV_HEADER', 'inv_header', 'Supplier Name',  'SUPPLIER_NAME','supplier_name',5, 'string',        false, true,  current_timestamp(), current_timestamp()),
-- ---- Tab: INV_DETAIL (composite PK = INVOICE_ID + LINE_ID) ----
(6,  '240790827', 'file_erp_bicc_inv', 'INV_DETAIL', 'inv_detail', 'Invoice Id', 'INVOICE_ID', 'invoice_id', 1, 'bigint',        true,  false, current_timestamp(), current_timestamp()),
(7,  '240790827', 'file_erp_bicc_inv', 'INV_DETAIL', 'inv_detail', 'Line Id',    'LINE_ID',    'line_id',    2, 'bigint',        true,  false, current_timestamp(), current_timestamp()),
(8,  '240790827', 'file_erp_bicc_inv', 'INV_DETAIL', 'inv_detail', 'Item',       'ITEM',       'item',       3, 'string',        false, true,  current_timestamp(), current_timestamp()),
(9,  '240790827', 'file_erp_bicc_inv', 'INV_DETAIL', 'inv_detail', 'Quantity',   'QTY',        'qty',        4, 'int',           false, true,  current_timestamp(), current_timestamp()),
(10, '240790827', 'file_erp_bicc_inv', 'INV_DETAIL', 'inv_detail', 'Unit Price', 'UNIT_PRICE', 'unit_price', 5, 'decimal(18,2)', false, true,  current_timestamp(), current_timestamp());
