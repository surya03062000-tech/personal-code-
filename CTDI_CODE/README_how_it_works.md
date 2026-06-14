# ERP – BICC Excel Ingestion — how it works

## Files in this delivery
| file | purpose |
|---|---|
| `erp_bicc_excel_ingestion.py` | the Databricks notebook (cell-by-cell, each cell titled) |
| `bicc_excel_ddls.sql` | DDLs for the 3 tables + drop of `bicc_load_type` + one-file test metadata |
| `sample_output_ohb_nonserial_comparison.md` | one curated output file + matching control/error rows |

## Config-table entry (the `parameter` JSON) for the one-file test
```json
{"storage":{"adls_container":"raw","folder":"erp","source":"bicc_INT_680","frequency":"hourly"},
 "decryption":{"private_key":"${erp_private_key}","scope_nm":"${erp_keyvault_scope_nm}","passphrase_key":"${erp_passphrase_key}"},
 "processing":{"decrypt_flag":"false","test_excel_path":"dbfs:/FileStore/erp_bicc_test/Rogers_Shaw_STB_OHB_Comparison_2026.06.10.xlsx","header_row_index":"1","data_start_row_index":"2"},
 "validations":{"metadata_table":"drvd__app_bicc.bicc_table_metadata","error_table":"drvd__app_bicc.bicc_ingestion_err_table","process_control":"drvd__app_bicc.bicc_process_control"},
 "email":{"sender":"${erp_email_sender}","receivers":"${erp_email_receivers}","server":"${erp_email_server}","subject":"Failed | ERP - BICC Excel File INT 680"}}
```
`config` lines stay the same as the old job (SFTP/key-vault). `load_type` is removed from `validations`.

## End-to-end flow
```
list_workbooks()                      TEST: read test_excel_path | PROD: latest dated folder -> GPG-decrypt *.xlsx.gpg
   |
   for each workbook:
      derive_file_meta()              file_name_prefix, batch_id, file_dttm  (from the file name)
      read_excel_sheets()            calamine reads EVERY tab; header = 2nd row (row-1 is the CTDI title banner)
         |
         for each tab (ISOLATED in try/except):
            get_sheet_metadata()      (file_prefix, sheet_tab) -> columns / PK / array col   (no metadata -> skip tab)
            write raw as-is parquet   lineage landing
            apply_column_mapping()    sheet_column_name -> original_column_name (+ keep order)
            run_pk_validations()      duplicate PK, NOT-NULL PK, datatype PK  (PK-only; no count/non-PK checks)
            bad records  -> error table (same process_id for the whole tab)
            good records -> build_curated_df(): PK as-is + non-PK folded into ONE JSON `DATA` col + metadata cols
                          -> write curated parquet
            collect one control row
   |
   write all control rows -> bicc_process_control
   send_failure_email() and fail the job if any tab failed
```

## Why these choices
- **No openpyxl** → `python-calamine` (Rust, much faster, lists tabs natively). Read once on the driver, build Spark DFs.
- **Title row** → CTDI sheets put a banner on row 1 and the real header on row 2, so `header_row_index=1` (0-based).
- **PK-only validation** → duplicate / NOT-NULL / datatype only. Column-count, record-count and non-PK checks
  were **removed** (and `bicc_load_type` dropped).
- **Per-tab isolation** → each tab runs in its own `try/except`; a failure in one tab still lets the other 14 run,
  pushes that tab’s bad rows to the error table, and includes it in the failure e-mail.
- **Performance** → metadata table cached once; all DQ counts computed in a single aggregation; curated parquet
  built from the in-memory validated DF (no re-read); single-file `.parquet` output via `repartition(1)`.

## How to test (decrypt_flag = false)
1. Run `bicc_excel_ddls.sql` (creates 3 tables, drops load_type, inserts the Rogers_Shaw metadata).
2. Upload the sample workbook to DBFS, e.g. `dbfs:/FileStore/erp_bicc_test/Rogers_Shaw_STB_OHB_Comparison_2026.06.10.xlsx`.
3. Set the widget `notebook_config` to the JSON above (`decrypt_flag:"false"`), `execution_id` blank.
4. Run the notebook. Expect: 2 control rows (`ohb_comparison`, `ohb_nonserial_comparison`), 2 curated parquets,
   error rows only if a PK is duplicate/null/non-numeric.
5. To go live: set `decrypt_flag:"true"` — it then finds the latest dated folder, GPG-decrypts `*.xlsx.gpg`,
   and runs the identical per-tab logic on every workbook found.

## To onboard a new file/tab
Insert metadata rows keyed by `(file_name_prefix, sheet_tab_name)`: one row per source column, set
`is_primary_key=true` on the business key(s), `data_type` on each, and the same `array_column_name` (e.g. `DATA`)
on the tab. No code change needed.
