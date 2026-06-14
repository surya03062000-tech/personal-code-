# Sample curated output — tab `OHB Nonserial Comparison` → table `ohb_nonserial_comparison`

This is what **one curated `.parquet`** looks like for a single tab. PK columns stay as real columns,
**every non-PK column is folded into the single JSON `DATA` column**, then the standard metadata columns
are appended.

## Schema of the curated parquet
| column | type | notes |
|---|---|---|
| `PART_NUMBER` | string | PK — kept as-is |
| `STOCK_LOCATION_ID` | string | PK — kept as-is |
| `DATA` | string (JSON) | all non-PK columns folded here (`array_column_name` from metadata) |
| `PK_DERIVED` | string | md5(PART_NUMBER + STOCK_LOCATION_ID) |
| `BATCH_ID` | string | parsed from file name `20260610000000` |
| `FILE_DTTM` | timestamp | parsed from file name |
| `SHEET_TAB_NAME` | string | `OHB Nonserial Comparison` |
| `SOURCE_FILE_NAME` | string | `Rogers_Shaw_STB_OHB_Comparison_2026.06.10.xlsx` |
| `PROCESS_ID` | string | unique per tab (matches process_control / error table) |
| `EXECUTION_ID` | string | run id |
| `_AZ_INSERT_TS` | timestamp | load time |

## Sample rows (rendered)
| PART_NUMBER | STOCK_LOCATION_ID | DATA | PK_DERIVED | BATCH_ID | SHEET_TAB_NAME |
|---|---|---|---|---|---|
| 0151000162 | OAK-A12 | `{"CTDI_COMP_DATE":"2026-06-10","SHAW_COMP_DATE":"2026-06-10","PARTTYPE":"STB","CTDI_QUANTITY":"5","SHAW_QUANTITY":"5","DELTA_QUANTITY":"0"}` | 3f8a…e1 | 20260610000000 | OHB Nonserial Comparison |
| 0151000177 | OAK-B03 | `{"CTDI_COMP_DATE":"2026-06-10","SHAW_COMP_DATE":"2026-06-10","PARTTYPE":"MODEM","CTDI_QUANTITY":"12","SHAW_QUANTITY":"11","DELTA_QUANTITY":"1"}` | 9c12…ab | 20260610000000 | OHB Nonserial Comparison |

## Matching process_control row (one per tab)
| column | value |
|---|---|
| execution_id | LOCAL_20260614… |
| process_id | 6f1c2d9a-… (unique) |
| batch_id | 20260610000000 |
| file_name | Rogers_Shaw_STB_OHB_Comparison_2026.06.10.xlsx |
| sheet_tab_name | OHB Nonserial Comparison |
| table_name | ohb_nonserial_comparison |
| source_file_path | abfss://raw@…/erp/bicc_INT_680/2026/06/10/10/Rogers_Shaw_…xlsx |
| raw_parquet_path | abfss://raw@…/erp/ohb_nonserial_comparison/_raw_asis/2026/06/10/10/ohb_nonserial_comparison.parquet |
| final_parquet_source_raw | abfss://raw@…/erp/ohb_nonserial_comparison/2026/06/10/10/ohb_nonserial_comparison.parquet |
| dq_check_validation | `["Duplicate PK Check : PASS \| duplicate_records=0", "Not Null PK Check : PASS \| null_records=0", "Data Type Check : PASS \| mismatch_records=0"]` |
| source_row_count | 38 |
| valid_record_count | 38 |
| error_record_count | 0 |
| final_ingestion_status | Succeeded |
| comments | All primary-key checks passed. |

## Matching error-table row (only when a record fails a PK check)
e.g. a duplicate `(PART_NUMBER, STOCK_LOCATION_ID)`:
| process_id | execution_id | file_name | table_name | sheet_tab_name | file_path | comments | err_record |
|---|---|---|---|---|---|---|---|
| 6f1c2d9a-… | LOCAL_… | Rogers_Shaw_…xlsx | ohb_nonserial_comparison | OHB Nonserial Comparison | abfss://… | `Duplicate Primary Key \|\|` | `{"CTDI_COMP_DATE":"…","PART_NUMBER":"0151000177","STOCK_LOCATION_ID":"OAK-B03",…,"PK_DERIVED":"…","COMMENTS":"Duplicate Primary Key || "}` |

> Values shown are illustrative; PK choices in the metadata are examples — set them to the real business keys per table.
