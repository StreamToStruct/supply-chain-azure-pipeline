# Daily Log — Problems & Fixes



Summary — Days 1-3
Day 1:

Downloaded DataCo Supply Chain dataset from Kaggle, reviewed schema (dates in MM/DD/YYYY HH:MM, will derive own delay flag from Days for shipping (real) > Days for shipment (scheduled))
Created ADLS Gen2 container supplychain-project with raw/silver/gold folder structure
Uploaded source CSV to raw/orders/full_load/
Built pl_fullload_orders — Copy Data activity, ran successfully in 18s

Day 2:

Created Azure SQL database supplychain_meta on Serverless tier (0.5–1 vCore, auto-pause) — cut estimated cost from $293/month to under $5/month
Used LRS backup, no zone redundancy — further cost savings
Created pipeline_watermark table (table_name, last_load_date), seeded orders | 2015-01-01
Enabled "Allow Azure services" firewall rule + Managed Identity auth for ADF→SQL
Fixed login error by creating ADF's managed identity as a DB user (CREATE USER FROM EXTERNAL PROVIDER + roles)
Built lkp_get_watermark — successfully read 2015-01-01
Design decision: ADF Copy can't filter CSV by date → filtering logic moved to Databricks notebook (Day 5-6); ADF will orchestrate

Day 3:

Created pipeline_config table (table_name, source_path, active_flag) — makes pipeline metadata-driven
Built lkp_get_config (outside ForEach) → returns active sources
Wrapped pipeline in ForEach loop using @activity('lkp_get_config').output.value
Inside ForEach: lkp_get_watermark now dynamic via @item().table_name — verified returns 2015-01-01 for orders
Landmine fixed: nesting lkp_get_config inside ForEach broke output reference — moved outside, connected via arrow
Result: adding new source tables = just insert a row in config, zero pipeline changes needed


Current pipeline state: pl_incremental_orders reads config → loops sources → fetches watermark per source. Ready for Day 5-6 where Databricks notebook does actual filtering + writes to silver layer.
