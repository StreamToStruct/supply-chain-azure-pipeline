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



# Day 4 — Databricks Setup + ADLS Read + Date Fix + Watermark Filter

## What We Did
- Created Azure Databricks cluster (SupplyChain_Cluster, single node, 30 min auto-terminate)
- Connected Databricks to ADLS Gen2 using storage account key
- Read 180,519 rows from source CSV into a Spark DataFrame
- Identified and fixed date format issue (M/d/yyyy H:mm)
- Hit SQL auth landmine (Entra Interactive needs browser — cluster has none)
- Hardcoded watermark as temporary fix, proper fix (Key Vault) coming Day 6

---

## Full Notebook Code

### Cell 1 — Connect Databricks to ADLS Gen2
```python
storage_account_name = "saoptimization"
storage_account_key = "your-key-here"  # Will move to Key Vault on Day 6
container_name = "supplychain-project"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("ADLS connection configured")
```
**What this does:**
- `spark.conf.set` registers the storage key as a Spark runtime config
- No connection made yet — just credentials registered
- `abfss://` protocol (Azure Blob File System Secure) is what Spark uses to talk to ADLS Gen2
- Like giving Spark the key to the house before it walks in

---

### Cell 2 — Read CSV from ADLS
```python
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw/orders/full_load/DataCoSupplyChainDataset.csv")

print(f"Total rows: {df.count()}")
df.printSchema()
```
**What this does:**
- `abfss://container@storageaccount.dfs.core.windows.net/path` = full ADLS Gen2 path format
- `header=true` = first row treated as column names, not data
- `inferSchema=true` = Spark reads a sample and guesses data types automatically
- `df.count()` = triggers actual read (Spark is lazy — nothing runs until an action is called)
- **Result: 180,519 rows loaded**

---

### Cell 3 — Preview Key Columns
```python
from pyspark.sql.functions import col

df.select(
    "Order Id",
    "order date (DateOrders)",
    "Shipping Date (DateOrders)",
    "Days for shipping (real)",
    "Days for shipment (scheduled)"
).show(5, truncate=False)
```
**What this does:**
- `select` = pick specific columns (like SQL SELECT)
- `show(5, truncate=False)` = show 5 rows, don't cut off long values
- **Discovery: dates in format `1/31/2018 22:56` — single digit month/hour possible**

---

### Cell 4 — Fix Date Parsing (after landmine)
```python
from pyspark.sql.functions import to_timestamp, col

df_with_date = df.withColumn(
    "order_date_parsed",
    to_timestamp(col("order date (DateOrders)"), "M/d/yyyy H:mm")
)

df_with_date.select("order date (DateOrders)", "order_date_parsed").show(5, truncate=False)
```
**What this does:**
- `withColumn` = adds a new column without modifying original DataFrame
- `to_timestamp` = converts string to proper timestamp type
- Format `M/d/yyyy H:mm` = handles BOTH single and double digit month/day/hour
- First tried `MM/dd/yyyy HH:mm` — failed on `9:15` (single digit hour)
- `M` and `H` in Spark format = flexible, accepts 1 or 2 digits

---

### Cell 5 — Watermark Filter (Incremental Load Logic)
```python
# Hardcoded for now — will come from Azure SQL via Key Vault on Day 6
watermark_date = "2015-01-01"

df_filtered = df.withColumn(
    "order_date_parsed",
    to_timestamp(col("order date (DateOrders)"), "M/d/yyyy H:mm")
).filter(
    col("order_date_parsed") > watermark_date
)

print(f"Rows after watermark filter: {df_filtered.count()}")
```
**What this does:**
- `filter` = keeps only rows where condition is true (like SQL WHERE)
- `order_date_parsed > watermark_date` = only new records since last load
- This IS the incremental load logic — same pattern as ADF watermark, just in PySpark
- In production: `watermark_date` comes from Azure SQL dynamically, not hardcoded

---

## Landmine Hit Today

**Problem:** Entra Interactive auth (`ActiveDirectoryInteractive`) from Databricks to Azure SQL fails with:
```
Unable to open default system browser
```

**Root cause:** Entra Interactive auth needs a browser popup for MFA. 
Databricks cluster = remote Linux machine = no browser. Fails every time.

**Temporary fix:** Hardcoded watermark value in notebook.

**Production fix (Day 6):**
```
Azure Key Vault
    → stores SQL connection string as a secret
    → Databricks fetches it at runtime using Managed Identity
    → No credentials in notebook code, no browser needed
```

**Interview answer:**
> "Initially tried Entra Interactive auth from Databricks to Azure SQL — failed because 
> clusters have no browser for MFA. Fixed by storing credentials in Azure Key Vault and 
> fetching via Databricks secret scope at runtime. Zero credentials stored in notebook code."

---

## Connection Methods — Current vs Production Grade

| Connection | Method Used Today | Production Grade Method | When We Fix It |
|---|---|---|---|
| Databricks → ADLS | Storage Account Key | Managed Identity / Service Principal | Day 6 |
| Databricks → Azure SQL | Hardcoded skip | Key Vault secret + token auth | Day 6 |
| ADF → Azure SQL | Managed Identity ✅ | Managed Identity ✅ | Already done |
| ADF → ADLS | Managed Identity ✅ | Managed Identity ✅ | Already done |

---

## Metrics Logged
- Total rows in dataset: **180,519**
- Date format identified: `M/d/yyyy H:mm`
- Null dates after fix: **0**
- Cluster: Single node, Standard_D4ds_v5, 2 DBU/h, auto-terminate 30 min

---

## Files
- `notebooks/01_mount_adls_read_csv.ipynb`
