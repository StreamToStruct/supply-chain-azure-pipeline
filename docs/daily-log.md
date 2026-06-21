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



# Day 5 — PySpark Transformations: Dedup, Delay Flag, Silver + Gold Layer Writes

## What We Did
- Ran watermark filter — 180,518 rows pass (1 row filtered out)
- Investigated duplicates — found 65,751 unique Order IDs across 180,518 rows
- Discovered these are legitimate order line items (not duplicates) — max 5 line items per order
- Built dedup safety check using row_number() window function — 0 rows dropped
- Derived custom delay flag from raw columns (Days for shipping real > scheduled)
- Found 4,423 row discrepancy between derived flag and source Late_delivery_risk column
- Wrote cleaned data to ADLS silver layer (Parquet, partitioned by delay_flag)
- Built vendor scorecard using window functions (dense_rank partitioned by Market)
- Hit and fixed window shuffle warning by adding partitionBy("Market")
- Wrote vendor scorecard to ADLS gold layer (Parquet, partitioned by Market)

---

## Full Notebook Code

### Cell 5 — Watermark Filter
```python
from pyspark.sql.functions import to_timestamp, col

# Hardcoded for now — will come from Azure SQL via Key Vault on Day 6
watermark_date = "2015-01-01"

df_filtered = df.withColumn(
    "order_date_parsed",
    to_timestamp(col("order date (DateOrders)"), "M/d/yyyy H:mm")
).filter(
    col("order_date_parsed") > watermark_date
)

print(f"Rows after watermark filter: {df_filtered.count()}")
# Result: 180,518 rows (1 row filtered — order_date exactly 2015-01-01)
```

---

### Cell 6 — Duplicate Investigation
```python
from pyspark.sql.functions import count

# Check if Order Id is truly unique
print(f"Before dedup: {df_filtered.count()}")
print(f"Distinct Order IDs: {df_filtered.select('Order Id').distinct().count()}")
# Result: 180,518 rows, 65,751 unique Order IDs
# Finding: Same Order ID appears multiple times = legitimate order line items

# Find max line items per order
df_filtered.groupBy("Order Id") \
    .agg(count("Order Item Id").alias("item_count")) \
    .filter(col("item_count") > 1) \
    .orderBy(col("item_count").desc()) \
    .show(10)
# Result: Max 5 line items per order — NOT duplicates

# Confirm Order Item Id is the true primary key
print(f"Distinct Order Item IDs: {df_filtered.select('Order Item Id').distinct().count()}")
# Result: 180,518 — matches total rows exactly. Zero true duplicates.
```

**Key insight:**
> "Investigated 180,518 rows with only 65,751 unique Order IDs. 
> Initially appeared to be duplicates but analysis revealed these were 
> legitimate order line items — one order can have up to 5 line items, 
> each with a unique Order Item Id. Deduplication on Order Id would have 
> incorrectly dropped valid data. Identified Order Item Id as true primary key."

---

### Cell 7 — Dedup Safety Check (on Order Item Id)
```python
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Safety dedup — in case source ever sends same line item twice
window_spec = Window.partitionBy("Order Item Id").orderBy(col("order_date_parsed").desc())

df_deduped = df_filtered.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print(f"After dedup: {df_deduped.count()}")
# Result: 180,518 — zero rows dropped, confirms no real duplicates
```

---

### Cell 8 — Derive Delay Flag
```python
from pyspark.sql.functions import when

df_transformed = df_deduped.withColumn(
    "delay_flag",
    when(
        col("Days for shipping (real)") > col("Days for shipment (scheduled)"),
        1
    ).otherwise(0)
)

# Compare derived flag vs existing source flag
df_transformed.groupBy("delay_flag", "Late_delivery_risk") \
    .count() \
    .orderBy("delay_flag", "Late_delivery_risk") \
    .show()
```

**Result:**
```
+----------+------------------+-----+
|delay_flag|Late_delivery_risk|count|
+----------+------------------+-----+
|         0|                 0|77118|
|         1|                 0| 4423|   <-- DISCREPANCY
|         1|                 1|98977|
+----------+------------------+-----+
```

**Key insight:**
> "Found 4,423 records where derived delay_flag=1 but source Late_delivery_risk=0. 
> Did not blindly trust source flag — flagged discrepancy for business review. 
> Possible source data quality issue or different business definition of late delivery."

---

### Cell 9 — Write to Silver Layer
```python
silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/orders/"

df_transformed.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("delay_flag") \
    .save(silver_path)

print("Written to silver layer successfully")

# Verify
df_silver_check = spark.read.format("parquet").load(silver_path)
print(f"Rows in silver layer: {df_silver_check.count()}")
print(f"Partitions: {df_silver_check.select('delay_flag').distinct().collect()}")
# Result: 180,518 rows, partitions: delay_flag=0, delay_flag=1
```

**Why Parquet over CSV:**
- Columnar format — reads only needed columns, not full row
- Compressed — smaller storage cost
- Partition pruning — queries filter by delay_flag without scanning all data
- Industry standard for data lake silver/gold layers

---

### Cell 10 — Vendor Scorecard (Window Functions)
```python
from pyspark.sql.functions import count, sum, round, dense_rank
from pyspark.sql.window import Window

# Step 1: Aggregate by Market + Region
vendor_scorecard = df_transformed.groupBy("Market", "Order Region") \
    .agg(
        count("Order Item Id").alias("total_orders"),
        sum("delay_flag").alias("delayed_orders")
    ) \
    .withColumn(
        "delay_pct",
        round((col("delayed_orders") / col("total_orders")) * 100, 2)
    )

# Step 2: Rank regions within each market by delay %
window_spec = Window.partitionBy("Market").orderBy(col("delay_pct").asc())

vendor_scorecard_final = vendor_scorecard.withColumn(
    "rank",
    dense_rank().over(window_spec)
)

vendor_scorecard_final.orderBy("Market", "rank").show(50, truncate=False)
```

---

### Cell 11 — Write Vendor Scorecard to Gold Layer
```python
gold_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold/vendor_scorecard/"

vendor_scorecard_final.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("Market") \
    .save(gold_path)

print("Vendor scorecard written to gold layer successfully")
```

---

## Landmines Hit Today

### Landmine 1 — False Duplicate Alert
**Symptom:** 180,518 rows but only 65,751 unique Order IDs
**Initial assumption:** Duplicates exist
**Root cause:** One order has multiple line items — each is a separate row
**Fix:** Used Order Item Id (not Order Id) as dedup key
**Lesson:** Always investigate before deduplicating — blind dedup on wrong key drops valid data

### Landmine 2 — Source Flag Discrepancy
**Symptom:** 4,423 rows where derived delay_flag ≠ source Late_delivery_risk
**Root cause:** Unknown — possible different business logic in source system
**Fix:** Used derived flag (from raw columns) as more trustworthy, flagged for business review
**Lesson:** Never blindly trust pre-calculated flags in source data

### Landmine 3 — Window Shuffle Warning
**Symptom:** `No Partition Defined for Window operation! Moving all data to a single partition`
**Root cause:** `Window.orderBy()` without `partitionBy()` forces all data to one executor
**Fix:** Added `partitionBy("Market")` — ranks regions within each market independently
**Lesson:** Always define partitionBy in window specs. Without it, performance degrades linearly with data size.

---

## Metrics
- Rows after watermark filter: 180,518
- True duplicates found: 0
- Delay flag discrepancy: 4,423 rows
- Silver layer: 180,518 rows, Parquet, partitioned by delay_flag (0/1)
- Gold layer: 23 regions ranked within 5 markets, Parquet, partitioned by Market

## ADLS Structure After Day 5
```
supplychain-project/
├── raw/orders/full_load/DataCoSupplyChainDataset.csv
├── silver/orders/
│   ├── delay_flag=0/  (77,118 rows)
│   └── delay_flag=1/  (103,400 rows)
└── gold/vendor_scorecard/
    ├── Market=Africa/
    ├── Market=Europe/
    ├── Market=LATAM/
    ├── Market=Pacific Asia/
    └── Market=USCA/
```

## Files
- `notebooks/01_mount_adls_read_csv.ipynb`
