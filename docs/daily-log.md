# Daily Log — Problems & Fixes

## Day 1
- Dataset downloaded, columns reviewed (dates in MM/DD/YYYY HH:MM format, will need explicit parsing)
- ## Day 1
**Observation:** Date columns (order date, shipping date) are in format MM/DD/YYYY HH:MM, not ISO. Will need explicit format string when parsing in PySpark (to_timestamp with format param).
- Derived delay flag plan: (Days for shipping (real) > Days for shipment (scheduled))
- ADLS structure created: raw/silver/gold
- Full load pipeline (pl_fullload_orders) built and run successfully — 18s
- Sink output: 1 file, no splitting
