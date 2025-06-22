# Data Integration and Automation Pipeline

This project automates the following tasks:

1. **Data Export**: Exports data from all tables in a MySQL database into:
   - CSV
   - Parquet
   - Avro

2. **Database Migration**:
   - Copies all tables from a source database to a target database.
   - Selectively transfers data (e.g., only `active` customers) to a processed database.

3. **Triggers**:
   - **`event_watcher.py`**: Detects changes in any table in the source DB and runs the pipeline.
   - **`time_watcher.py`**: Runs the pipeline every fixed interval.

---

## Files

### `automation.py`
- Core logic for:
  - Exporting each table to `exports/` as `.csv`, `.parquet`, `.avro`
  - Copying all tables to a target DB
  - Selectively copying data to a processed DB (based on subscription status)
- Runs once each time it’s called.

### `event_watcher.py`
- Monitors the database for changes by checking:
  - Row counts
  - Maximum `join_date` (if present)
- If a change is detected, it runs `automation.py`

### `time_watcher.py`
- Simply runs `automation.py` every fixed interval (1 day).
- Does not inspect the database — purely time-based automation.

---

## Table Format (DDL)

To use this project, your tables should have the join_date and the subscription_status columns:
```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    join_date DATE,
    subscription_status VARCHAR(20)
);
