# Setup Guide: Broker Summary Stock Endpoint

This guide walks through setting up the `broker_summary_stock` endpoint with date iteration and custom partition keys.

## Prerequisites

1. **Load Syariah Stock List**

First, load the reference data from CSV to BigQuery:

```bash
python scripts/load_syariah_stocks.py
```

This creates the `syariah_stock_list` table with JII70 stocks.

Verify the table:
```bash
bq query --use_legacy_sql=false "SELECT indeks, COUNT(*) as count FROM \`emerald-skill-352503.invezgo_data.syariah_stock_list\` GROUP BY indeks"
```

## Configuration

The endpoint configuration in `config/endpoints.yaml`:

```yaml
- name: broker_summary_stock
  path: /analysis/summary/stock/{stock_code}
  description: "Daily broker summary for JII70 stocks"
  bq_table: broker_summary_stock
  execution_mode: batch
  
  batch_config:
    # Iterate through JII70 stocks
    iterate_by: stock_code
    source_table: syariah_stock_list
    source_column: kode
    source_filter: "indeks = 'JII70'"
    
    # Iterate through date range
    date_iteration:
      enabled: true
      start_date: "2024-12-01"
      end_date: "2024-12-30"
      date_column: date
  
  # Partition by date only (not stock_code) to batch BigQuery operations
  # This avoids "too many table update operations" rate limit errors
  partition_key:
    - date
  
  # Inject context into each record
  inject_columns:
    stock_code: "{stock_code}"
    date: "{date}"
  
  params:
    from: "{date}"      # Single day
    to: "{date}"
    investor: "all"
    market: "RG"
  
  path_variables:
    stock_code: null
  
  enabled: true
```

## How It Works

### 1. Date Range Splitting (Important!)

When you provide a date range, the pipeline **splits it into individual days**:

```bash
# Input:
python main.py --mode batch --start-date 2025-10-20 --end-date 2025-10-24

# Pipeline splits into 5 separate days:
# - 2025-10-20
# - 2025-10-21
# - 2025-10-22
# - 2025-10-23
# - 2025-10-24
```

**Each day gets its own API call** with `from=date` and `to=date` (same date), ensuring you get **daily values, not averages**.

### 2. Batch Iteration

The pipeline will:
1. Query JII70 stocks: `SELECT DISTINCT kode FROM syariah_stock_list WHERE indeks = 'JII70'` (70 stocks)
2. Split date range into individual days: `2024-12-01` to `2024-12-30` = 30 days
3. Create combinations: **70 stocks × 30 dates = 2,100 API calls**

### 3. API Call Example

For each stock-date combination, makes a **separate API call with single day**:

```
# For ACES on 2024-12-01
GET https://api.invezgo.com/analysis/summary/stock/ACES?from=2024-12-01&to=2024-12-01&investor=all&market=RG

# For ACES on 2024-12-02
GET https://api.invezgo.com/analysis/summary/stock/ACES?from=2024-12-02&to=2024-12-02&investor=all&market=RG

# For BBCA on 2024-12-01
GET https://api.invezgo.com/analysis/summary/stock/BBCA?from=2024-12-01&to=2024-12-01&investor=all&market=RG
```

**Notice:** `from` and `to` parameters are **always the same date** - this gets daily data, not averaged data.

### 3. Response Transformation

API returns broker data without stock/date context:
```json
{
  "code": "AF",
  "buy_freq": "0",
  "buy_volume": "0",
  "name": "HARITA KENCANA SEKURITAS"
}
```

Pipeline injects context:
```json
{
  "code": "AF",
  "buy_freq": "0",
  "buy_volume": "0",
  "name": "HARITA KENCANA SEKURITAS",
  "stock_code": "ACES",
  "date": "2024-12-01",
  "_sys_ingested_at": "2024-12-30T10:15:30+07:00"
}
```

### 4. Idempotent Loading (Batched by Date)

To avoid BigQuery rate limits, the pipeline batches operations by date:

**For 70 stocks on 2024-12-01:**
1. Fetches all 70 stocks (70 API calls)
2. Collects all records in memory
3. **Single delete operation:**
   ```sql
   DELETE FROM broker_summary_stock
   WHERE date = '2024-12-01'
   ```
4. **Single insert operation:** Loads all 70 stocks' data at once

**Result:** Only 2 BigQuery operations per date (not 140), avoiding rate limits.

**Re-running same date:** Safe and idempotent - replaces all data for that date.

## Running the Pipeline

### Daily Run (Today's Date - Auto-detected)

```bash
# Process today's data (Jakarta timezone auto-detected)
python main.py --mode batch
```

This automatically uses today's date in Jakarta timezone. Perfect for daily scheduled runs.

### Backdate Single Day

```bash
# Process specific date (e.g., yesterday)
python main.py --mode batch --date 2024-12-01

# Process last Friday's data
python main.py --mode batch --date 2024-12-27
```

### Backfill Date Range

```bash
# Backfill last week (processes each day separately)
python main.py --mode batch --start-date 2024-12-23 --end-date 2024-12-29
# Result: 70 stocks × 7 days = 490 API calls (each day separate)

# Backfill entire month (processes each day separately)
python main.py --mode batch --start-date 2024-12-01 --end-date 2024-12-31
# Result: 70 stocks × 31 days = 2,170 API calls (each day separate)

# Backfill 5 days (example from requirements)
python main.py --mode batch --start-date 2025-10-20 --end-date 2025-10-24
# Result: 70 stocks × 5 days = 350 API calls
# Each API call: from=2025-10-20&to=2025-10-20 (single day, not range)
#                from=2025-10-21&to=2025-10-21 (next day)
# ... and so on for each day
```

**Important:** The date range is **automatically split** into individual days. Each day gets its own API call to ensure you receive **daily values, not averaged values**.

### Test with Single Stock

Edit `endpoints.yaml`:
```yaml
batch_config:
  # Comment out source_table, use static list
  # source_table: syariah_stock_list
  # source_column: kode
  # source_filter: "indeks = 'JII70'"
  values: ["BBCA"]  # Test with one stock
```

Then run:
```bash
# Test today's data for BBCA only
python main.py --mode batch

# Test specific date for BBCA only
python main.py --mode batch --date 2024-12-01
```

## Performance Considerations

### Execution Time

- API calls: ~2,100 calls
- Estimated time: ~35 minutes (1 call/second)
- BigQuery loading: Fast (batch inserts)

### Rate Limiting

If API has rate limits, add delay in `src/invezgo_client.py`:
```python
import time

def fetch_data(self, ...):
    response = self.session.get(url, ...)
    time.sleep(1)  # 1 second delay between calls
    return response.json()
```

### Parallel Processing

For faster processing, modify to use threading (future enhancement).

## Querying Results

### Check data loaded
```sql
SELECT 
  COUNT(*) as total_records,
  COUNT(DISTINCT stock_code) as stocks,
  COUNT(DISTINCT date) as dates,
  MIN(date) as earliest_date,
  MAX(date) as latest_date
FROM `emerald-skill-352503.invezgo_data.broker_summary_stock`
```

### Broker summary for specific stock and date
```sql
SELECT *
FROM `emerald-skill-352503.invezgo_data.broker_summary_stock`
WHERE stock_code = 'BBCA' AND date = '2024-12-01'
ORDER BY sell_value DESC
```

### Top brokers by trading value
```sql
SELECT 
  stock_code,
  code as broker_code,
  name as broker_name,
  date,
  CAST(sell_value AS INT64) as sell_value,
  CAST(buy_value AS INT64) as buy_value
FROM `emerald-skill-352503.invezgo_data.broker_summary_stock`
WHERE date = '2024-12-01'
ORDER BY sell_value DESC
LIMIT 10
```

## Troubleshooting

### Table schema issues
If schema auto-detection fails, manually create table:
```bash
bq mk --table \
  emerald-skill-352503:invezgo_data.broker_summary_stock \
  stock_code:STRING,code:STRING,buy_freq:STRING,buy_volume:STRING,buy_value:STRING,sell_freq:STRING,sell_volume:STRING,sell_value:STRING,buy_avg:FLOAT,sell_avg:FLOAT,name:STRING,date:STRING,_sys_ingested_at:STRING
```

### Re-run failed dates
Check which dates failed:
```sql
SELECT date, COUNT(DISTINCT stock_code) as stocks
FROM `emerald-skill-352503.invezgo_data.broker_summary_stock`
GROUP BY date
HAVING stocks < 70  -- Expected 70 JII70 stocks
ORDER BY date
```

Update `date_iteration` in endpoints.yaml to re-run those dates.

## Scheduling

### Daily Automatic Updates

The pipeline automatically detects today's date in Jakarta timezone, so scheduling is simple:

**Windows Task Scheduler:**
```powershell
# Create daily task running at 6 PM (after market close)
$action = New-ScheduledTaskAction -Execute "python" -Argument "main.py --mode batch" -WorkingDirectory "D:\path\to\playground"
$trigger = New-ScheduledTaskTrigger -Daily -At 6PM
Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "InvezgoDataPipeline" -Description "Daily broker summary data ingestion"
```

**Linux Cron:**
```bash
# Run daily at 6 PM (after market close), Monday-Friday
0 18 * * 1-5 cd /path/to/playground && /usr/bin/python3 main.py --mode batch
```

**Docker/Kubernetes CronJob:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: invezgo-pipeline
spec:
  schedule: "0 18 * * 1-5"  # 6 PM, Mon-Fri
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: pipeline
            image: invezgo-pipeline:latest
            command: ["python", "main.py", "--mode", "batch"]
```

### Catch-up / Missing Days

If the pipeline fails or misses days, backfill easily:

```bash
# Backfill last 3 days
python main.py --mode batch --start-date 2024-12-27 --end-date 2024-12-29

# Backfill specific missing day
python main.py --mode batch --date 2024-12-25
```

### Historical Backfill

For initial setup, backfill historical data once:

```bash
# Backfill 3 months of history
python main.py --mode batch --start-date 2024-10-01 --end-date 2024-12-30
```

Then switch to daily scheduled runs (no date arguments needed).
