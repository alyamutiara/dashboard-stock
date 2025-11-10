# Invezgo to BigQuery Data Pipeline

A Python-based data pipeline that fetches data from multiple Invezgo API endpoints and loads them into Google BigQuery. The API authentication token is securely stored in GCP Secret Manager.

## Features

- 🔐 Secure token management using GCP Secret Manager
- 🌐 Invezgo API integration with bearer token authentication
- 🔄 **Support for multiple API endpoints** with flexible configuration
- 📊 **Three execution modes**: onetime, batch, and streaming
- 🔁 **Batch processing** with automatic iteration over parameters
- 📈 Automatic data loading to BigQuery with schema auto-detection
- ⏰ **Automatic timestamp tracking** with `_sys_ingested_at` column (Jakarta timezone)
- 📝 Comprehensive logging
- ⚙️ Configurable via YAML and environment variables

## Execution Modes

### 1. **Onetime Mode** 
For reference/lookup data that changes infrequently:
- Fetches data once
- Replaces existing data in BigQuery (`WRITE_TRUNCATE`)
- Examples: stock lists, broker lists, market holidays

### 2. **Batch Mode**
For periodic data fetching with multiple parameters:
- Iterates through a list of values (stocks, dates, etc.)
- Appends data to BigQuery (`WRITE_APPEND`)
- Can use static list or query BigQuery for batch values
- Examples: daily broker summaries per stock, historical data

### 3. **Streaming Mode**
For real-time or near-real-time data ingestion:
- Continuously polls API endpoints
- Processes data as it arrives
- (Implementation coming soon)

## Prerequisites

- Python 3.8+
- Google Cloud Platform account with:
  - Secret Manager API enabled
  - BigQuery API enabled
  - Service account with appropriate permissions
- Invezgo API access token

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. GCP Authentication

Set up authentication using your service account key file:

**Windows (PowerShell):**
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="$PWD\secret\secret.json"
```

**Windows (Command Prompt):**
```cmd
set GOOGLE_APPLICATION_CREDENTIALS=%CD%\secret\secret.json
```

**Linux/Mac:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/secret/secret.json"
```

**Or add to your `.env` file (recommended):**
```env
GOOGLE_APPLICATION_CREDENTIALS=./secret/secret.json
```

Your service account needs these permissions:
- `roles/secretmanager.secretAccessor` - To access secrets
- `roles/bigquery.dataEditor` - To write to BigQuery
- `roles/bigquery.jobUser` - To run BigQuery jobs

### 3. Store Invezgo Token in Secret Manager

```bash
# Create the secret (one time)
gcloud secrets create $SECRET_NAME --project=$PROJECT_ID

# Add the token value
echo -n "YOUR_INVEZGO_TOKEN" | gcloud secrets versions add $SECRET_NAME --data-file=-
```

### 4. Create BigQuery Dataset and Table

```bash
# Create dataset
bq mk --dataset $PROJECT_ID:invezgo_data

# The table will be created automatically on first run with auto-detected schema
# Or you can create it manually with a specific schema
```

### 5. Configure API Endpoints

Edit `config/endpoints.yaml` to define which API endpoints to fetch:

```yaml
endpoints:
  # Broker summary per stock
  - name: broker_summary_stock
    path: /analysis/summary/stock/{stock_code}
    description: "Broker summary for a specific stock"
    bq_table: broker_summary_stock
    params:
      from: "2024-12-01"
      to: "2024-12-30"
      investor: "all"
      market: "RG"
    path_variables:
      stock_code: "BBCA"
    enabled: true

  # Stock list
  - name: stock_list
    path: /analysis/list/stock
    description: "List of all available stocks"
    bq_table: stock_list
    params: {}
    enabled: true

  # Broker list
  - name: broker_list
    path: /analysis/list/broker
    description: "List of all brokers"
    bq_table: broker_list
    params: {}
    enabled: true
```

**Endpoint Configuration Fields:**
- `name`: Unique identifier for the endpoint
- `path`: API endpoint path (supports `{variable}` placeholders)
- `description`: Human-readable description
- `bq_table`: BigQuery table name to load data into
- `params`: Query parameters (e.g., `?from=2024-12-01`)
- `path_variables`: Values to replace in path placeholders
- `enabled`: Set to `false` to skip this endpoint

### 6. Configure Environment Variables

Copy the example environment file and customize it:

```bash
copy config\.env.example .env
```

Edit `.env` in the root directory with your configuration:

```env
GCP_PROJECT_ID=$PROJECT_ID
SECRET_NAME=$SECRET_NAME
SECRET_VERSION=latest

INVEZGO_API_BASE_URL=https://api.invezgo.com/

BQ_DATASET_ID=invezgo_data
BQ_LOCATION=US

LOG_LEVEL=INFO
```

**Note:** API endpoints and their specific configurations are now managed in `config/endpoints.yaml` instead of environment variables.

## Quick Start

### Using Helper Scripts

The project includes utility scripts for easy setup:

```bash
# 1. Verify your setup
python scripts\verify_setup.py

# 2. Setup GCP Secret (if needed)
python scripts\setup_secret.py --token INVEZGO_TOKEN

# 3. Setup BigQuery (if needed)
python scripts\setup_bigquery.py

# 4. Run the pipeline (all modes)
python main.py

# 5. Run specific execution mode
python main.py --mode onetime    # Run only onetime endpoints
python main.py --mode batch      # Run only batch endpoints
python main.py --mode streaming  # Run only streaming endpoints
```

### Manual Usage

**Basic Usage:**
```bash
# Run all enabled endpoints for today (auto-detected Jakarta timezone)
python main.py

# Run only onetime endpoints (reference data)
python main.py --mode onetime

# Run only batch endpoints for today
python main.py --mode batch

# Show help
python main.py --help
```

**Date Options (for Batch Mode):**
```bash
# Process specific date (backdate single day)
python main.py --mode batch --date 2024-12-01

# Process date range (backfill historical data)
python main.py --mode batch --start-date 2024-12-01 --end-date 2024-12-10

# Common scenarios:
# Yesterday's data
python main.py --mode batch --date 2024-12-29

# Last week's data (backfill)
python main.py --mode batch --start-date 2024-12-23 --end-date 2024-12-29

# Today (explicit, same as no date argument)
python main.py --mode batch --date 2024-12-30
```

**How Date Detection Works:**
- **No date arguments**: Uses today's date in Jakarta timezone (auto-detected)
- **--date**: Process single specific date (for backdating)
- **--start-date + --end-date**: Process date range (for backfilling)
- Command-line dates **override** `date_iteration` settings in `endpoints.yaml`

The pipeline will:
1. Detect or parse date arguments (defaults to today in Jakarta timezone)
2. Load enabled endpoints from `config/endpoints.yaml`
3. Override endpoint date ranges with command-line arguments
4. Filter by execution mode (if specified)
5. Retrieve the Invezgo API token from GCP Secret Manager
6. For each enabled endpoint:
   - **Onetime**: Fetch once and replace data
   - **Batch**: Iterate through parameters and dates, replace partitions
   - **Streaming**: Continuously poll and process data
7. Provide a detailed summary of all operations

### Running Tests

```bash
python -m unittest discover tests
```

## Project Structure

```
playground/
├── main.py                      # Main pipeline entry point
├── requirements.txt             # Python dependencies
├── README.md                    # This file
├── .gitignore                   # Git ignore rules
│
├── config/                      # Configuration files
│   ├── __init__.py
│   ├── settings.py              # Application settings
│   ├── endpoints.yaml           # API endpoints configuration
│   └── .env.example             # Example environment variables
│
├── src/                         # Source code modules
│   ├── __init__.py
│   ├── pipeline.py              # Pipeline orchestrator
│   ├── gcp_secret_manager.py   # GCP Secret Manager integration
│   ├── invezgo_client.py        # Invezgo API client
│   └── bigquery_loader.py       # BigQuery data loader
│
├── scripts/                     # Utility scripts
│   ├── setup_secret.py          # Setup GCP secrets
│   ├── setup_bigquery.py        # Setup BigQuery dataset/table
│   └── verify_setup.py          # Verify configuration
│
├── tests/                       # Unit tests
│   ├── __init__.py
│   └── test_pipeline.py         # Pipeline tests
│
├── logs/                        # Application logs
│   └── pipeline.log
│
└── secret/                      # GCP service account keys
    └── secret.json
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GCP_PROJECT_ID` | GCP project ID | `$PROJECT$` |
| `SECRET_NAME` | Secret name in Secret Manager | `$SECRET_NAME` |
| `SECRET_VERSION` | Secret version | `latest` |
| `INVEZGO_API_BASE_URL` | Invezgo API base URL | `https://api.invezgo.com/` |
| `BQ_DATASET_ID` | BigQuery dataset ID | `invezgo_data` |
| `BQ_LOCATION` | BigQuery dataset location | `$REGION` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Endpoints Configuration (config/endpoints.yaml)

The pipeline supports multiple API endpoints with different execution modes:

#### Field Reference

| Field | Description | Required |
|-------|-------------|----------|
| `name` | Unique identifier for the endpoint | Yes |
| `path` | API endpoint path (supports `{variable}` placeholders) | Yes |
| `description` | Human-readable description | No |
| `bq_table` | BigQuery table name | Yes |
| `execution_mode` | Execution mode: `onetime`, `batch`, or `streaming` | Yes |
| `params` | Query parameters (e.g., `?from=2024-12-01`) | No |
| `path_variables` | Values for path placeholders | No |
| `batch_config` | Batch-specific configuration (for batch mode only) | No |
| `enabled` | Enable/disable endpoint | Yes |

#### Execution Mode Examples

**1. Onetime Mode (Reference Data)**

Simple endpoint that runs once and replaces data:

```yaml
- name: stock_list
  path: /analysis/list/stock
  description: "List of all available stocks"
  bq_table: stock_list
  execution_mode: onetime
  params: {}
  enabled: true
```

**2. Batch Mode (Periodic Data)**

Iterates through a list of values:

```yaml
# Option A: Using static list
- name: broker_summary_stock
  path: /analysis/summary/stock/{stock_code}
  description: "Broker summary for multiple stocks"
  bq_table: broker_summary_stock
  execution_mode: batch
  batch_config:
    iterate_by: stock_code          # Parameter to iterate
    values: ["BBCA", "BBRI", "BMRI"]  # List of values
  params:
    from: "2024-12-01"
    to: "2024-12-30"
  path_variables:
    stock_code: null                # Will be replaced during iteration
  enabled: true

# Option B: Query values from BigQuery
- name: broker_summary_all_stocks
  path: /analysis/summary/stock/{stock_code}
  bq_table: broker_summary_stock
  execution_mode: batch
  batch_config:
    iterate_by: stock_code
    source_table: stock_list        # Get values from this table
    source_column: stock_code       # Column containing values
  params:
    from: "2024-12-01"
    to: "2024-12-30"
  enabled: true

# Option C: Date iteration with filtered source + custom partition keys
- name: broker_summary_stock
  path: /analysis/summary/stock/{stock_code}
  description: "Daily broker summary for JII70 stocks"
  bq_table: broker_summary_stock
  execution_mode: batch
  batch_config:
    iterate_by: stock_code
    source_table: syariah_stock_list          # Source table
    source_column: kode                       # Column with stock codes
    source_filter: "indeks = 'JII70'"         # SQL WHERE clause
    
    # Date iteration for daily backfill
    date_iteration:
      enabled: true
      start_date: "2024-12-01"
      end_date: "2024-12-30"
      date_column: date
  
  # Multi-column partition keys for idempotency
  partition_key:
    - stock_code  # First partition dimension
    - date        # Second partition dimension
  
  # Inject columns into response (API doesn't return these)
  inject_columns:
    stock_code: "{stock_code}"  # Current iteration value
    date: "{date}"              # Current date value
  
  params:
    from: "{date}"    # Replaced with actual date
    to: "{date}"
    investor: "all"
    market: "RG"
  path_variables:
    stock_code: null
  enabled: true
```

**3. Streaming Mode (Real-time Data)**

```yaml
- name: realtime_trades
  path: /streaming/trades
  description: "Real-time trade data"
  bq_table: realtime_trades
  execution_mode: streaming
  streaming_config:
    poll_interval: 5              # Poll every 5 seconds
    max_records_per_poll: 100
  enabled: false  # Coming soon
```

### Adding New Endpoints

To add a new endpoint:

1. Edit `config/endpoints.yaml`
2. Choose appropriate execution mode:
   - **onetime**: For reference data that rarely changes
   - **batch**: For data that needs multiple API calls with different parameters
   - **streaming**: For real-time data (coming soon)
3. Add configuration following the examples above
4. Set `enabled: true`
5. Run the pipeline - BigQuery table will be created automatically

**Example workflow for batch processing:**

```yaml
# Step 1: First run onetime mode to get stock list
- name: stock_list
  path: /analysis/list/stock
  bq_table: stock_list
  execution_mode: onetime
  enabled: true

# Step 2: Then run batch mode using the stock list
- name: stock_details_batch
  path: /analysis/detail/{stock_code}
  bq_table: stock_details
  execution_mode: batch
  batch_config:
    iterate_by: stock_code
    source_table: stock_list     # Use data from step 1
    source_column: code
  enabled: true
```

Run separately:
```bash
python main.py --mode onetime   # First, get reference data
python main.py --mode batch     # Then, process batch data
```

## Logging

The pipeline uses Python's logging module with **separate log files for each execution mode** for easier troubleshooting.

### Log Files

All logs are written to the `logs/` directory:

| File | Description | Content |
|------|-------------|---------|
| `pipeline.log` | Main/combined log | All logs from all modes |
| `onetime.log` | Onetime mode only | Logs with `[ONETIME]` marker + general logs |
| `batch.log` | Batch mode only | Logs with `[BATCH]` marker + general logs |
| `streaming.log` | Streaming mode only | Logs with `[STREAMING]` marker + general logs |

### Log Format

Each log entry includes:
- Timestamp (Jakarta timezone)
- Logger name
- Log level (INFO, WARNING, ERROR)
- Message with execution mode marker

Example:
```
2024-12-30 14:30:45,123 - src.execution_modes - INFO - [BATCH] Processing 70 items x 5 dates = 350 total API calls
```

### Viewing Logs

**Watch all activity:**
```bash
tail -f logs/pipeline.log
```

**Watch batch processing only:**
```bash
tail -f logs/batch.log
```

**Watch onetime endpoints only:**
```bash
tail -f logs/onetime.log
```

**View latest errors:**
```bash
grep ERROR logs/pipeline.log
```

### Mode-Specific Tracing

When troubleshooting, use the appropriate log file:
- **Batch issues** → Check `logs/batch.log` (cleaner, no onetime noise)
- **Reference data issues** → Check `logs/onetime.log`
- **All activity** → Check `logs/pipeline.log`

## Advanced Features

### Custom Partition Keys

For complex scenarios requiring idempotent writes based on multiple dimensions (e.g., date + stock), use `partition_key`:

```yaml
partition_key:
  - stock_code  # First partition dimension
  - date        # Second partition dimension
```

When re-running the pipeline, only records matching ALL partition key values will be replaced.

### Date Iteration & Range Splitting

For daily backfill or historical data processing:

```yaml
batch_config:
  date_iteration:
    enabled: true
    start_date: "2024-12-01"
    end_date: "2024-12-30"
    date_column: date  # Column name in result

params:
  from: "{date}"  # Replaced with each individual date
  to: "{date}"    # Same date - ensures daily values, not averages
```

**Important:** Date ranges are **automatically split into individual days**. Each day gets a separate API call.

Example:
```bash
# Command:
python main.py --mode batch --start-date 2025-10-20 --end-date 2025-10-24

# What happens:
# Day 1: API call with from=2025-10-20&to=2025-10-20
# Day 2: API call with from=2025-10-21&to=2025-10-21
# Day 3: API call with from=2025-10-22&to=2025-10-22
# Day 4: API call with from=2025-10-23&to=2025-10-23
# Day 5: API call with from=2025-10-24&to=2025-10-24

# Result: 5 separate API calls, each with daily data (not averaged)
```

This ensures you get **true daily values**, not averaged values across a date range.

### Column Injection

When API responses don't include iteration context (e.g., stock_code, date), inject them:

```yaml
inject_columns:
  stock_code: "{stock_code}"  # Injects current stock being processed
  date: "{date}"              # Injects current date being processed
```

This ensures each record has the complete context for partitioning and querying.

### Source Table Filtering

Query batch values from BigQuery with SQL filters:

```yaml
batch_config:
  source_table: syariah_stock_list
  source_column: kode
  source_filter: "indeks = 'JII70'"  # Only JII70 stocks
```

## System Columns

The pipeline automatically adds system columns at the end of every record:

| Column | Description | Example |
|--------|-------------|---------|
| `_sys_ingested_at` | Timestamp when data was ingested (Asia/Jakarta timezone) | `2024-12-30T14:30:45.123456+07:00` |

Business data columns appear first, followed by system columns.

## Idempotency Guarantees

The pipeline is designed to be **idempotent** - running it multiple times produces consistent results without duplicating data:

### Onetime Mode
- **Strategy**: Full table replacement (`WRITE_TRUNCATE`)
- **Behavior**: Replaces ALL data in the table with fresh data
- **Idempotency**: Running multiple times always results in the same final state
- **Use case**: Reference data, lookup tables (stock lists, broker lists)

```bash
# Running this multiple times on the same day = same result
python main.py --mode onetime
```

### Batch Mode
- **Strategy**: Partition replacement (`WRITE_TRUNCATE_PARTITION`)
- **Behavior**: Deletes existing partition(s), then loads fresh data
- **Idempotency**: Re-running with same partition key values replaces only that partition
- **Use case**: Daily/periodic data fetches (broker summaries, historical data)
- **BigQuery Rate Limits**: Data is batched by date to avoid "too many table update operations" errors

```bash
# Running this multiple times with same date = only that date's partition is replaced
python main.py --mode batch
```

**How batching works (date partition)**:
```yaml
partition_key:
  - date  # Partition by date only
```

**Processing flow for 70 stocks on 2024-12-30:**
1. Fetch data from API for all 70 stocks (70 API calls)
2. Collect all records in memory
3. **Single DELETE** operation: `WHERE date='2024-12-30'`
4. **Single INSERT** operation: Load all 70 stocks' data at once
5. Total BigQuery operations: **2** (not 140), avoiding rate limits

**Re-running same date:**
- First run on Dec 30: Loads all stocks for `date='2024-12-30'`
- Second run on Dec 30: Deletes all records WHERE `date='2024-12-30'`, loads fresh data
- Run on Dec 31: Loads data for `date='2024-12-31'`, Dec 30 data unchanged

**Important:** While we partition only by date for BigQuery efficiency, each record still includes `stock_code` and `date` columns for querying.

### Streaming Mode
- **Strategy**: Partition replacement (when implemented)
- **Behavior**: Will replace current hour/minute partition
- **Idempotency**: Re-running in same time window replaces only that window's data
- **Use case**: Real-time data, frequent updates

### Benefits of Idempotency

1. **Safe Re-runs**: Pipeline failures can be safely retried without data duplication
2. **Scheduled Jobs**: Cron/scheduler misconfigurations won't create duplicate records
3. **Data Corrections**: Re-run to fix issues without cleanup scripts
4. **Testing**: Safe to run multiple times during development/testing

## Error Handling

The pipeline includes error handling for:
- Secret Manager access failures
- API request failures
- JSON parsing errors
- BigQuery loading errors

## Scheduling

### Recommended Scheduling Strategy

Different execution modes should run on different schedules:

```bash
# Daily at 1 AM: Update reference data (onetime)
0 1 * * * cd /path/to/playground && python main.py --mode onetime

# Daily at 2 AM: Process batch data (batch)
0 2 * * * cd /path/to/playground && python main.py --mode batch

# Continuous: Streaming data (when implemented)
# Run as a background service
```

### Cloud Scheduler + Cloud Run

1. Containerize the application
2. Deploy to Cloud Run
3. Set up Cloud Scheduler with different schedules for each mode:
   - Onetime: Daily or weekly
   - Batch: Daily or hourly
   - Streaming: Continuous or frequent

### Cloud Functions

Deploy as a Cloud Function with a Pub/Sub or HTTP trigger

### Cron Job

Run locally or on a VM with cron:

```bash
# Edit crontab
crontab -e

# Run daily at 2 AM
0 2 * * * cd /path/to/playground && /path/to/python main.py >> pipeline.log 2>&1
```
