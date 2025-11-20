# Flattened Invezgo Data Pipeline for Cloud Run Functions

This is a simplified, flattened version of the Invezgo to BigQuery data pipeline designed for easy deployment to Google Cloud Run Functions.

## Structure

All code is consolidated into a single `main.py` file for simplicity:
- No folder structure (config/, src/ removed)
- All modules combined into one file
- Same functionality as the original project
- `endpoints.yaml` remains unchanged

## Files

- `main.py` - Complete pipeline code (all-in-one)
- `requirements.txt` - Python dependencies
- `endpoints.yaml` - API endpoints configuration (same as original)
- `README.md` - This file

## Deployment to Cloud Run Functions

### 1. Prerequisites

- Google Cloud Project with billing enabled
- Required APIs enabled:
  - Cloud Functions API
  - Secret Manager API
  - BigQuery API
- API token stored in Secret Manager as `invezgo-api-token`

### 2. Deploy via gcloud CLI

```bash
gcloud functions deploy invezgo-pipeline \
  --gen2 \
  --runtime=python311 \
  --region=asia-southeast1 \
  --source=. \
  --entry-point=invezgo_pipeline \
  --trigger-http \
  --allow-unauthenticated \
  --memory=512MB \
  --timeout=540s
```

### 3. Configuration

Update these variables in `main.py` if needed:

```python
GCP_PROJECT_ID = 'your-project-id'
SECRET_NAME = 'invezgo-api-token'
BQ_DATASET_ID = 'invezgo_data'
BQ_LOCATION = 'asia-southeast1'
```

## Usage

### Trigger via HTTP Request

```bash
# Run all endpoints for today
curl -X POST https://REGION-PROJECT_ID.cloudfunctions.net/invezgo-pipeline \
  -H "Content-Type: application/json" \
  -d '{}'

# Run only batch mode for today
curl -X POST https://REGION-PROJECT_ID.cloudfunctions.net/invezgo-pipeline \
  -H "Content-Type: application/json" \
  -d '{"mode": "batch"}'

# Run for specific date
curl -X POST https://REGION-PROJECT_ID.cloudfunctions.net/invezgo-pipeline \
  -H "Content-Type: application/json" \
  -d '{"mode": "batch", "date": "2024-12-01"}'

# Run for date range
curl -X POST https://REGION-PROJECT_ID.cloudfunctions.net/invezgo-pipeline \
  -H "Content-Type: application/json" \
  -d '{"mode": "batch", "start_date": "2024-12-01", "end_date": "2024-12-10"}'
```

### Request Parameters

- `mode` (optional): `onetime`, `batch`, `streaming`, or `all` (default: `all`)
- `date` (optional): Single date in `YYYY-MM-DD` format
- `start_date` (optional): Start date for range
- `end_date` (optional): End date for range

## Features

✅ Same functionality as original project  
✅ Simplified deployment (no folders)  
✅ Cloud Run Functions optimized  
✅ Supports all execution modes (onetime, batch, streaming)  
✅ Date range iteration  
✅ Partition replacement for idempotency  
✅ Automatic system columns  
✅ Jakarta timezone support  

## Differences from Original

- **Single file**: All code in `main.py` instead of multiple modules
- **No logs folder**: Uses Cloud Logging (stdout only)
- **No .env file**: Configuration via environment or code constants
- **Simplified structure**: No config/ or src/ folders

## Monitoring

View logs in Cloud Console:
```
https://console.cloud.google.com/functions/details/REGION/invezgo-pipeline?tab=logs
```

Or via gcloud:
```bash
gcloud functions logs read invezgo-pipeline --gen2 --region=asia-southeast1
```

## Cost Optimization

- Function timeout: 540s (9 minutes max)
- Memory: 512MB (adjust based on data volume)
- Only runs when triggered (no always-on costs)
- Use Cloud Scheduler for periodic execution

## Schedule with Cloud Scheduler

```bash
# Daily at 6 PM Jakarta time (11 AM UTC)
gcloud scheduler jobs create http invezgo-daily \
  --location=asia-southeast1 \
  --schedule="0 11 * * *" \
  --time-zone="Asia/Jakarta" \
  --uri="https://REGION-PROJECT_ID.cloudfunctions.net/invezgo-pipeline" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{"mode": "batch"}'
```
