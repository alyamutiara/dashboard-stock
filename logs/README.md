# Logs Directory

This directory contains log files for the Invezgo data pipeline.

## Log Files

| File | Purpose | When to Use |
|------|---------|-------------|
| `pipeline.log` | **Main log** - Contains all logs from all modes | General monitoring, complete history |
| `onetime.log` | **Onetime mode** - Reference data fetches | Troubleshoot stock lists, broker lists issues |
| `batch.log` | **Batch mode** - Daily data processing | Troubleshoot broker summary, batch processing issues |
| `streaming.log` | **Streaming mode** - Real-time data | Troubleshoot streaming issues (when implemented) |

## Log File Rotation

Logs are **not** automatically rotated. For production deployments, consider using:

**Linux/Mac (logrotate):**
```bash
# /etc/logrotate.d/invezgo-pipeline
/path/to/playground/logs/*.log {
    daily
    rotate 30
    compress
    missingok
    notifempty
}
```

**Manual cleanup:**
```bash
# Keep only last 30 days
find logs/ -name "*.log" -mtime +30 -delete
```

## Reading Logs

### Real-time monitoring

**Watch all activity:**
```bash
tail -f logs/pipeline.log
```

**Watch batch processing only:**
```bash
tail -f logs/batch.log
```

**Watch both batch and main logs (split screen):**
```bash
# Terminal 1
tail -f logs/pipeline.log

# Terminal 2  
tail -f logs/batch.log
```

### Search for errors

**All errors:**
```bash
grep ERROR logs/pipeline.log
```

**Batch errors only:**
```bash
grep ERROR logs/batch.log
```

**Errors from today:**
```bash
grep "$(date +%Y-%m-%d)" logs/pipeline.log | grep ERROR
```

### Filter by endpoint

**Broker summary logs:**
```bash
grep "broker_summary" logs/batch.log
```

**Stock list logs:**
```bash
grep "stock_list" logs/onetime.log
```

### Filter by date

**Logs from specific date:**
```bash
grep "2024-12-30" logs/batch.log
```

**Last hour of logs:**
```bash
grep "$(date +%Y-%m-%d' '%H):" logs/pipeline.log
```

## Log Format

Each log line contains:
```
YYYY-MM-DD HH:MM:SS,mmm - logger_name - LEVEL - [MODE] message
```

Example:
```
2024-12-30 14:30:45,123 - src.execution_modes - INFO - [BATCH] Processing 70 items x 5 dates = 350 total API calls
```

- **Timestamp**: `2024-12-30 14:30:45,123` (Jakarta timezone)
- **Logger**: `src.execution_modes`
- **Level**: `INFO`, `WARNING`, `ERROR`
- **Mode**: `[ONETIME]`, `[BATCH]`, `[STREAMING]` (or none for general logs)
- **Message**: Descriptive text

## Troubleshooting with Logs

### Problem: Batch processing is slow

**Check:**
```bash
# Count API calls being made
grep "Fetching data from Invezgo API" logs/batch.log | wc -l

# Check processing time per stock
grep "Processing batch" logs/batch.log
```

### Problem: Missing data for specific date

**Check:**
```bash
# See if date was processed
grep "date=2024-12-01" logs/batch.log

# Check for errors on that date
grep "2024-12-01" logs/batch.log | grep ERROR
```

### Problem: API errors

**Check:**
```bash
# Find API errors
grep "Error fetching data from Invezgo API" logs/batch.log

# Check HTTP errors
grep "raise_for_status" logs/pipeline.log
```

### Problem: BigQuery loading fails

**Check:**
```bash
# Find BigQuery errors
grep "Error loading data to BigQuery" logs/pipeline.log

# Check for partition deletion errors
grep "Error deleting partition" logs/batch.log
```

## Best Practices

1. **Regular cleanup**: Don't let logs grow indefinitely
2. **Monitor disk space**: Large backfills can generate large logs
3. **Use mode-specific logs**: Easier to trace issues
4. **Archive important runs**: Keep logs from critical backfills
5. **Check logs after runs**: Verify success before scheduling

## Log Retention Recommendations

- **Development**: Keep 7 days
- **Production (scheduled)**: Keep 30 days
- **Production (backfill)**: Archive permanently
- **Pipeline.log**: Keep longest (30-90 days)
- **Mode-specific logs**: Keep shorter (7-30 days)
