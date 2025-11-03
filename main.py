"""
Invezgo to BigQuery Data Pipeline
Main entry point for pipeline execution.
"""
import sys
import logging
import argparse
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    setup_logging,
    GCP_PROJECT_ID,
    SECRET_NAME,
    SECRET_VERSION,
    INVEZGO_API_BASE_URL,
    BQ_DATASET_ID,
    get_enabled_endpoints
)
from src.pipeline import DataPipeline

# Note: setup_logging() will be called in main() after parsing arguments
logger = None  # Will be initialized after setup_logging()


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Invezgo to BigQuery Data Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Execution Modes:
  onetime   - Fetch reference/lookup data (runs once, replaces existing data)
  batch     - Fetch data with multiple parameters (runs periodically, appends data)
  streaming - Real-time data ingestion (continuous polling)
  
Date Options:
  --date            Process specific date (YYYY-MM-DD format, Jakarta timezone)
  --start-date      Start date for date range (YYYY-MM-DD)
  --end-date        End date for date range (YYYY-MM-DD)
  
  If no date options provided, uses today's date in Jakarta timezone.
  
Examples:
  # Run all enabled endpoints for today
  python main.py
  
  # Run only onetime endpoints
  python main.py --mode onetime
  
  # Run batch endpoints for specific date (backdate)
  python main.py --mode batch --date 2024-12-01
  
  # Run batch endpoints for date range (backfill)
  python main.py --mode batch --start-date 2024-12-01 --end-date 2024-12-10
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['onetime', 'batch', 'streaming', 'all'],
        default='all',
        help='Execution mode filter (default: all)'
    )
    
    parser.add_argument(
        '--date',
        type=str,
        help='Process specific date in YYYY-MM-DD format (Jakarta timezone)'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date for date range in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for date range in YYYY-MM-DD format'
    )
    
    return parser.parse_args()


def main():
    """Main execution function."""
    try:
        args = parse_arguments()
        
        # Setup logging with mode filter for separate log files
        mode_filter = None if args.mode == 'all' else args.mode
        setup_logging(mode_filter=mode_filter)
        
        # Initialize logger after setup_logging
        global logger
        logger = logging.getLogger(__name__)
        
        logger.info("="*60)
        logger.info("Starting Invezgo to BigQuery Data Pipeline")
        logger.info("="*60)
        
        # Log which log files are being written
        from config.settings import LOG_DIR
        logger.info(f"Logs directory: {LOG_DIR}")
        logger.info(f"Main log: pipeline.log (all modes)")
        if mode_filter == 'onetime' or mode_filter is None:
            logger.info(f"Onetime log: onetime.log (onetime mode only)")
        if mode_filter == 'batch' or mode_filter is None:
            logger.info(f"Batch log: batch.log (batch mode only)")
        if mode_filter == 'streaming' or mode_filter is None:
            logger.info(f"Streaming log: streaming.log (streaming mode only)")
        logger.info("="*60)
        
        # Determine date range
        from datetime import datetime
        import pytz
        
        jakarta_tz = pytz.timezone('Asia/Jakarta')
        today = datetime.now(jakarta_tz).strftime('%Y-%m-%d')
        
        # Handle date arguments
        if args.date:
            # Single date mode
            start_date = args.date
            end_date = args.date
            logger.info(f"Date mode: Processing single date {args.date}")
        elif args.start_date and args.end_date:
            # Date range mode
            start_date = args.start_date
            end_date = args.end_date
            logger.info(f"Date range mode: {start_date} to {end_date}")
        elif args.start_date or args.end_date:
            # Error: both start and end required
            logger.error("Both --start-date and --end-date must be provided for date range")
            sys.exit(1)
        else:
            # Default: today's date in Jakarta timezone
            start_date = today
            end_date = today
            logger.info(f"Auto-detected date (Jakarta timezone): {today}")
        
        # Load endpoints configuration
        endpoints = get_enabled_endpoints()
        
        if not endpoints:
            logger.warning("No enabled endpoints found in configuration")
            logger.info("Please check config/endpoints.yaml")
            sys.exit(1)
        
        # Override date_iteration in endpoints with command-line dates
        for endpoint in endpoints:
            batch_config = endpoint.get('batch_config', {})
            date_iteration = batch_config.get('date_iteration', {})
            
            if date_iteration.get('enabled'):
                # Override with command-line dates
                date_iteration['start_date'] = start_date
                date_iteration['end_date'] = end_date
                logger.info(f"Overriding date range for {endpoint['name']}: {start_date} to {end_date}")
        
        if mode_filter:
            logger.info(f"Mode filter: {mode_filter}")
        
        # Show endpoints summary
        mode_summary = {}
        for ep in endpoints:
            mode = ep.get('execution_mode', 'onetime')
            mode_summary[mode] = mode_summary.get(mode, 0) + 1
        
        logger.info(f"Found {len(endpoints)} enabled endpoint(s):")
        for mode, count in sorted(mode_summary.items()):
            logger.info(f"  {mode}: {count} endpoint(s)")
        
        # List individual endpoints
        for ep in endpoints:
            mode = ep.get('execution_mode', 'onetime')
            logger.info(f"  - [{mode.upper()}] {ep['name']}: {ep.get('description', 'No description')}")
        
        # Initialize pipeline
        pipeline = DataPipeline(
            project_id=GCP_PROJECT_ID,
            secret_name=SECRET_NAME,
            invezgo_base_url=INVEZGO_API_BASE_URL,
            bq_dataset_id=BQ_DATASET_ID,
            secret_version=SECRET_VERSION
        )
        
        # Run pipeline
        result = pipeline.run(endpoints_config=endpoints, mode_filter=mode_filter)
        
        # Log results
        logger.info("="*60)
        logger.info("Pipeline Execution Summary")
        logger.info("="*60)
        logger.info(f"Status: {result['status']}")
        logger.info(f"Endpoints Processed: {result.get('endpoints_processed', 0)}")
        logger.info(f"Endpoints Failed: {result.get('endpoints_failed', 0)}")
        logger.info(f"Total Records Loaded: {result.get('total_records_loaded', 0)}")
        
        # Show mode breakdown
        if result.get('mode_counts'):
            logger.info("\nBreakdown by Mode:")
            for mode, count in sorted(result['mode_counts'].items()):
                logger.info(f"  {mode}: {count} endpoint(s)")
        
        # Log individual endpoint results
        if result.get('results'):
            logger.info("\nIndividual Endpoint Results:")
            for ep_result in result['results']:
                status_icon = "[OK]" if ep_result['status'] == 'success' else "[FAIL]"
                mode = ep_result.get('mode', 'unknown')
                endpoint = ep_result['endpoint']
                
                # Handle different result formats
                if 'records_loaded' in ep_result:
                    records = ep_result['records_loaded']
                elif 'total_records_loaded' in ep_result:
                    records = ep_result['total_records_loaded']
                else:
                    records = 0
                
                logger.info(f"  {status_icon} [{mode.upper()}] {endpoint}: {records} records")
                
                # Show batch details
                if mode == 'batch' and 'batches_processed' in ep_result:
                    logger.info(f"      Batches processed: {ep_result['batches_processed']}")
                
                if ep_result['status'] == 'failed':
                    logger.error(f"      Error: {ep_result.get('error', 'Unknown error')}")
        
        if result['status'] == 'failed':
            logger.error(f"\nError: {result.get('error', 'Unknown error')}")
            sys.exit(1)
        
        logger.info("="*60)
        
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
