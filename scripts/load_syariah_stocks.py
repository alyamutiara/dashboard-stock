"""
Script to load syariah stock list from CSV to BigQuery.
This creates the syariah_stock_list table needed for batch processing.
"""
import os
import sys
import csv
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.bigquery_loader import BigQueryLoader
from config.settings import GCP_PROJECT_ID, BQ_DATASET_ID

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_csv_to_bigquery():
    """Load indeks_syariah.csv to BigQuery."""
    try:
        # Get file path
        csv_path = Path(__file__).parent.parent / 'data' / 'indeks_syariah.csv'
        
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return False
        
        logger.info(f"Reading CSV file: {csv_path}")
        
        # Read CSV with semicolon delimiter
        data = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                # Convert column names to lowercase
                data.append({
                    'indeks': row['Indeks'].strip(),
                    'kode': row['Kode'].strip()
                })
        
        logger.info(f"Read {len(data)} records from CSV")
        
        # Initialize BigQuery loader
        bq_loader = BigQueryLoader(GCP_PROJECT_ID)
        
        # Create dataset if not exists
        logger.info(f"Ensuring dataset exists: {BQ_DATASET_ID}")
        bq_loader.create_dataset(BQ_DATASET_ID, location='asia-southeast1')
        
        # Load data to BigQuery (replace existing data)
        logger.info(f"Loading data to BigQuery table: syariah_stock_list")
        
        # Don't add system columns for this reference table
        # Load directly without _add_system_columns
        from google.cloud import bigquery
        
        table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.syariah_stock_list"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )
        
        load_job = bq_loader.client.load_table_from_json(
            data,
            table_ref,
            job_config=job_config
        )
        load_job.result()
        
        logger.info(f"Successfully loaded {len(data)} records to BigQuery")
        logger.info(f"Table: {table_ref}")
        
        # Show sample data
        logger.info("\nSample data from table:")
        query = f"""
            SELECT indeks, COUNT(*) as count
            FROM `{table_ref}`
            GROUP BY indeks
            ORDER BY indeks
        """
        results = bq_loader.client.query(query).result()
        for row in results:
            logger.info(f"  {row['indeks']}: {row['count']} stocks")
        
        return True
        
    except Exception as e:
        logger.error(f"Error loading CSV to BigQuery: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = load_csv_to_bigquery()
    sys.exit(0 if success else 1)
