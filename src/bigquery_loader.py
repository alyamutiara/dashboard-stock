"""Module for loading data into BigQuery."""
import logging
from typing import Dict, List, Any
from datetime import datetime
import pytz
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Loader for BigQuery operations."""
    
    def __init__(self, project_id: str):
        """
        Initialize BigQuery loader.
        
        Args:
            project_id: GCP project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.jakarta_tz = pytz.timezone('Asia/Jakarta')
    
    def _add_system_columns(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Add system columns to data records at the end.
        
        Args:
            data: List of data records
            
        Returns:
            Data with system columns added at the end
        """
        # Get current timestamp in Jakarta timezone
        jakarta_now = datetime.now(self.jakarta_tz)
        timestamp_str = jakarta_now.isoformat()
        date_str = jakarta_now.strftime('%Y-%m-%d')
        
        # Add system columns at the end of each record
        result = []
        for record in data:
            # Create new ordered dict with original fields first
            new_record = {k: v for k, v in record.items() if not k.startswith('_sys_')}
            # Add system columns at the end (using Jakarta timezone)
            new_record['_sys_ingested_at'] = timestamp_str
            result.append(new_record)
        
        return result
    
    def _delete_partition(
        self,
        dataset_id: str,
        table_id: str,
        partition_fields: Dict[str, Any]
    ) -> None:
        """
        Delete existing partition data to enable idempotent writes.
        Supports multiple partition keys (e.g., date + stock_code).
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            partition_fields: Dict of field names and values to filter on
                             e.g., {'date': '2024-12-01', 'stock_code': 'BBCA'}
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            
            # Build WHERE clause with multiple conditions
            where_conditions = []
            for field, value in partition_fields.items():
                where_conditions.append(f"{field} = '{value}'")
            
            where_clause = " AND ".join(where_conditions)
            
            # Delete existing data for this partition
            delete_query = f"""
                DELETE FROM `{table_ref}`
                WHERE {where_clause}
            """
            
            partition_desc = ", ".join([f"{k}={v}" for k, v in partition_fields.items()])
            logger.info(f"Deleting partition: {partition_desc} from {table_ref}")
            query_job = self.client.query(delete_query)
            query_job.result()  # Wait for deletion
            logger.info(f"Partition deleted successfully")
            
        except Exception as e:
            # If table doesn't exist yet, that's fine
            if "Not found" in str(e):
                logger.info(f"Table not found, will be created: {table_ref}")
            else:
                logger.warning(f"Error deleting partition: {e}")
    
    def load_data(
        self,
        dataset_id: str,
        table_id: str,
        data: List[Dict[str, Any]],
        write_disposition: str = 'WRITE_APPEND',
        partition_fields: Dict[str, Any] = None
    ) -> int:
        """
        Load data into BigQuery table with automatic system columns.
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            data: List of dictionaries to load
            write_disposition: Write disposition (WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY)
            partition_fields: Dict of partition fields for idempotent writes
                             e.g., {'date': '2024-12-01', 'stock_code': 'BBCA'}
        
        Returns:
            Number of rows loaded
            
        Raises:
            Exception: If data loading fails
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

            # Add system columns to data
            data_with_system_cols = self._add_system_columns(data)

            # Normalize and handle write disposition
            normalized_disposition = write_disposition

            # Special handling for custom partition truncate mode
            if isinstance(normalized_disposition, str) and normalized_disposition.upper() == 'WRITE_TRUNCATE_PARTITION':
                if partition_fields:
                    # Delete existing rows for the target partition keys, then append
                    self._delete_partition(dataset_id, table_id, partition_fields)
                    normalized_disposition = 'WRITE_APPEND'
                else:
                    # No partition provided; avoid truncating entire table unexpectedly
                    logger.warning(
                        "WRITE_TRUNCATE_PARTITION requested without partition_fields; falling back to WRITE_APPEND"
                    )
                    normalized_disposition = 'WRITE_APPEND'

            # Map to a valid BigQuery WriteDisposition enum
            if isinstance(normalized_disposition, str):
                try:
                    bq_write_disposition = getattr(
                        bigquery.WriteDisposition, normalized_disposition
                    )
                except AttributeError:
                    raise ValueError(
                        f"Invalid write_disposition: {write_disposition}. "
                        "Use one of: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY, or the special WRITE_TRUNCATE_PARTITION with partition_fields."
                    )
            else:
                # Assume caller passed an enum value already
                bq_write_disposition = normalized_disposition

            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bq_write_disposition,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,  # Auto-detect schema from JSON
            )
            
            logger.info(f"Loading {len(data_with_system_cols)} records to BigQuery: {table_ref}")
            
            # Load data
            load_job = self.client.load_table_from_json(
                data_with_system_cols,
                table_ref,
                job_config=job_config
            )
            
            # Wait for job to complete
            load_job.result()
            
            logger.info(f"Successfully loaded {len(data_with_system_cols)} records to BigQuery")
            return len(data_with_system_cols)
            
        except Exception as e:
            logger.error(f"Error loading data to BigQuery: {e}")
            raise
    
    def create_dataset(self, dataset_id: str, location: str = 'US') -> None:
        """
        Create BigQuery dataset if it doesn't exist.
        
        Args:
            dataset_id: Dataset ID to create
            location: Dataset location
        """
        try:
            dataset_ref = f"{self.project_id}.{dataset_id}"
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {dataset_id} ready")
            
        except Exception as e:
            logger.error(f"Error creating dataset: {e}")
            raise
    
    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """
        Check if table exists.
        
        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False
