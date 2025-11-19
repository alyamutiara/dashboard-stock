"""Execution modes for different types of data pipeline operations."""
import logging
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod

from src.invezgo_client import InvezgoClient
from src.bigquery_loader import BigQueryLoader

logger = logging.getLogger(__name__)


class ExecutionMode(ABC):
    """Base class for execution modes."""
    
    def __init__(
        self,
        invezgo_client: InvezgoClient,
        bq_loader: BigQueryLoader,
        project_id: str,
        dataset_id: str
    ):
        """
        Initialize execution mode.
        
        Args:
            invezgo_client: Invezgo API client
            bq_loader: BigQuery loader
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
        """
        self.invezgo_client = invezgo_client
        self.bq_loader = bq_loader
        self.project_id = project_id
        self.dataset_id = dataset_id
    
    @abstractmethod
    def execute(self, endpoint_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the endpoint with specific mode logic.
        
        Args:
            endpoint_config: Endpoint configuration
            
        Returns:
            Execution result dictionary
        """
        pass


class OnetimeMode(ExecutionMode):
    """
    Onetime execution mode for reference/lookup data.
    Fetches data once and replaces all existing data in BigQuery.
    Idempotent: Running multiple times produces the same result.
    """
    
    def execute(self, endpoint_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute onetime data fetch with full table replacement."""
        endpoint_name = endpoint_config.get('name', 'unknown')
        endpoint_path = endpoint_config.get('path', '')
        bq_table = endpoint_config.get('bq_table', endpoint_name)
        params = endpoint_config.get('params', {})
        path_variables = endpoint_config.get('path_variables', {})
        
        try:
            logger.info(f"[ONETIME] Processing: {endpoint_name}")
            
            # Fetch data
            data = self.invezgo_client.fetch_data(
                endpoint=endpoint_path,
                params=params,
                path_variables=path_variables
            )
            
            if not data:
                logger.warning(f"[ONETIME] No data for: {endpoint_name}")
                return {
                    'endpoint': endpoint_name,
                    'mode': 'onetime',
                    'status': 'success',
                    'records_fetched': 0,
                    'records_loaded': 0
                }
            
            logger.info(f"[ONETIME] Fetched {len(data)} records from {endpoint_name}")
            
            # Load to BigQuery with WRITE_TRUNCATE (replace all existing data)
            # This ensures idempotency - running multiple times gives same result
            rows_loaded = self.bq_loader.load_data(
                self.dataset_id,
                bq_table,
                data,
                write_disposition='WRITE_TRUNCATE'
            )
            
            logger.info(f"[ONETIME] Replaced all data in table (idempotent)")
            
            return {
                'endpoint': endpoint_name,
                'mode': 'onetime',
                'status': 'success',
                'records_fetched': len(data),
                'records_loaded': rows_loaded
            }
            
        except Exception as e:
            logger.error(f"[ONETIME] Failed {endpoint_name}: {e}")
            return {
                'endpoint': endpoint_name,
                'mode': 'onetime',
                'status': 'failed',
                'error': str(e)
            }


class BatchMode(ExecutionMode):
    """
    Batch execution mode for periodic data fetching.
    Iterates through a list of parameters and fetches data for each.
    Supports date iteration for daily backfill.
    Uses partition replacement for idempotency.
    """
    
    def execute(self, endpoint_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute batch data fetch with partition replacement."""
        endpoint_name = endpoint_config.get('name', 'unknown')
        endpoint_path = endpoint_config.get('path', '')
        bq_table = endpoint_config.get('bq_table', endpoint_name)
        params = endpoint_config.get('params', {})
        path_variables = endpoint_config.get('path_variables', {})
        batch_config = endpoint_config.get('batch_config', {})
        partition_key = endpoint_config.get('partition_key', [])
        inject_columns = endpoint_config.get('inject_columns', {})
        
        try:
            logger.info(f"[BATCH] Processing: {endpoint_name}")
            
            # Get batch values for primary iteration (e.g., stock codes)
            batch_values = self._get_batch_values(batch_config)
            
            if not batch_values:
                logger.warning(f"[BATCH] No batch values for: {endpoint_name}")
                return {
                    'endpoint': endpoint_name,
                    'mode': 'batch',
                    'status': 'success',
                    'batches_processed': 0,
                    'total_records_loaded': 0
                }
            
            # Check if date iteration is enabled
            date_iteration = batch_config.get('date_iteration', {})
            date_list = self._get_date_range(date_iteration) if date_iteration.get('enabled') else [None]
            
            if date_list and date_list[0] is not None:
                logger.info(f"[BATCH] Date range split into {len(date_list)} individual days")
                logger.info(f"[BATCH] Each day will be processed separately (not as range)")
            
            logger.info(f"[BATCH] Processing {len(batch_values)} items x {len(date_list)} dates = {len(batch_values) * len(date_list)} total API calls")
            logger.info(f"[BATCH] Using partition replacement for idempotency")
            logger.info(f"[BATCH] Batching by date to avoid BigQuery rate limits")
            
            total_records = 0
            iterate_by = batch_config.get('iterate_by')
            batches_processed = 0
            
            # Process by date first (outer loop) to batch BigQuery operations
            # This avoids rate limits by doing one delete + one insert per date
            for date_idx, date_value in enumerate(date_list, 1):
                logger.info(f"[BATCH] === Processing date {date_idx}/{len(date_list)}: {date_value} ===")
                
                # Collect all data for this date
                date_data = []
                
                # Process each batch value (e.g., each stock) for this date
                for i, value in enumerate(batch_values, 1):
                    batches_processed += 1
                    
                    if date_value:
                        logger.info(f"[BATCH] Fetching [{i}/{len(batch_values)}]: {iterate_by}={value}, date={date_value}")
                    else:
                        logger.info(f"[BATCH] Processing batch {i}/{len(batch_values)}: {iterate_by}={value}")
                    
                    # Update path variables or params with current batch value
                    current_path_vars = path_variables.copy() if path_variables else {}
                    current_params = params.copy() if params else {}
                    
                    # Replace iterate_by value (e.g., stock_code)
                    if iterate_by:
                        if f"{{{iterate_by}}}" in endpoint_path:
                            current_path_vars[iterate_by] = value
                        else:
                            current_params[iterate_by] = value
                    
                    # Replace date placeholders in params
                    if date_value:
                        for param_key, param_value in current_params.items():
                            if isinstance(param_value, str) and '{date}' in param_value:
                                current_params[param_key] = param_value.replace('{date}', date_value)
                        
                        # Log the actual API parameters to show single-day request
                        if 'from' in current_params and 'to' in current_params:
                            logger.debug(f"[BATCH] API params: from={current_params['from']}, to={current_params['to']} (single day)")
                    
                    # Fetch data for this batch
                    data = self.invezgo_client.fetch_data(
                        endpoint=endpoint_path,
                        params=current_params,
                        path_variables=current_path_vars
                    )
                    
                    if data:
                        # Inject additional columns into each record
                        data = self._inject_columns(
                            data, 
                            inject_columns, 
                            {iterate_by: value, 'date': date_value}
                        )
                        
                        # Add to date batch
                        date_data.extend(data)
                
                # Now load all data for this date in one operation (avoids rate limit)
                if date_data:
                    logger.info(f"[BATCH] Loading {len(date_data)} records for date {date_value} (batched)")
                    
                    # Build partition fields for this date
                    partition_fields = self._build_partition_fields(
                        partition_key,
                        {iterate_by: None, 'date': date_value}  # Only date partition, not stock_code
                    )
                    
                    # Determine write disposition based on whether partition keys are configured
                    if partition_fields:
                        # Has partition keys (e.g., date) - use partition replacement for idempotency
                        write_disposition = 'WRITE_TRUNCATE_PARTITION'
                        logger.info(f"[BATCH] Using partition replacement (idempotent) for partition: {partition_fields}")
                    else:
                        # No partition keys - always append (no replacement)
                        write_disposition = 'WRITE_APPEND'
                        logger.info(f"[BATCH] Using append mode (no partition replacement)")
                    
                    # Single delete + insert operation for all stocks on this date (or append if no partition)
                    rows = self.bq_loader.load_data(
                        self.dataset_id,
                        bq_table,
                        date_data,
                        write_disposition=write_disposition,
                        partition_fields=partition_fields
                    )
                    total_records += rows
                    logger.info(f"[BATCH] Successfully loaded {rows} records for date {date_value}")
                else:
                    logger.warning(f"[BATCH] No data collected for date {date_value}")
            
            logger.info(f"[BATCH] Completed with partition replacement (idempotent)")
            
            return {
                'endpoint': endpoint_name,
                'mode': 'batch',
                'status': 'success',
                'batches_processed': len(batch_values),
                'total_records_loaded': total_records
            }
            
        except Exception as e:
            logger.error(f"[BATCH] Failed {endpoint_name}: {e}")
            return {
                'endpoint': endpoint_name,
                'mode': 'batch',
                'status': 'failed',
                'error': str(e)
            }
    
    def _get_batch_values(self, batch_config: Dict[str, Any]) -> List[Any]:
        """
        Get list of values to iterate through in batch mode.
        
        Args:
            batch_config: Batch configuration
            
        Returns:
            List of values to process
        """
        # Option 1: Use static list
        if 'values' in batch_config and batch_config['values']:
            return batch_config['values']
        
        # Option 2: Query from BigQuery table
        if 'source_table' in batch_config and 'source_column' in batch_config:
            try:
                source_table = batch_config['source_table']
                source_column = batch_config['source_column']
                source_filter = batch_config.get('source_filter', '')
                
                # Build query with optional WHERE filter
                where_clause = f"WHERE {source_filter}" if source_filter else "WHERE {source_column} IS NOT NULL"
                
                query = f"""
                    SELECT DISTINCT {source_column}
                    FROM `{self.project_id}.{self.dataset_id}.{source_table}`
                    {where_clause}
                    ORDER BY {source_column}
                """
                
                logger.info(f"[BATCH] Querying batch values from {source_table}.{source_column}")
                if source_filter:
                    logger.info(f"[BATCH] Applying filter: {source_filter}")
                    
                results = self.bq_loader.client.query(query).result()
                values = [row[source_column] for row in results]
                logger.info(f"[BATCH] Found {len(values)} values from BigQuery")
                return values
                
            except Exception as e:
                logger.error(f"[BATCH] Failed to query batch values: {e}")
                return []
        
        return []
    
    def _get_date_range(self, date_iteration: Dict[str, Any]) -> List[str]:
        """
        Generate list of dates for iteration.
        
        Args:
            date_iteration: Date iteration configuration
            
        Returns:
            List of date strings in YYYY-MM-DD format
        """
        from datetime import datetime, timedelta
        
        start_date_str = date_iteration.get('start_date')
        end_date_str = date_iteration.get('end_date')
        
        if not start_date_str or not end_date_str:
            return []
        
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            
            date_list = []
            current_date = start_date
            while current_date <= end_date:
                date_list.append(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)
            
            logger.info(f"[BATCH] Generated {len(date_list)} dates from {start_date_str} to {end_date_str}")
            return date_list
            
        except Exception as e:
            logger.error(f"[BATCH] Error generating date range: {e}")
            return []
    
    def _inject_columns(
        self, 
        data: List[Dict[str, Any]], 
        inject_columns: Dict[str, str],
        context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Inject additional columns into each data record.
        
        Args:
            data: List of data records
            inject_columns: Column definitions with placeholders
            context: Context values to replace placeholders
            
        Returns:
            Data with injected columns
        """
        if not inject_columns:
            return data
        
        for record in data:
            for col_name, col_template in inject_columns.items():
                # Replace placeholders with actual values
                col_value = col_template
                for key, value in context.items():
                    if value is not None:
                        col_value = col_value.replace(f'{{{key}}}', str(value))
                record[col_name] = col_value
        
        return data
    
    def _build_partition_fields(
        self,
        partition_key: List[str],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Build partition fields dictionary from partition key configuration.
        
        Args:
            partition_key: List of field names to use as partition keys
            context: Context values containing partition key values
            
        Returns:
            Dictionary of partition fields and values
        """
        if not partition_key:
            return {}
        
        partition_fields = {}
        for key in partition_key:
            if key in context and context[key] is not None:
                partition_fields[key] = context[key]
        
        return partition_fields


class StreamingMode(ExecutionMode):
    """
    Streaming execution mode for real-time or frequent data updates.
    Uses partition replacement for idempotency - running multiple times
    replaces only the current partition (date/hour).
    
    TODO: Implement actual streaming logic with polling/scheduling.
    """
    
    def execute(self, endpoint_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute streaming data fetch with partition replacement."""
        endpoint_name = endpoint_config.get('name', 'unknown')
        
        logger.info(f"[STREAMING] {endpoint_name} - Not yet implemented")
        logger.info(f"[STREAMING] Will use partition replacement for idempotency when implemented")
        
        # TODO: Implement streaming logic:
        # 1. Poll endpoint at configured intervals
        # 2. Get current timestamp/date for partition
        # 3. Use write_disposition='WRITE_TRUNCATE_PARTITION' with partition_field='_sys_ingestion_date'
        # 4. This ensures idempotency - re-running replaces only current period's data
        
        return {
            'endpoint': endpoint_name,
            'mode': 'streaming',
            'status': 'not_implemented',
            'message': 'Streaming mode will use partition replacement for idempotency'
        }
