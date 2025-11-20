"""
Flattened Invezgo to BigQuery Data Pipeline for Cloud Run Functions.
All modules combined into a single file for simple deployment.
"""
import logging
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pytz
import yaml
from pathlib import Path

# Third-party imports
import requests
from google.cloud import secretmanager, bigquery
import functions_framework
from flask import jsonify

# =============================================================================
# CONFIGURATION
# =============================================================================

# GCP Configuration
GCP_PROJECT_ID = 'emerald-skill-352503'
SECRET_NAME = 'invezgo-api-token'
SECRET_VERSION = 'latest'

# Invezgo API Configuration
INVEZGO_API_BASE_URL = 'https://api.invezgo.com/'

# BigQuery Configuration
BQ_DATASET_ID = 'invezgo_data'
BQ_LOCATION = 'asia-southeast1'

# Endpoints Configuration File
ENDPOINTS_CONFIG_FILE = Path(__file__).parent / 'endpoints.yaml'

# Configure logging for Cloud Run
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# =============================================================================
# SECRET MANAGER
# =============================================================================

def get_secret(project_id: str, secret_name: str, version: str = 'latest') -> str:
    """Retrieve secret from GCP Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
        response = client.access_secret_version(request={"name": name})
        secret_value = response.payload.data.decode('UTF-8')
        logger.info(f"Successfully retrieved secret: {secret_name}")
        return secret_value
    except Exception as e:
        logger.error(f"Error retrieving secret from GCP: {e}")
        raise

# =============================================================================
# INVEZGO API CLIENT
# =============================================================================

class InvezgoClient:
    """Client for interacting with Invezgo API."""
    
    def __init__(self, base_url: str, api_token: str):
        self.base_url = base_url.rstrip('/')
        self.api_token = api_token
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        })
    
    def fetch_data(self, endpoint: str, params: Dict = None, path_variables: Dict = None) -> List[Dict[str, Any]]:
        """Fetch data from Invezgo API endpoint."""
        try:
            if path_variables:
                for key, value in path_variables.items():
                    endpoint = endpoint.replace(f'{{{key}}}', str(value))
            
            url = f"{self.base_url}{endpoint}"
            logger.info(f"Fetching data from Invezgo API: {url}")
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched data from Invezgo API")
            
            return self._normalize_response(data)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from Invezgo API: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")
            raise
    
    def _normalize_response(self, data: Any) -> List[Dict[str, Any]]:
        """Normalize API response to list of dictionaries."""
        if isinstance(data, dict):
            if 'data' in data:
                return data['data'] if isinstance(data['data'], list) else [data['data']]
            return [data]
        elif isinstance(data, list):
            return data
        else:
            logger.warning(f"Unexpected response format: {type(data)}")
            return [{'raw_data': str(data)}]
    
    def close(self):
        """Close the session."""
        self.session.close()

# =============================================================================
# BIGQUERY LOADER
# =============================================================================

class BigQueryLoader:
    """Loader for BigQuery operations."""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.jakarta_tz = pytz.timezone('Asia/Jakarta')
    
    def _add_system_columns(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add system columns to data records."""
        jakarta_now = datetime.now(self.jakarta_tz)
        timestamp_str = jakarta_now.isoformat()
        
        result = []
        for record in data:
            new_record = {k: v for k, v in record.items() if not k.startswith('_sys_')}
            new_record['_sys_ingested_at'] = timestamp_str
            result.append(new_record)
        
        return result
    
    def _delete_partition(self, dataset_id: str, table_id: str, partition_fields: Dict[str, Any]) -> None:
        """Delete existing partition data for idempotent writes."""
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            
            where_conditions = []
            for field, value in partition_fields.items():
                where_conditions.append(f"{field} = '{value}'")
            
            where_clause = " AND ".join(where_conditions)
            
            delete_query = f"DELETE FROM `{table_ref}` WHERE {where_clause}"
            
            partition_desc = ", ".join([f"{k}={v}" for k, v in partition_fields.items()])
            logger.info(f"Deleting partition: {partition_desc} from {table_ref}")
            query_job = self.client.query(delete_query)
            query_job.result()
            logger.info(f"Partition deleted successfully")
            
        except Exception as e:
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
        """Load data into BigQuery table with automatic system columns."""
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            data_with_system_cols = self._add_system_columns(data)
            normalized_disposition = write_disposition

            if isinstance(normalized_disposition, str) and normalized_disposition.upper() == 'WRITE_TRUNCATE_PARTITION':
                if partition_fields:
                    self._delete_partition(dataset_id, table_id, partition_fields)
                    normalized_disposition = 'WRITE_APPEND'
                else:
                    logger.warning("WRITE_TRUNCATE_PARTITION requested without partition_fields; falling back to WRITE_APPEND")
                    normalized_disposition = 'WRITE_APPEND'

            if isinstance(normalized_disposition, str):
                try:
                    bq_write_disposition = getattr(bigquery.WriteDisposition, normalized_disposition)
                except AttributeError:
                    raise ValueError(f"Invalid write_disposition: {write_disposition}")
            else:
                bq_write_disposition = normalized_disposition

            job_config = bigquery.LoadJobConfig(
                write_disposition=bq_write_disposition,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
            )
            
            logger.info(f"Loading {len(data_with_system_cols)} records to BigQuery: {table_ref}")
            
            load_job = self.client.load_table_from_json(
                data_with_system_cols,
                table_ref,
                job_config=job_config
            )
            
            load_job.result()
            
            logger.info(f"Successfully loaded {len(data_with_system_cols)} records to BigQuery")
            return len(data_with_system_cols)
            
        except Exception as e:
            logger.error(f"Error loading data to BigQuery: {e}")
            raise
    
    def create_dataset(self, dataset_id: str, location: str = 'US') -> None:
        """Create BigQuery dataset if it doesn't exist."""
        try:
            dataset_ref = f"{self.project_id}.{dataset_id}"
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {dataset_id} ready")
            
        except Exception as e:
            logger.error(f"Error creating dataset: {e}")
            raise

# =============================================================================
# EXECUTION MODES
# =============================================================================

class OnetimeMode:
    """Onetime execution mode for reference/lookup data."""
    
    def __init__(self, invezgo_client: InvezgoClient, bq_loader: BigQueryLoader, project_id: str, dataset_id: str):
        self.invezgo_client = invezgo_client
        self.bq_loader = bq_loader
        self.project_id = project_id
        self.dataset_id = dataset_id
    
    def execute(self, endpoint_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute onetime data fetch with full table replacement."""
        endpoint_name = endpoint_config.get('name', 'unknown')
        endpoint_path = endpoint_config.get('path', '')
        bq_table = endpoint_config.get('bq_table', endpoint_name)
        params = endpoint_config.get('params', {})
        path_variables = endpoint_config.get('path_variables', {})
        
        try:
            logger.info(f"[ONETIME] Processing: {endpoint_name}")
            
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


class BatchMode:
    """Batch execution mode for periodic data fetching."""
    
    def __init__(self, invezgo_client: InvezgoClient, bq_loader: BigQueryLoader, project_id: str, dataset_id: str):
        self.invezgo_client = invezgo_client
        self.bq_loader = bq_loader
        self.project_id = project_id
        self.dataset_id = dataset_id
    
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
            
            date_iteration = batch_config.get('date_iteration', {})
            date_list = self._get_date_range(date_iteration) if date_iteration.get('enabled') else [None]
            
            if date_list and date_list[0] is not None:
                logger.info(f"[BATCH] Date range split into {len(date_list)} individual days")
            
            logger.info(f"[BATCH] Processing {len(batch_values)} items x {len(date_list)} dates = {len(batch_values) * len(date_list)} total API calls")
            
            total_records = 0
            iterate_by = batch_config.get('iterate_by')
            batches_processed = 0
            
            for date_idx, date_value in enumerate(date_list, 1):
                if date_value:
                    logger.info(f"[BATCH] === Processing date {date_idx}/{len(date_list)}: {date_value} ===")
                
                date_data = []
                
                for i, value in enumerate(batch_values, 1):
                    batches_processed += 1
                    
                    if date_value:
                        logger.info(f"[BATCH] Fetching [{i}/{len(batch_values)}]: {iterate_by}={value}, date={date_value}")
                    else:
                        logger.info(f"[BATCH] Processing batch {i}/{len(batch_values)}: {iterate_by}={value}")
                    
                    current_path_vars = path_variables.copy() if path_variables else {}
                    current_params = params.copy() if params else {}
                    
                    if iterate_by:
                        if f"{{{iterate_by}}}" in endpoint_path:
                            current_path_vars[iterate_by] = value
                        else:
                            current_params[iterate_by] = value
                    
                    if date_value:
                        for param_key, param_value in current_params.items():
                            if isinstance(param_value, str) and '{date}' in param_value:
                                current_params[param_key] = param_value.replace('{date}', date_value)
                    
                    data = self.invezgo_client.fetch_data(
                        endpoint=endpoint_path,
                        params=current_params,
                        path_variables=current_path_vars
                    )
                    
                    if data:
                        data = self._inject_columns(
                            data, 
                            inject_columns, 
                            {iterate_by: value, 'date': date_value}
                        )
                        date_data.extend(data)
                
                if date_data:
                    logger.info(f"[BATCH] Loading {len(date_data)} records for date {date_value} (batched)")
                    
                    partition_fields = self._build_partition_fields(
                        partition_key,
                        {iterate_by: None, 'date': date_value}
                    )
                    
                    if partition_fields:
                        write_disposition = 'WRITE_TRUNCATE_PARTITION'
                        logger.info(f"[BATCH] Using partition replacement (idempotent) for partition: {partition_fields}")
                    else:
                        write_disposition = 'WRITE_APPEND'
                        logger.info(f"[BATCH] Using append mode (no partition replacement)")
                    
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
        """Get list of values to iterate through in batch mode."""
        if 'values' in batch_config and batch_config['values']:
            return batch_config['values']
        
        if 'source_table' in batch_config and 'source_column' in batch_config:
            try:
                source_table = batch_config['source_table']
                source_column = batch_config['source_column']
                source_filter = batch_config.get('source_filter', '')
                
                where_clause = f"WHERE {source_filter}" if source_filter else f"WHERE {source_column} IS NOT NULL"
                
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
        """Generate list of dates for iteration."""
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
        """Inject additional columns into each data record."""
        if not inject_columns:
            return data
        
        for record in data:
            for col_name, col_template in inject_columns.items():
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
        """Build partition fields dictionary from partition key configuration."""
        if not partition_key:
            return {}
        
        partition_fields = {}
        for key in partition_key:
            if key in context and context[key] is not None:
                partition_fields[key] = context[key]
        
        return partition_fields


class StreamingMode:
    """Streaming execution mode for real-time or frequent data updates."""
    
    def __init__(self, invezgo_client: InvezgoClient, bq_loader: BigQueryLoader, project_id: str, dataset_id: str):
        self.invezgo_client = invezgo_client
        self.bq_loader = bq_loader
        self.project_id = project_id
        self.dataset_id = dataset_id
    
    def execute(self, endpoint_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute streaming data fetch."""
        endpoint_name = endpoint_config.get('name', 'unknown')
        
        logger.info(f"[STREAMING] {endpoint_name} - Not yet implemented")
        
        return {
            'endpoint': endpoint_name,
            'mode': 'streaming',
            'status': 'not_implemented',
            'message': 'Streaming mode not yet implemented'
        }

# =============================================================================
# PIPELINE ORCHESTRATOR
# =============================================================================

class DataPipeline:
    """Data pipeline orchestrator for multiple endpoints."""
    
    def __init__(
        self,
        project_id: str,
        secret_name: str,
        invezgo_base_url: str,
        bq_dataset_id: str,
        secret_version: str = 'latest'
    ):
        self.project_id = project_id
        self.secret_name = secret_name
        self.secret_version = secret_version
        self.invezgo_base_url = invezgo_base_url
        self.bq_dataset_id = bq_dataset_id
        
        self.invezgo_client: Optional[InvezgoClient] = None
        self.bq_loader: Optional[BigQueryLoader] = None
        self.execution_modes: Dict[str, Any] = {}
    
    def _initialize_clients(self):
        """Initialize API clients and execution modes."""
        logger.info("Retrieving API token from GCP Secret Manager")
        api_token = get_secret(self.project_id, self.secret_name, self.secret_version)
        
        logger.info("Initializing clients")
        self.invezgo_client = InvezgoClient(self.invezgo_base_url, api_token)
        self.bq_loader = BigQueryLoader(self.project_id)
        
        self.execution_modes = {
            'onetime': OnetimeMode(self.invezgo_client, self.bq_loader, self.project_id, self.bq_dataset_id),
            'batch': BatchMode(self.invezgo_client, self.bq_loader, self.project_id, self.bq_dataset_id),
            'streaming': StreamingMode(self.invezgo_client, self.bq_loader, self.project_id, self.bq_dataset_id)
        }
    
    def run_single_endpoint(self, endpoint_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute pipeline for a single endpoint."""
        endpoint_name = endpoint_config.get('name', 'unknown')
        execution_mode = endpoint_config.get('execution_mode', 'onetime')
        
        try:
            logger.info(f"Processing endpoint: {endpoint_name} (mode: {execution_mode})")
            
            mode_handler = self.execution_modes.get(execution_mode)
            
            if not mode_handler:
                logger.error(f"Unknown execution mode: {execution_mode}")
                return {
                    'endpoint': endpoint_name,
                    'status': 'failed',
                    'error': f'Unknown execution mode: {execution_mode}'
                }
            
            result = mode_handler.execute(endpoint_config)
            return result
            
        except Exception as e:
            logger.error(f"Failed to process endpoint {endpoint_name}: {e}")
            return {
                'endpoint': endpoint_name,
                'status': 'failed',
                'error': str(e)
            }
    
    def run(
        self,
        endpoints_config: List[Dict[str, Any]],
        mode_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute the data pipeline for multiple endpoints."""
        try:
            logger.info("Starting data pipeline...")
            
            self._initialize_clients()
            
            logger.info("Ensuring BigQuery dataset exists")
            self.bq_loader.create_dataset(self.bq_dataset_id, BQ_LOCATION)
            
            if mode_filter:
                endpoints_config = [
                    ep for ep in endpoints_config
                    if ep.get('execution_mode', 'onetime') == mode_filter
                ]
                logger.info(f"Filtered to {len(endpoints_config)} endpoint(s) with mode: {mode_filter}")
            
            logger.info(f"Processing {len(endpoints_config)} endpoint(s)")
            results = []
            
            for endpoint_config in endpoints_config:
                result = self.run_single_endpoint(endpoint_config)
                results.append(result)
            
            total_records = sum(
                r.get('records_loaded', 0) or r.get('total_records_loaded', 0)
                for r in results
            )
            failed_count = sum(1 for r in results if r.get('status') == 'failed')
            
            mode_counts = {}
            for r in results:
                mode = r.get('mode', 'unknown')
                mode_counts[mode] = mode_counts.get(mode, 0) + 1
            
            logger.info("Pipeline completed!")
            
            return {
                'status': 'completed',
                'endpoints_processed': len(results),
                'endpoints_failed': failed_count,
                'total_records_loaded': total_records,
                'mode_filter': mode_filter,
                'mode_counts': mode_counts,
                'results': results,
                'message': 'Pipeline execution completed'
            }
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'message': 'Pipeline execution failed'
            }
        finally:
            if self.invezgo_client:
                self.invezgo_client.close()

# =============================================================================
# CONFIGURATION HELPERS
# =============================================================================

def load_endpoints_config() -> List[Dict[str, Any]]:
    """Load endpoints configuration from YAML file."""
    try:
        with open(ENDPOINTS_CONFIG_FILE, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('endpoints', [])
    except FileNotFoundError:
        logger.warning(f"Endpoints config file not found: {ENDPOINTS_CONFIG_FILE}")
        return []
    except Exception as e:
        logger.error(f"Error loading endpoints config: {e}")
        return []


def get_enabled_endpoints() -> List[Dict[str, Any]]:
    """Get only enabled endpoints from configuration."""
    all_endpoints = load_endpoints_config()
    return [ep for ep in all_endpoints if ep.get('enabled', True)]

# =============================================================================
# CLOUD FUNCTION HTTP ENTRY POINT
# =============================================================================

@functions_framework.http
def invezgo_pipeline(request):
    """
    HTTP Cloud Function entry point for Invezgo data pipeline.
    
    Accepts JSON payload:
    {
        "mode": "onetime|batch|streaming|all",
        "date": "YYYY-MM-DD",
        "start_date": "YYYY-MM-DD",
        "end_date": "YYYY-MM-DD"
    }
    """
    try:
        request_json = request.get_json(silent=True) or {}
        
        mode = request_json.get('mode', 'all')
        date = request_json.get('date')
        start_date = request_json.get('start_date')
        end_date = request_json.get('end_date')
        
        valid_modes = ['onetime', 'batch', 'streaming', 'all']
        if mode not in valid_modes:
            return jsonify({
                'status': 'error',
                'message': f'Invalid mode. Must be one of: {", ".join(valid_modes)}'
            }), 400
        
        mode_filter = None if mode == 'all' else mode
        
        logger.info("="*60)
        logger.info("Cloud Function: Invezgo to BigQuery Data Pipeline")
        logger.info("="*60)
        logger.info(f"Mode: {mode}")
        
        jakarta_tz = pytz.timezone('Asia/Jakarta')
        today = datetime.now(jakarta_tz).strftime('%Y-%m-%d')
        
        if date:
            process_start_date = date
            process_end_date = date
            logger.info(f"Date mode: Processing single date {date}")
        elif start_date and end_date:
            process_start_date = start_date
            process_end_date = end_date
            logger.info(f"Date range mode: {start_date} to {end_date}")
        elif start_date or end_date:
            return jsonify({
                'status': 'error',
                'message': 'Both start_date and end_date must be provided for date range'
            }), 400
        else:
            process_start_date = today
            process_end_date = today
            logger.info(f"Auto-detected date (Jakarta timezone): {today}")
        
        endpoints = get_enabled_endpoints()
        
        if not endpoints:
            logger.warning("No enabled endpoints found in configuration")
            return jsonify({
                'status': 'error',
                'message': 'No enabled endpoints found in configuration'
            }), 400
        
        for endpoint in endpoints:
            batch_config = endpoint.get('batch_config', {})
            date_iteration = batch_config.get('date_iteration', {})
            
            if date_iteration.get('enabled'):
                date_iteration['start_date'] = process_start_date
                date_iteration['end_date'] = process_end_date
                logger.info(f"Overriding date range for {endpoint['name']}: {process_start_date} to {process_end_date}")
        
        logger.info(f"Found {len(endpoints)} enabled endpoint(s)")
        for ep in endpoints:
            ep_mode = ep.get('execution_mode', 'onetime')
            logger.info(f"  - [{ep_mode.upper()}] {ep['name']}")
        
        pipeline = DataPipeline(
            project_id=GCP_PROJECT_ID,
            secret_name=SECRET_NAME,
            invezgo_base_url=INVEZGO_API_BASE_URL,
            bq_dataset_id=BQ_DATASET_ID,
            secret_version=SECRET_VERSION
        )
        
        result = pipeline.run(endpoints_config=endpoints, mode_filter=mode_filter)
        
        logger.info("="*60)
        logger.info("Pipeline Execution Summary")
        logger.info("="*60)
        logger.info(f"Status: {result['status']}")
        logger.info(f"Endpoints Processed: {result.get('endpoints_processed', 0)}")
        logger.info(f"Endpoints Failed: {result.get('endpoints_failed', 0)}")
        logger.info(f"Total Records Loaded: {result.get('total_records_loaded', 0)}")
        logger.info("="*60)
        
        response = {
            'status': 'success' if result['status'] == 'completed' else 'error',
            'message': result.get('message', 'Pipeline execution completed'),
            'endpoints_processed': result.get('endpoints_processed', 0),
            'endpoints_failed': result.get('endpoints_failed', 0),
            'total_records_loaded': result.get('total_records_loaded', 0),
            'mode': mode,
            'date_range': {
                'start': process_start_date,
                'end': process_end_date
            },
            'results': result.get('results', [])
        }
        
        if result['status'] == 'failed':
            response['error'] = result.get('error', 'Unknown error')
            return jsonify(response), 500
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Cloud Function error: {e}", exc_info=True)
        
        return jsonify({
            'status': 'error',
            'message': str(e),
            'error': str(e)
        }), 500
