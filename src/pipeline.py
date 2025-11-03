"""Main pipeline orchestration module."""
import logging
from typing import Optional, Dict, Any, List

from src.gcp_secret_manager import get_secret
from src.invezgo_client import InvezgoClient
from src.bigquery_loader import BigQueryLoader
from src.execution_modes import OnetimeMode, BatchMode, StreamingMode

logger = logging.getLogger(__name__)


class DataPipeline:
    """Data pipeline orchestrator for multiple endpoints with different execution modes."""
    
    def __init__(
        self,
        project_id: str,
        secret_name: str,
        invezgo_base_url: str,
        bq_dataset_id: str,
        secret_version: str = 'latest'
    ):
        """
        Initialize data pipeline.
        
        Args:
            project_id: GCP project ID
            secret_name: Name of secret in Secret Manager
            invezgo_base_url: Invezgo API base URL
            bq_dataset_id: BigQuery dataset ID
            secret_version: Secret version (default: 'latest')
        """
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
        # Get API token
        logger.info("Retrieving API token from GCP Secret Manager")
        api_token = get_secret(
            self.project_id,
            self.secret_name,
            self.secret_version
        )
        
        # Initialize clients
        logger.info("Initializing clients")
        self.invezgo_client = InvezgoClient(self.invezgo_base_url, api_token)
        self.bq_loader = BigQueryLoader(self.project_id)
        
        # Initialize execution modes
        self.execution_modes = {
            'onetime': OnetimeMode(
                self.invezgo_client,
                self.bq_loader,
                self.project_id,
                self.bq_dataset_id
            ),
            'batch': BatchMode(
                self.invezgo_client,
                self.bq_loader,
                self.project_id,
                self.bq_dataset_id
            ),
            'streaming': StreamingMode(
                self.invezgo_client,
                self.bq_loader,
                self.project_id,
                self.bq_dataset_id
            )
        }
    
    def run_single_endpoint(
        self,
        endpoint_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute pipeline for a single endpoint based on its execution mode.
        
        Args:
            endpoint_config: Endpoint configuration dictionary
        
        Returns:
            Dictionary with execution results
        """
        endpoint_name = endpoint_config.get('name', 'unknown')
        execution_mode = endpoint_config.get('execution_mode', 'onetime')
        
        try:
            logger.info(f"Processing endpoint: {endpoint_name} (mode: {execution_mode})")
            
            # Get appropriate execution mode handler
            mode_handler = self.execution_modes.get(execution_mode)
            
            if not mode_handler:
                logger.error(f"Unknown execution mode: {execution_mode}")
                return {
                    'endpoint': endpoint_name,
                    'status': 'failed',
                    'error': f'Unknown execution mode: {execution_mode}'
                }
            
            # Execute with the appropriate mode
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
        """
        Execute the data pipeline for multiple endpoints.
        
        Args:
            endpoints_config: List of endpoint configurations
            mode_filter: Optional filter to run only specific mode ('onetime', 'batch', 'streaming')
            
        Returns:
            Dictionary with pipeline execution results
        """
        try:
            logger.info("Starting data pipeline...")
            
            # Initialize clients
            self._initialize_clients()
            
            # Ensure dataset exists
            logger.info("Ensuring BigQuery dataset exists")
            self.bq_loader.create_dataset(self.bq_dataset_id)
            
            # Filter endpoints by mode if specified
            if mode_filter:
                endpoints_config = [
                    ep for ep in endpoints_config
                    if ep.get('execution_mode', 'onetime') == mode_filter
                ]
                logger.info(f"Filtered to {len(endpoints_config)} endpoint(s) with mode: {mode_filter}")
            
            # Process each endpoint
            logger.info(f"Processing {len(endpoints_config)} endpoint(s)")
            results = []
            
            for endpoint_config in endpoints_config:
                result = self.run_single_endpoint(endpoint_config)
                results.append(result)
            
            # Calculate summary
            total_records = sum(
                r.get('records_loaded', 0) or r.get('total_records_loaded', 0)
                for r in results
            )
            failed_count = sum(1 for r in results if r.get('status') == 'failed')
            
            # Count by mode
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
            # Cleanup
            if self.invezgo_client:
                self.invezgo_client.close()
