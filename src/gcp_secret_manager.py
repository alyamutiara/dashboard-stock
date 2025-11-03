"""Module for interacting with GCP Secret Manager."""
import logging
from google.cloud import secretmanager

logger = logging.getLogger(__name__)


def get_secret(project_id: str, secret_name: str, version: str = 'latest') -> str:
    """
    Retrieve secret from GCP Secret Manager.
    
    Args:
        project_id: GCP project ID
        secret_name: Name of the secret
        version: Version of the secret (default: 'latest')
    
    Returns:
        Secret value as string
        
    Raises:
        Exception: If secret retrieval fails
    """
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
