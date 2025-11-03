"""Module for interacting with Invezgo API."""
import json
import logging
from typing import Dict, List, Any
import requests

logger = logging.getLogger(__name__)


class InvezgoClient:
    """Client for interacting with Invezgo API."""
    
    def __init__(self, base_url: str, api_token: str):
        """
        Initialize Invezgo API client.
        
        Args:
            base_url: Base URL for Invezgo API
            api_token: Bearer token for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.api_token = api_token
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        })
    
    def fetch_data(self, endpoint: str, params: Dict = None, path_variables: Dict = None) -> List[Dict[str, Any]]:
        """
        Fetch data from Invezgo API endpoint.
        
        Args:
            endpoint: API endpoint (e.g., '/accounts', '/analysis/summary/stock/{stock_code}')
            params: Optional query parameters
            path_variables: Optional path variables to replace in endpoint (e.g., {'stock_code': 'BBCA'})
        
        Returns:
            List of dictionaries containing API response data
            
        Raises:
            requests.exceptions.RequestException: If API request fails
            json.JSONDecodeError: If response parsing fails
        """
        try:
            # Replace path variables if provided
            if path_variables:
                for key, value in path_variables.items():
                    endpoint = endpoint.replace(f'{{{key}}}', str(value))
            
            url = f"{self.base_url}{endpoint}"
            logger.info(f"Fetching data from Invezgo API: {url}")
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched data from Invezgo API")
            
            # Handle different response formats
            return self._normalize_response(data)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from Invezgo API: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")
            raise
    
    def _normalize_response(self, data: Any) -> List[Dict[str, Any]]:
        """
        Normalize API response to list of dictionaries.
        
        Args:
            data: Raw API response
            
        Returns:
            Normalized list of dictionaries
        """
        if isinstance(data, dict):
            # If response has a 'data' key, extract it
            if 'data' in data:
                return data['data'] if isinstance(data['data'], list) else [data['data']]
            # Otherwise return the dict as a single-item list
            return [data]
        elif isinstance(data, list):
            return data
        else:
            logger.warning(f"Unexpected response format: {type(data)}")
            return [{'raw_data': str(data)}]
    
    def close(self):
        """Close the session."""
        self.session.close()
