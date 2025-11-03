"""Configuration settings for the data pipeline."""
import os
import logging
import yaml
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# GCP Configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'emerald-skill-352503')
SECRET_NAME = os.getenv('SECRET_NAME', 'invezgo-api-token')
SECRET_VERSION = os.getenv('SECRET_VERSION', 'latest')

# Invezgo API Configuration
INVEZGO_API_BASE_URL = os.getenv('INVEZGO_API_BASE_URL', 'https://api.invezgo.com/')

# BigQuery Configuration
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID', 'invezgo_data')
BQ_LOCATION = os.getenv('BQ_LOCATION', 'asia-southeast1')

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DIR = PROJECT_ROOT / 'logs'

# Log files for different execution modes
LOG_FILE_MAIN = LOG_DIR / 'pipeline.log'  # Main/combined log
LOG_FILE_ONETIME = LOG_DIR / 'onetime.log'  # Onetime mode only
LOG_FILE_BATCH = LOG_DIR / 'batch.log'  # Batch mode only
LOG_FILE_STREAMING = LOG_DIR / 'streaming.log'  # Streaming mode only

# Ensure log directory exists
LOG_DIR.mkdir(exist_ok=True)

# Endpoints Configuration
ENDPOINTS_CONFIG_FILE = Path(__file__).parent / 'endpoints.yaml'


def load_endpoints_config() -> List[Dict[str, Any]]:
    """
    Load endpoints configuration from YAML file.
    
    Returns:
        List of endpoint configurations
    """
    try:
        with open(ENDPOINTS_CONFIG_FILE, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('endpoints', [])
    except FileNotFoundError:
        logging.warning(f"Endpoints config file not found: {ENDPOINTS_CONFIG_FILE}")
        return []
    except Exception as e:
        logging.error(f"Error loading endpoints config: {e}")
        return []


def get_enabled_endpoints() -> List[Dict[str, Any]]:
    """
    Get only enabled endpoints from configuration.
    
    Returns:
        List of enabled endpoint configurations
    """
    all_endpoints = load_endpoints_config()
    return [ep for ep in all_endpoints if ep.get('enabled', True)]


def setup_logging(mode_filter=None):
    """
    Configure logging for the application.
    Creates separate log files for each execution mode.
    
    Args:
        mode_filter: Optional execution mode to determine which log file to emphasize
    """
    # Remove any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create handlers
    handlers = []
    
    # Console handler (always enabled)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, LOG_LEVEL))
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handlers.append(console_handler)
    
    # Main log file (all logs)
    main_handler = logging.FileHandler(LOG_FILE_MAIN, encoding='utf-8')
    main_handler.setLevel(getattr(logging, LOG_LEVEL))
    main_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handlers.append(main_handler)
    
    # Mode-specific log files
    if mode_filter == 'onetime' or mode_filter is None:
        onetime_handler = logging.FileHandler(LOG_FILE_ONETIME, encoding='utf-8')
        onetime_handler.setLevel(getattr(logging, LOG_LEVEL))
        onetime_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        onetime_handler.addFilter(ModeLogFilter('onetime'))
        handlers.append(onetime_handler)
    
    if mode_filter == 'batch' or mode_filter is None:
        batch_handler = logging.FileHandler(LOG_FILE_BATCH, encoding='utf-8')
        batch_handler.setLevel(getattr(logging, LOG_LEVEL))
        batch_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        batch_handler.addFilter(ModeLogFilter('batch'))
        handlers.append(batch_handler)
    
    if mode_filter == 'streaming' or mode_filter is None:
        streaming_handler = logging.FileHandler(LOG_FILE_STREAMING, encoding='utf-8')
        streaming_handler.setLevel(getattr(logging, LOG_LEVEL))
        streaming_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        streaming_handler.addFilter(ModeLogFilter('streaming'))
        handlers.append(streaming_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
        handlers=handlers
    )


class ModeLogFilter(logging.Filter):
    """Filter logs based on execution mode markers in the message."""
    
    def __init__(self, mode):
        super().__init__()
        self.mode = mode.upper()
    
    def filter(self, record):
        """
        Return True if the log record should be included in this mode's log file.
        Includes logs with [MODE] markers or general logs without mode markers.
        """
        message = record.getMessage()
        
        # Include logs with matching mode marker [ONETIME], [BATCH], [STREAMING]
        if f'[{self.mode}]' in message:
            return True
        
        # Exclude logs with different mode markers
        if '[ONETIME]' in message or '[BATCH]' in message or '[STREAMING]' in message:
            return False
        
        # Include general logs (no mode marker)
        return True
