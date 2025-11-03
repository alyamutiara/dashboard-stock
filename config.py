"""Configuration settings for the data pipeline."""
import os
from dotenv import load_dotenv

load_dotenv()

# GCP Configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'emerald-skill-352503')
SECRET_NAME = os.getenv('SECRET_NAME', 'invezgo-api-token')
SECRET_VERSION = os.getenv('SECRET_VERSION', 'latest')

# Invezgo API Configuration
INVEZGO_API_BASE_URL = os.getenv('INVEZGO_API_BASE_URL', 'https://api.invezgo.com/')
INVEZGO_API_ENDPOINT = os.getenv('INVEZGO_API_ENDPOINT', '/accounts')  # Change based on your needs

# BigQuery Configuration
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID', 'invezgo_data')
BQ_TABLE_ID = os.getenv('BQ_TABLE_ID', 'accounts')
