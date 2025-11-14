"""
Configuration module for loading environment variables.
This module uses python-dotenv to load variables from a .env file.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Get the project root directory (one level up from protocolo folder)
PROJECT_ROOT = Path(__file__).parent.parent

# Load environment variables from .env file
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path)

# USGS API Credentials
USGS_USERNAME = os.getenv('USGS_USERNAME')
USGS_PASSWORD = os.getenv('USGS_PASSWORD')

# PostgreSQL Database Configuration
DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# SSH Configuration
SSH_USER = os.getenv('SSH_USER')
SSH_KEY_PATH = os.getenv('SSH_KEY_PATH')

# Server Configuration
SERVER_HOSTS = {
    os.getenv('SERVER_HOST_1', '10.17.14.84'): os.getenv('SERVER_PATH', '/srv/productos_recibidos_last'),
    os.getenv('SERVER_HOST_2', '5.134.118.83'): os.getenv('SERVER_PATH', '/srv/productos_recibidos_last')
}

# Email Recipients
EMAIL_RECIPIENTS = os.getenv('EMAIL_RECIPIENTS', '').split(',') if os.getenv('EMAIL_RECIPIENTS') else []

# GeoNetwork Configuration
GEONETWORK_USERNAME = os.getenv('GEONETWORK_USERNAME')
GEONETWORK_PASSWORD = os.getenv('GEONETWORK_PASSWORD')
GEONETWORK_SERVER = os.getenv('GEONETWORK_SERVER', 'https://goyas.csic.es/geonetwork')

# Validation: Check if critical variables are loaded
def validate_config():
    """Validate that critical environment variables are loaded."""
    critical_vars = {
        'USGS_USERNAME': USGS_USERNAME,
        'USGS_PASSWORD': USGS_PASSWORD,
    }

    missing_vars = [var for var, value in critical_vars.items() if not value]

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}. "
            f"Please check your .env file at {env_path}"
        )
