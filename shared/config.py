import os
import logging
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
import pathlib

# Get project root (2 levels up from shared/config.py)
PROJECT_ROOT = pathlib.Path(__file__).parent.parent

env = os.getenv("ENV", "prod").lower()
env_file = PROJECT_ROOT / "infra" / ".env" 

# Only load from file if it exists (local development)
# In Docker, env vars are already set by docker-compose
if env_file.exists():
    load_dotenv(dotenv_path=env_file, override=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Settings(BaseSettings):
    """Single settings class - reads from unsuffixed env vars"""
    ENV: str = "prod"
    
    # Database
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str
    
    # Odoo (optional)
    ODOO_URL: str = ""
    ODOO_DB: str = ""
    ODOO_USER: str = ""
    ODOO_PASSWORD: str = ""
    
    # Shopify
    SHOP_URL: str
    SHOP_TOKEN: str
    SHOP_KEY: str
    SHOP_SECRET_KEY: str
    
    # Klaviyo
    KLAVIYO_API_KEY: str
    
    # Defaults
    DEFAULT_DATE: str
    LOG_LEVEL: str = "INFO"
    
    # S3/MinIO
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    S3_ENDPOINT: str = ""
    S3_BUCKET: str = ""
    AWS_REGION: str = "us-east-1"
    
    model_config = SettingsConfigDict(env_file=None, case_sensitive=True, extra="ignore")


settings = Settings()

logger.debug(f"Loaded from {'file' if env_file.exists() else 'env vars'} | ENV={settings.ENV} | DB={settings.DB_NAME}")