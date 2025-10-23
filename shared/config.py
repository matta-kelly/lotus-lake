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


class TestSettings(BaseSettings):
    ENV: str = "test"
    
    DB_HOST_TEST: str
    DB_PORT_TEST: int
    DB_USER_TEST: str
    DB_PASSWORD_TEST: str
    DB_NAME_TEST: str
    
    ODOO_URL_TEST: str
    ODOO_DB_TEST: str
    ODOO_USER_TEST: str
    ODOO_PASSWORD_TEST: str
    
    SHOP_URL_TEST: str
    SHOP_TOKEN_TEST: str
    SHOP_KEY_TEST: str
    SHOP_SECRET_KEY_TEST: str
    
    KLAVIYO_API_KEY: str
    DEFAULT_DATE_TEST: str
    LOG_LEVEL: str = "DEBUG"
    
    model_config = SettingsConfigDict(env_file=None, case_sensitive=True, extra="ignore")
    
    # Aliases for clean access
    @property
    def DB_HOST(self): return self.DB_HOST_TEST
    @property
    def DB_PORT(self): return self.DB_PORT_TEST
    @property
    def DB_USER(self): return self.DB_USER_TEST
    @property
    def DB_PASSWORD(self): return self.DB_PASSWORD_TEST
    @property
    def DB_NAME(self): return self.DB_NAME_TEST
    @property
    def ODOO_URL(self): return self.ODOO_URL_TEST
    @property
    def ODOO_DB(self): return self.ODOO_DB_TEST
    @property
    def ODOO_USER(self): return self.ODOO_USER_TEST
    @property
    def ODOO_PASSWORD(self): return self.ODOO_PASSWORD_TEST
    @property
    def SHOP_URL(self): return self.SHOP_URL_TEST
    @property
    def SHOP_TOKEN(self): return self.SHOP_TOKEN_TEST
    @property
    def SHOP_KEY(self): return self.SHOP_KEY_TEST
    @property
    def SHOP_SECRET_KEY(self): return self.SHOP_SECRET_KEY_TEST
    @property
    def DEFAULT_DATE(self): return self.DEFAULT_DATE_TEST


class ProdSettings(BaseSettings):
    ENV: str = "production"
    
    DB_HOST_PROD: str
    DB_PORT_PROD: int
    DB_USER_PROD: str
    DB_PASSWORD_PROD: str
    DB_NAME_PROD: str
    
    ODOO_URL_PROD: str = ""
    ODOO_DB_PROD: str = ""
    ODOO_USER_PROD: str = ""
    ODOO_PASSWORD_PROD: str = ""
    
    SHOP_URL_PROD: str
    SHOP_TOKEN_PROD: str
    SHOP_KEY_PROD: str
    SHOP_SECRET_KEY_PROD: str
    
    KLAVIYO_API_KEY: str
    DEFAULT_DATE_PROD: str
    LOG_LEVEL: str = "INFO"
    
    # S3 for production
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    S3_ENDPOINT: str = ""
    S3_BUCKET: str = ""
    AWS_REGION: str = "us-east-1"
    
    model_config = SettingsConfigDict(env_file=None, case_sensitive=True, extra="ignore")
    
    # Aliases for clean access
    @property
    def DB_HOST(self): return self.DB_HOST_PROD
    @property
    def DB_PORT(self): return self.DB_PORT_PROD
    @property
    def DB_USER(self): return self.DB_USER_PROD
    @property
    def DB_PASSWORD(self): return self.DB_PASSWORD_PROD
    @property
    def DB_NAME(self): return self.DB_NAME_PROD
    @property
    def ODOO_URL(self): return self.ODOO_URL_PROD
    @property
    def ODOO_DB(self): return self.ODOO_DB_PROD
    @property
    def ODOO_USER(self): return self.ODOO_USER_PROD
    @property
    def ODOO_PASSWORD(self): return self.ODOO_PASSWORD_PROD
    @property
    def SHOP_URL(self): return self.SHOP_URL_PROD
    @property
    def SHOP_TOKEN(self): return self.SHOP_TOKEN_PROD
    @property
    def SHOP_KEY(self): return self.SHOP_KEY_PROD
    @property
    def SHOP_SECRET_KEY(self): return self.SHOP_SECRET_KEY_PROD
    @property
    def DEFAULT_DATE(self): return self.DEFAULT_DATE_PROD


# Load the appropriate settings class
settings = TestSettings() if env == "test" else ProdSettings()

logger.debug(f"Loaded from {'file' if env_file.exists() else 'env vars'} | ENV={settings.ENV} | DB={settings.DB_NAME}")