"""
Connection tests for data infrastructure components.
Run before docker-compose to verify config, or against running services.
"""
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
import psycopg2
from dotenv import load_dotenv


def test_env_loaded():
    """Test that environment variables are loaded correctly."""
    print("\n" + "="*60)
    print("TEST: Environment Configuration")
    print("="*60)
    
    # Load environment
    env_file = Path(__file__).parent.parent / ".env.prod"
    if not env_file.exists():
        print(f"FAIL: .env.prod not found at {env_file}")
        return False
    
    load_dotenv(env_file)
    
    required_vars = [
        'ENV',
        'DB_HOST_PROD',
        'DB_PORT_PROD', 
        'DB_USER_PROD',
        'DB_NAME_PROD',
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'S3_ENDPOINT_URL',
        'S3_BUCKET',
        'SHOP_URL_PROD',
        'SHOP_TOKEN_PROD'
    ]
    
    missing = []
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing.append(var)
            print(f"[MISSING] {var}")
        else:
            # Mask sensitive values
            if any(x in var.lower() for x in ['password', 'secret', 'token', 'key']):
                display = value[:8] + "..." if len(value) > 8 else "***"
            else:
                display = value
            print(f"[OK] {var}: {display}")
    
    if missing:
        print(f"\nFAIL: Missing {len(missing)} required variables")
        return False
    
    print(f"\nPASS: All environment variables loaded")
    return True


def test_s3_connection():
    """Test connection to MinIO/S3."""
    print("\n" + "="*60)
    print("TEST: S3/MinIO Connection")
    print("="*60)
    
    try:
        endpoint = os.getenv('S3_ENDPOINT_URL')
        bucket = os.getenv('S3_BUCKET')
        
        print(f"Endpoint: {endpoint}")
        print(f"Bucket: {bucket}")
        
        # Create S3 client
        client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        # Try to list buckets
        response = client.list_buckets()
        buckets = [b['Name'] for b in response.get('Buckets', [])]
        
        print(f"\nAvailable buckets: {buckets}")
        
        if bucket in buckets:
            print(f"[OK] Target bucket '{bucket}' exists")
        else:
            print(f"[WARN] Target bucket '{bucket}' not found (will be created by minio-init)")
        
        print(f"\nPASS: S3 connection successful")
        return True
        
    except Exception as e:
        print(f"\nFAIL: S3 connection failed")
        print(f"Error: {e}")
        print("\nNote: This is expected if docker-compose is not running.")
        return False


def test_postgres_connection():
    """Test connection to PostgreSQL."""
    print("\n" + "="*60)
    print("TEST: PostgreSQL Connection")
    print("="*60)
    
    try:
        # For docker, use service name; for local, use localhost
        host = os.getenv('DB_HOST_PROD', 'localhost')
        port = os.getenv('DB_PORT_PROD', '5432')
        user = os.getenv('DB_USER_PROD')
        password = os.getenv('DB_PASSWORD_PROD')
        database = os.getenv('DB_NAME_PROD')
        
        print(f"Host: {host}")
        print(f"Port: {port}")
        print(f"Database: {database}")
        print(f"User: {user}")
        
        # Try connection with and without SSL
        for sslmode in ['require', 'prefer', 'disable']:
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=database,
                    sslmode=sslmode,
                    connect_timeout=5
                )
                
                # Test query
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()[0]
                    print(f"\nPostgreSQL version: {version[:50]}...")
                
                conn.close()
                print(f"[OK] Connected with sslmode={sslmode}")
                print(f"\nPASS: PostgreSQL connection successful")
                return True
                
            except psycopg2.OperationalError as e:
                if sslmode == 'disable':
                    raise
                print(f"[INFO] sslmode={sslmode} failed, trying next...")
                continue
        
    except Exception as e:
        print(f"\nFAIL: PostgreSQL connection failed")
        print(f"Error: {e}")
        print("\nNote: This is expected if docker-compose is not running.")
        return False


def test_shared_config():
    """Test that shared config module loads correctly."""
    print("\n" + "="*60)
    print("TEST: Shared Config Module")
    print("="*60)
    
    try:
        from shared.config import settings
        
        print(f"ENV: {settings.ENV}")
        print(f"DB_HOST: {settings.DB_HOST}")
        print(f"DB_NAME: {settings.DB_NAME}")
        print(f"SHOP_URL: {settings.SHOP_URL}")
        print(f"S3_BUCKET: {settings.S3_BUCKET if hasattr(settings, 'S3_BUCKET') else 'Not in test mode'}")
        
        print(f"\nPASS: Config module loaded successfully")
        return True
        
    except Exception as e:
        print(f"\nFAIL: Config module failed to load")
        print(f"Error: {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("DATA INFRASTRUCTURE CONNECTION TESTS")
    print("="*60)
    
    results = []
    
    # Test 1: Environment variables
    results.append(('Environment Config', test_env_loaded()))
    
    # Test 2: Shared config module
    results.append(('Shared Config', test_shared_config()))
    
    # Test 3: PostgreSQL (may fail if not running)
    results.append(('PostgreSQL', test_postgres_connection()))
    
    # Test 4: S3/MinIO (may fail if not running)
    results.append(('S3/MinIO', test_s3_connection()))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"{name:.<40} {status}")
    
    passed_count = sum(1 for _, p in results if p)
    total_count = len(results)
    
    print(f"\nTotal: {passed_count}/{total_count} tests passed")
    
    if passed_count == total_count:
        print("\nAll tests passed!")
        return 0
    else:
        print(f"\n{total_count - passed_count} test(s) failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())