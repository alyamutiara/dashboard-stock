"""
Script to verify pipeline configuration and connectivity.
Checks GCP authentication, Secret Manager access, and BigQuery connectivity.
"""
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from google.cloud import secretmanager, bigquery
from config.settings import (
    GCP_PROJECT_ID,
    SECRET_NAME,
    BQ_DATASET_ID,
    INVEZGO_API_BASE_URL
)


def check_secret_manager() -> bool:
    """Check Secret Manager connectivity and secret existence."""
    try:
        print("🔐 Checking Secret Manager...")
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{GCP_PROJECT_ID}/secrets/{SECRET_NAME}"
        
        try:
            client.get_secret(request={"name": name})
            print(f"   ✓ Secret '{SECRET_NAME}' exists")
            return True
        except Exception as e:
            print(f"   ✗ Secret '{SECRET_NAME}' not found: {e}")
            return False
            
    except Exception as e:
        print(f"   ✗ Secret Manager error: {e}")
        return False


def check_bigquery() -> bool:
    """Check BigQuery connectivity and dataset existence."""
    try:
        print("\n📊 Checking BigQuery...")
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        # Check dataset
        try:
            dataset_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}"
            client.get_dataset(dataset_ref)
            print(f"   ✓ Dataset '{BQ_DATASET_ID}' exists")
        except Exception:
            print(f"   ⚠ Dataset '{BQ_DATASET_ID}' not found (will be created on first run)")
        
        # Note: Tables are created dynamically based on endpoints
        print(f"   ℹ Tables will be created automatically based on endpoints.yaml")
        
        return True
        
    except Exception as e:
        print(f"   ✗ BigQuery error: {e}")
        return False


def check_config() -> bool:
    """Check configuration settings."""
    print("\n⚙️  Checking Configuration...")
    print(f"   Project ID:    {GCP_PROJECT_ID}")
    print(f"   Secret Name:   {SECRET_NAME}")
    print(f"   Dataset ID:    {BQ_DATASET_ID}")
    print(f"   API Base URL:  {INVEZGO_API_BASE_URL}")
    return True


def main():
    """Main execution function."""
    print("\n" + "="*60)
    print("  Pipeline Configuration Verification")
    print("="*60 + "\n")
    
    checks = [
        check_config(),
        check_secret_manager(),
        check_bigquery()
    ]
    
    print("\n" + "="*60)
    if all(checks):
        print("✅ All checks passed! Pipeline is ready to run.")
    else:
        print("⚠️  Some checks failed. Please review the errors above.")
        print("\nNext steps:")
        print("  1. Run: python scripts/setup_secret.py --token YOUR_TOKEN")
        print("  2. Run: python scripts/setup_bigquery.py")
    print("="*60 + "\n")
    
    sys.exit(0 if all(checks) else 1)


if __name__ == "__main__":
    main()
