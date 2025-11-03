"""
Setup script to initialize GCP Secret Manager with Invezgo API token.
Run this script to store your Invezgo API token in GCP Secret Manager.
"""
import sys
import argparse
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from google.cloud import secretmanager
from config.settings import GCP_PROJECT_ID, SECRET_NAME


def create_secret(project_id: str, secret_id: str) -> None:
    """
    Create a new secret in Secret Manager.
    
    Args:
        project_id: GCP project ID
        secret_id: Secret ID to create
    """
    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_id}"
    
    try:
        secret = client.create_secret(
            request={
                "parent": parent,
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        )
        print(f"✓ Created secret: {secret.name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"ℹ Secret already exists: {secret_id}")
        else:
            raise


def add_secret_version(project_id: str, secret_id: str, payload: str) -> None:
    """
    Add a new version to an existing secret.
    
    Args:
        project_id: GCP project ID
        secret_id: Secret ID
        payload: Secret value to store
    """
    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_id}/secrets/{secret_id}"
    
    response = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": payload.encode("UTF-8")},
        }
    )
    print(f"✓ Added secret version: {response.name}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Setup Invezgo API token in GCP Secret Manager"
    )
    parser.add_argument(
        "--token",
        required=True,
        help="Invezgo API token to store"
    )
    parser.add_argument(
        "--project-id",
        default=GCP_PROJECT_ID,
        help=f"GCP project ID (default: {GCP_PROJECT_ID})"
    )
    parser.add_argument(
        "--secret-name",
        default=SECRET_NAME,
        help=f"Secret name (default: {SECRET_NAME})"
    )
    
    args = parser.parse_args()
    
    print(f"\n📦 Setting up GCP Secret Manager")
    print(f"   Project: {args.project_id}")
    print(f"   Secret:  {args.secret_name}\n")
    
    try:
        # Create secret if it doesn't exist
        create_secret(args.project_id, args.secret_name)
        
        # Add secret version
        add_secret_version(args.project_id, args.secret_name, args.token)
        
        print(f"\n✅ Setup completed successfully!\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
