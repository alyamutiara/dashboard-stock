"""
Script to create BigQuery dataset and optionally a table with custom schema.
"""
import sys
import argparse
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from google.cloud import bigquery
from config.settings import GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID, BQ_LOCATION


def create_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    """
    Create BigQuery dataset.
    
    Args:
        client: BigQuery client
        dataset_id: Dataset ID to create
        location: Dataset location
    """
    dataset_ref = f"{client.project}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"✓ Dataset {dataset_id} is ready in {location}")


def create_table(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    schema: list = None
) -> None:
    """
    Create BigQuery table with optional schema.
    
    Args:
        client: BigQuery client
        dataset_id: Dataset ID
        table_id: Table ID to create
        schema: Optional list of SchemaField objects
    """
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)
    
    table = client.create_table(table, exists_ok=True)
    print(f"✓ Table {table_id} created in {dataset_id}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Setup BigQuery dataset and table"
    )
    parser.add_argument(
        "--project-id",
        default=GCP_PROJECT_ID,
        help=f"GCP project ID (default: {GCP_PROJECT_ID})"
    )
    parser.add_argument(
        "--dataset-id",
        default=BQ_DATASET_ID,
        help=f"Dataset ID (default: {BQ_DATASET_ID})"
    )
    parser.add_argument(
        "--table-id",
        default=BQ_TABLE_ID,
        help=f"Table ID (default: {BQ_TABLE_ID})"
    )
    parser.add_argument(
        "--location",
        default=BQ_LOCATION,
        help=f"Dataset location (default: {BQ_LOCATION})"
    )
    parser.add_argument(
        "--create-table",
        action="store_true",
        help="Create table with auto-detected schema (optional)"
    )
    
    args = parser.parse_args()
    
    print(f"\n📊 Setting up BigQuery")
    print(f"   Project:  {args.project_id}")
    print(f"   Dataset:  {args.dataset_id}")
    print(f"   Location: {args.location}\n")
    
    try:
        client = bigquery.Client(project=args.project_id)
        
        # Create dataset
        create_dataset(client, args.dataset_id, args.location)
        
        # Optionally create table
        if args.create_table:
            print(f"   Table:    {args.table_id}\n")
            create_table(client, args.dataset_id, args.table_id)
        else:
            print(f"\nℹ Table will be created automatically on first pipeline run\n")
        
        print(f"✅ Setup completed successfully!\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
