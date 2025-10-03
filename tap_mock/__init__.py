#!/usr/bin/env python3
"""
Mock Singer Tap for Testing

This tap generates deterministic mock data for customers and opportunities.
Supports both OAuth and API key authentication.
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

import singer
from singer import Transformer, metrics, get_bookmark, write_bookmark
from singer.utils import strptime_to_utc

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["auth_type"]

JOB_ID = os.getenv("JOB_ID")
LOCAL_ROOT = f"/home/hotglue/{JOB_ID}"
SYNC_OUTPUT_PATH = f"{LOCAL_ROOT}/sync-output"

class MockTap:
    """Mock Singer Tap for testing purposes."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.auth_type = config.get("auth_type")
        
        # Store config file path for token updates
        self.config_file_path = config.get("config_file_path", "config.json")
        
        # Copy JSON files to sync output path
        self.copy_json_files_to_sync_output()
        
        # Validate configuration
        self._validate_config()
        
        # Initialize authentication
        self._authenticate()
    
    def copy_json_files_to_sync_output(self):
        """Copy all JSON files from current working directory to SYNC_OUTPUT_PATH."""
        import shutil
        
        current_dir = os.getcwd()
        
        try:
            # Get all items in the directory
            items = os.listdir(current_dir)
            
            # Find JSON files
            json_files = []
            for item in items:
                if item.endswith('.json') and os.path.isfile(os.path.join(current_dir, item)):
                    json_files.append(item)
            
            if json_files:
                for json_file in sorted(json_files):
                    source_path = os.path.join(current_dir, json_file)
                    dest_path = os.path.join(SYNC_OUTPUT_PATH, f".test.{json_file}")
                    
                    try:
                        shutil.copy2(source_path, dest_path)
                    except Exception as e:
                        pass  # Silently handle copy errors
                
        except (FileNotFoundError, PermissionError, Exception):
            pass  # Silently handle all errors
    
    def _validate_config(self):
        """Validate the configuration."""
        if self.auth_type not in ["oauth", "api_key"]:
            raise ValueError("auth_type must be either 'oauth' or 'api_key'")
        
        if self.auth_type == "oauth":
            required_oauth_keys = ["client_id", "client_secret"]
            for key in required_oauth_keys:
                if key not in self.config:
                    raise ValueError(f"OAuth config missing required key: {key}")
        
        if self.auth_type == "api_key":
            if "api_key" not in self.config:
                raise ValueError("API key config missing required key: 'api_key'")
        
        # Validate records_qty if provided
        if "records_qty" in self.config:
            records_qty = self.config["records_qty"]
            if not isinstance(records_qty, int) or records_qty < 0:
                raise ValueError("records_qty must be a non-negative integer")
        
        # Validate base_date if provided
        if "base_date" in self.config:
            base_date = self.config["base_date"]
            if not isinstance(base_date, str):
                raise ValueError("base_date must be a string in ISO format")
            try:
                # Try to parse the ISO format datetime
                datetime.fromisoformat(base_date.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError("base_date must be a valid datetime in ISO format")
    
    def _authenticate(self):
        """Simulate authentication based on auth_type."""
        if self.auth_type == "oauth":
            self._authenticate_oauth()
        else:
            self._authenticate_api_key()
    
    def _authenticate_oauth(self):
        """Simulate OAuth authentication."""
        LOGGER.info("Authenticating with OAuth...")
        
        # Simulate token refresh if enabled
        if self.config.get("rotate_refresh_token", False):
            LOGGER.info("Rotating refresh token...")
            
            # Check if next_refresh_token exists
            next_refresh_token = self.config.get("next_refresh_token")
            if not next_refresh_token:
                raise ValueError("rotate_refresh_token is true but next_refresh_token is not provided in config")
            
            # Store the original refresh token
            original_refresh_token = self.config.get("refresh_token")
            if original_refresh_token:
                self.config["original_refresh_token"] = original_refresh_token
                LOGGER.info("Stored original refresh token")
            
            # Update the config with next_refresh_token
            self.config["refresh_token"] = next_refresh_token
            
            # Update the config file
            self._update_config_file()
            
            LOGGER.info("Refresh token rotated successfully")
            LOGGER.info(f"New refresh token: {next_refresh_token}")
        
        LOGGER.info("OAuth authentication successful")
    
    def _update_config_file(self):
        """Update the config file with new refresh token."""
        # Read the current config file
        with open(self.config_file_path, 'r') as f:
            config_data = json.load(f)
        
        # Update the refresh token at root level
        config_data["refresh_token"] = self.config["refresh_token"]
        
        # Write the updated config back to file
        with open(self.config_file_path, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        LOGGER.info(f"Updated config file: {self.config_file_path}")
    
    def _authenticate_api_key(self):
        """Simulate API key authentication."""
        LOGGER.info("Authenticating with API key...")
        api_key = self.config.get("api_key")
        if api_key:
            LOGGER.info(f"Using API key: {api_key[:10]}...")
        else:
            LOGGER.info("No API key provided")
        LOGGER.info("API key authentication successful")
    
    def generate_customer_data(self, count: int, state: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Generate mock customer data."""
        customers = []
        
        # Get bookmark for incremental sync
        bookmark_value = None
        if state:
            bookmark_value = get_bookmark(state, "customers", "last_updated")
        
        # Use base_date from config if provided, otherwise use current time
        if "base_date" in self.config:
            base_date_str = self.config["base_date"]
            # Handle 'Z' suffix for UTC timezone
            if base_date_str.endswith('Z'):
                base_date_str = base_date_str.replace('Z', '+00:00')
            base_date = datetime.fromisoformat(base_date_str)
        else:
            base_date = datetime.now(timezone.utc)
        
        if bookmark_value:
            # Incremental sync: 5 updates + 5 new customers
            # Ensure bookmark_value is timezone-aware
            if not bookmark_value.endswith('Z') and not bookmark_value.endswith('+00:00'):
                bookmark_value = bookmark_value + '+00:00'
            bookmark_dt = strptime_to_utc(bookmark_value)
            
            # 5 existing customers with updated emails (updated after bookmark)
            for i in range(5):
                customer_id = f"CUST_{i+1:06d}"
                created_date = base_date - timedelta(days=365 - i)
                updated_date = base_date - timedelta(hours=5-i)  # Recent updates
                
                if updated_date > bookmark_dt:
                    customer = {
                        "id": customer_id,
                        "name": f"Customer {i+1}",
                        "email": f"updated.customer{i+1}@example.com",  # Updated email
                        "status": ["active", "inactive", "pending"][i % 3],
                        "created_at": created_date.isoformat(),
                        "updated_at": updated_date.isoformat(),
                        "metadata": {
                            "source": "mock_tap",
                            "generated_at": datetime.now(timezone.utc).isoformat()
                        }
                    }
                    customers.append(customer)
            
            # 5 new customers (created after bookmark)
            for i in range(5):
                customer_id = f"CUST_{100+i+1:06d}"  # New IDs starting from 101
                created_date = base_date - timedelta(hours=10-i)
                updated_date = created_date
                
                if created_date > bookmark_dt:
                    customer = {
                        "id": customer_id,
                        "name": f"New Customer {i+1}",
                        "email": f"newcustomer{i+1}@example.com",
                        "status": ["active", "inactive", "pending"][i % 3],
                        "created_at": created_date.isoformat(),
                        "updated_at": updated_date.isoformat(),
                        "metadata": {
                            "source": "mock_tap",
                            "generated_at": datetime.now(timezone.utc).isoformat()
                        }
                    }
                    customers.append(customer)
        else:
            # Full sync: generate all customers
            for i in range(count):
                customer_id = f"CUST_{i+1:06d}"
                created_date = base_date - timedelta(days=365 - (i % 365))
                updated_date = created_date + timedelta(days=i % 30)
                
                customer = {
                    "id": customer_id,
                    "name": f"Customer {i+1}",
                    "email": f"customer{i+1}@example.com",
                    "status": ["active", "inactive", "pending"][i % 3],
                    "created_at": created_date.isoformat(),
                    "updated_at": updated_date.isoformat(),
                    "metadata": {
                        "source": "mock_tap",
                        "generated_at": datetime.now(timezone.utc).isoformat()
                    }
                }
                customers.append(customer)
        
        return customers
    
    def generate_opportunity_data(self, count: int, state: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Generate mock opportunity data."""
        opportunities = []
        
        # Get bookmark for incremental sync
        bookmark_value = None
        if state:
            bookmark_value = get_bookmark(state, "opportunities", "last_updated")
        
        # Use base_date from config if provided, otherwise use current time
        if "base_date" in self.config:
            base_date_str = self.config["base_date"]
            # Handle 'Z' suffix for UTC timezone
            if base_date_str.endswith('Z'):
                base_date_str = base_date_str.replace('Z', '+00:00')
            base_date = datetime.fromisoformat(base_date_str)
        else:
            base_date = datetime.now(timezone.utc)
        
        if bookmark_value:
            # Incremental sync: 1 new opportunity
            # Ensure bookmark_value is timezone-aware
            if not bookmark_value.endswith('Z') and not bookmark_value.endswith('+00:00'):
                bookmark_value = bookmark_value + '+00:00'
            bookmark_dt = strptime_to_utc(bookmark_value)
            
            # 1 new opportunity (created after bookmark)
            opportunity_id = f"OPP_{100+1:06d}"  # New ID
            created_date = base_date - timedelta(hours=2)
            updated_date = created_date
            
            if created_date > bookmark_dt:
                opportunity = {
                    "id": opportunity_id,
                    "name": f"New Opportunity 1",
                    "customer_id": f"CUST_{1:06d}",
                    "amount": 5000,
                    "stage": "prospecting",
                    "probability": 25,
                    "created_at": created_date.isoformat(),
                    "updated_at": updated_date.isoformat(),
                    "metadata": {
                        "source": "mock_tap",
                        "generated_at": datetime.now(timezone.utc).isoformat()
                    }
                }
                opportunities.append(opportunity)
        else:
            # Full sync: generate all opportunities
            for i in range(count):
                opportunity_id = f"OPP_{i+1:06d}"
                created_date = base_date - timedelta(days=180 - (i % 180))
                updated_date = created_date + timedelta(days=i % 15)
                
                opportunity = {
                    "id": opportunity_id,
                    "name": f"Opportunity {i+1}",
                    "customer_id": f"CUST_{(i % 100) + 1:06d}",
                    "amount": 1000 + (i * 100),
                    "stage": ["prospecting", "qualification", "proposal", "negotiation", "closed"][i % 5],
                    "probability": (i % 100) + 1,
                    "created_at": created_date.isoformat(),
                    "updated_at": updated_date.isoformat(),
                    "metadata": {
                        "source": "mock_tap",
                        "generated_at": datetime.now(timezone.utc).isoformat()
                    }
                }
                opportunities.append(opportunity)
        
        return opportunities
    
    def sync_stream(self, stream_name: str, state: Optional[Dict] = None):
        """Sync a specific stream."""
        LOGGER.info(f"Syncing stream: {stream_name}")
        
        # Get records_qty from config, use defaults if not provided
        records_qty = self.config.get("records_qty")
        
        # Generate schema
        if stream_name == "customers":
            schema = {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "status": {"type": "string"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "updated_at": {"type": "string", "format": "date-time"},
                    "metadata": {"type": "object"}
                }
            }
            # Use records_qty if provided, otherwise default to 100
            count = records_qty if records_qty is not None else 100
            data = self.generate_customer_data(count, state)
        elif stream_name == "opportunities":
            schema = {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "customer_id": {"type": "string"},
                    "amount": {"type": "number"},
                    "stage": {"type": "string"},
                    "probability": {"type": "integer"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "updated_at": {"type": "string", "format": "date-time"},
                    "metadata": {"type": "object"}
                }
            }
            # Use records_qty if provided, otherwise default to 50
            count = records_qty if records_qty is not None else 50
            data = self.generate_opportunity_data(count, state)
        else:
            raise ValueError(f"Unknown stream: {stream_name}")
        
        # Write schema
        singer.write_schema(stream_name, schema, ["id"])
        
        # Write records
        for record in data:
            singer.write_record(stream_name, record)
        
        # Update bookmark
        if data:
            latest_updated = max(record["updated_at"] for record in data)
            # Ensure the bookmark value is timezone-aware
            if not latest_updated.endswith('Z') and not latest_updated.endswith('+00:00'):
                # Convert to timezone-aware format
                latest_updated = latest_updated + '+00:00'
            state = write_bookmark(state, stream_name, "last_updated", latest_updated)
            singer.write_state(state)
        
        LOGGER.info(f"Synced {len(data)} records from {stream_name}")
    
    def sync_all(self, state: Optional[Dict] = None):
        """Sync all streams."""
        streams = ["customers", "opportunities"]
        for stream in streams:
            self.sync_stream(stream, state)

def discover():
    """Discover available streams and their schemas."""
    streams = [
        {
            "stream": "customers",
            "tap_stream_id": "customers",
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "status": {"type": "string"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "updated_at": {"type": "string", "format": "date-time"},
                    "metadata": {"type": "object"}
                }
            },
            "metadata": [
                # Field-level metadata
                {
                    "breadcrumb": ["properties", "id"],
                    "metadata": {
                        "inclusion": "automatic"
                    }
                },
                {
                    "breadcrumb": ["properties", "name"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "email"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "status"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "created_at"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "updated_at"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "metadata"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                # Root-level metadata
                {
                    "breadcrumb": [],
                    "metadata": {
                        "inclusion": "available",
                        "selected": False,
                        "table-key-properties": ["id"],
                        "valid-replication-keys": ["updated_at"]
                    }
                }
            ]
        },
        {
            "stream": "opportunities",
            "tap_stream_id": "opportunities",
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "customer_id": {"type": "string"},
                    "amount": {"type": "number"},
                    "stage": {"type": "string"},
                    "probability": {"type": "integer"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "updated_at": {"type": "string", "format": "date-time"},
                    "metadata": {"type": "object"}
                }
            },
            "metadata": [
                # Field-level metadata
                {
                    "breadcrumb": ["properties", "id"],
                    "metadata": {
                        "inclusion": "automatic"
                    }
                },
                {
                    "breadcrumb": ["properties", "name"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "customer_id"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "amount"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "stage"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "probability"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "created_at"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "updated_at"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                {
                    "breadcrumb": ["properties", "metadata"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },
                # Root-level metadata
                {
                    "breadcrumb": [],
                    "metadata": {
                        "inclusion": "available",
                        "selected": False,
                        "table-key-properties": ["id"],
                        "valid-replication-keys": ["updated_at"]
                    }
                }
            ]
        }
    ]
    
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)

def main():
    """Main entry point for the tap."""
    import argparse
    
    # Create argument parser
    parser = argparse.ArgumentParser(description='Mock Singer Tap')
    parser.add_argument('--config', required=True, help='Config file path')
    parser.add_argument('--catalog', help='Catalog file path')
    parser.add_argument('--state', help='State file path')
    parser.add_argument('--discover', action='store_true', help='Run in discover mode')
    
    args = parser.parse_args()
    
    # Load config
    with open(args.config) as f:
        config = json.load(f)
    
    # Validate required config keys
    for key in REQUIRED_CONFIG_KEYS:
        if key not in config:
            raise ValueError(f"Config missing required key: {key}")
    
    # Handle catalog file
    catalog = args.catalog
    
    # Load catalog if provided
    if catalog:
        with open(catalog) as f:
            catalog = json.load(f)
    
    # Load state if provided
    state = {}
    if args.state:
        try:
            with open(args.state) as f:
                state = json.load(f)
        except FileNotFoundError:
            # State file doesn't exist, start with empty state
            state = {}
    
    if args.discover:
        discover()
    else:
        # Initialize tap
        tap = MockTap(config)
        
        # Sync streams
        if catalog:
            # Sync specific streams from catalog
            for stream in catalog["streams"]:
                # Check if stream is selected by looking in metadata
                is_selected = False
                for metadata_item in stream.get("metadata", []):
                    if metadata_item.get("breadcrumb") == []:  # Root level metadata
                        if metadata_item.get("metadata", {}).get("selected", False):
                            is_selected = True
                            break
                
                if is_selected:
                    tap.sync_stream(stream["stream"], state)
        else:
            # Sync all streams
            tap.sync_all(state)

        tap.copy_json_files_to_sync_output()

if __name__ == "__main__":
    main() 