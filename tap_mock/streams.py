import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Iterator

from hotglue_singer_sdk import Stream, typing as th


def _get_base_date(config: Dict[str, Any]) -> datetime:
    """Return a timezone-aware base datetime from config, defaulting to now (UTC)."""
    if "base_date" not in config:
        return datetime.now(timezone.utc)
    base_date_str = config["base_date"].replace("Z", "+00:00")
    base_date = datetime.fromisoformat(base_date_str)
    if base_date.tzinfo is None:
        base_date = base_date.replace(tzinfo=timezone.utc)
    return base_date


def _filter_record(record: Dict[str, Any], selected_filters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Filter a record based on the selected filters configuration.

    Implements a very simple filter clause for testing purposes.
    Only one filter clause is supported.
    """
    if len(selected_filters) > 1:
        raise ValueError("Only one filter clause is supported")

    for clause_key, clause_value in selected_filters.items():
        if not clause_key.startswith("clause_"):
            raise ValueError(f"Invalid clause key: {clause_key} not supported. The key must start with: clause_")
        
        filter_field = clause_value.get("field")
        filter_operator = clause_value.get("operator")
        filter_value = clause_value.get("value")
        
        if filter_operator == "IN":
            if record.get(filter_field) not in filter_value:
                return None
        elif filter_operator == "EQ":
            if record.get(filter_field) != filter_value:
                return None
        else:
            raise ValueError(f"Invalid operator: {filter_operator} not supported. Supported operators are: IN, EQ")

    return record


class CustomersStream(Stream):
    name = "customers"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType()),
        th.Property("name", th.StringType()),
        th.Property("email", th.StringType()),
        th.Property("status", th.StringType()),
        th.Property("created_at", th.DateTimeType()),
        th.Property("updated_at", th.DateTimeType()),
        th.Property("metadata", th.ObjectType()),
    ).to_dict()

    def generate_customer_data(
        self, count: int, bookmark_dt: Optional[datetime]
    ) -> List[Dict[str, Any]]:
        """Generate mock customer data."""
        customers = []

        base_date = _get_base_date(self.config)

        if bookmark_dt:
            # Incremental sync: 5 updates + 5 new customers
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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post-process a record based on the filter configuration."""
        if self._selected_filters and not _filter_record(row, self._selected_filters):
            return None
        return row

    def get_records(self, context: Optional[dict]) -> Iterator[Dict[str, Any]]:
        sleep_time = (60 * 60) + (60 * 5)
        self.logger.info(f"Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)
        count = self.config.get("records_qty", 100)
        for row in self.generate_customer_data(count, self.get_starting_timestamp(context)):
            transformed_record = self.post_process(row, context)
            if transformed_record is None:
                continue
            yield transformed_record

    def get_available_filters_metadata(self) -> Dict[str, Any]:
        return {
            "supported_operators": ["AND", "OR"],
            "supports_nesting_clauses": True,
            "filters": {
                "id": {
                    "label": "Customer ID",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "id",
                    "options": "reference_data.customers.id",
                },
                "name": {
                    "label": "Customer Name",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "name",
                    "options": "reference_data.customers.name",
                },
                "status": {
                    "label": "Customer Status",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "status",
                    "options": ["active", "inactive", "pending"],
                }
            },
        }


class OpportunitiesStream(Stream):
    name = "opportunities"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType()),
        th.Property("name", th.StringType()),
        th.Property("customer_id", th.StringType()),
        th.Property("amount", th.NumberType()),
        th.Property("stage", th.StringType()),
        th.Property("probability", th.IntegerType()),
        th.Property("created_at", th.DateTimeType()),
        th.Property("updated_at", th.DateTimeType()),
        th.Property("metadata", th.ObjectType()),
    ).to_dict()

    def generate_opportunity_data(
        self, count: int, bookmark_dt: Optional[datetime]
    ) -> List[Dict[str, Any]]:
        """Generate mock opportunity data."""
        opportunities = []

        base_date = _get_base_date(self.config)

        if bookmark_dt:
            # Incremental sync: 1 new opportunity
            opportunity_id = f"OPP_{100+1:06d}"  # New ID
            created_date = base_date - timedelta(hours=2)
            updated_date = created_date

            if created_date > bookmark_dt:
                opportunity = {
                    "id": opportunity_id,
                    "name": "New Opportunity 1",
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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Post-process a record based on the filter configuration."""
        if self._selected_filters and not _filter_record(row, self._selected_filters):
            return None
        return row

    def get_records(self, context: Optional[dict]) -> Iterator[Dict[str, Any]]:
        count = self.config.get("records_qty", 50)
        for row in self.generate_opportunity_data(count, self.get_starting_timestamp(context)):
            transformed_record = self.post_process(row, context)
            if transformed_record is None:
                continue
            yield transformed_record

    def get_available_filters_metadata(self) -> Dict[str, Any]:
        return {
            "supported_operators": ["AND", "OR"],
            "supports_nesting_clauses": True,
            "filters": {
                "id": {
                    "label": "Opportunity ID",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "id",
                    "options": "reference_data.opportunities.id",
                },
                "customer_id": {
                    "label": "Customer ID",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "customer_id",
                    "options": "reference_data.customers.id",
                },
            },
        }
