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

    def get_records(self, context: Optional[dict]) -> Iterator[Dict[str, Any]]:
        count = self.config.get("records_qty", 100)
        yield from self.generate_customer_data(count, self.get_starting_timestamp(context))


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

    def get_records(self, context: Optional[dict]) -> Iterator[Dict[str, Any]]:
        count = self.config.get("records_qty", 50)
        yield from self.generate_opportunity_data(count, self.get_starting_timestamp(context))
