import os
import shutil
from datetime import datetime
from typing import List

from hotglue_singer_sdk import Tap, Stream, typing as th

from tap_mock.auth import MockOAuthAuthenticator
from tap_mock.streams import CustomersStream, OpportunitiesStream

JOB_ID = os.getenv("JOB_ID")
LOCAL_ROOT = f"/home/hotglue/{JOB_ID}"
SYNC_OUTPUT_PATH = f"{LOCAL_ROOT}/sync-output"


class TapMock(Tap):
    """Mock Singer Tap for testing purposes."""

    name = "tap-mock"
    config_jsonschema = th.PropertiesList(
        th.Property("auth_type", th.StringType(), required=True),
        th.Property("client_id", th.StringType()),
        th.Property("client_secret", th.StringType()),
        th.Property("refresh_token", th.StringType()),
        th.Property("rotate_refresh_token", th.BooleanType()),
        th.Property("api_key", th.StringType()),
        th.Property("records_qty", th.IntegerType()),
        th.Property("base_date", th.StringType()),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.copy_json_files_to_sync_output()
        self._validate_settings()
        if self.config.get("auth_type") == "api_key":
            self._authenticate_api_key()

    @classmethod
    def access_token_support(cls, connector=None):
        return MockOAuthAuthenticator, "https://mock.invalid/oauth/token"

    def copy_json_files_to_sync_output(self):
        """Copy all JSON files from current working directory to SYNC_OUTPUT_PATH."""
        current_dir = os.getcwd()

        try:
            items = os.listdir(current_dir)

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
                    except Exception:
                        pass  # Silently handle copy errors

        except (FileNotFoundError, PermissionError, Exception):
            pass  # Silently handle all errors

    def _validate_settings(self):
        """Validate the configuration."""
        if self.config.get("auth_type") not in ["oauth", "api_key"]:
            raise ValueError("auth_type must be either 'oauth' or 'api_key'")

        if self.config.get("auth_type") == "oauth":
            required_oauth_keys = ["client_id", "client_secret"]
            for key in required_oauth_keys:
                if key not in self.config:
                    raise ValueError(f"OAuth config missing required key: {key}")

        if self.config.get("auth_type") == "api_key":
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
                datetime.fromisoformat(base_date.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError("base_date must be a valid datetime in ISO format")

    def _authenticate_oauth(self):
        """Get an access token through the SDK OAuth flow, which calls the Hotglue access token endpoint."""
        authenticator = MockOAuthAuthenticator(stream=_AuthStream(self))
        authenticator.update_access_token()
        self.logger.info(f"Fetched access token: {authenticator.access_token}")

    def _authenticate_api_key(self):
        """Simulate API key authentication."""
        self.logger.info("Authenticating with API key...")
        api_key = self.config.get("api_key")
        if api_key:
            self.logger.info(f"Using API key: {api_key[:10]}...")
        else:
            self.logger.info("No API key provided")
        self.logger.info("API key authentication successful")

    def discover_streams(self) -> List[Stream]:
        return [
            CustomersStream(self),
            OpportunitiesStream(self),
        ]

    def sync_all(self) -> None:
        try:
            if self.config.get("auth_type") == "oauth":
                self._authenticate_oauth()
            super().sync_all()
        finally:
            self.copy_json_files_to_sync_output()


class _AuthStream:
    """Stand-in for a stream, which the authenticator needs."""

    def __init__(self, tap: Tap):
        self._tap = tap
        self.tap_name = tap.name
        self.config = tap.config
        self.logger = tap.logger


if __name__ == "__main__":
    TapMock.cli()
