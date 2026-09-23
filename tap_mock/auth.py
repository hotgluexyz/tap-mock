import json
import uuid

from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from hotglue_singer_sdk.helpers._util import utc_now


class MockOAuthAuthenticator(OAuthAuthenticator):
    """OAuth authenticator that fakes the provider token exchange, since tap-mock has no real provider."""

    def update_access_token_locally(self) -> None:
        request_time = utc_now()
        self.access_token = f"mock_{uuid.uuid4().hex[:16]}"
        self.expires_in = 60 + int(request_time.timestamp())
        self.last_refreshed = request_time

        self._tap._config["access_token"] = self.access_token
        self._tap._config["expires_in"] = self.expires_in
        if self.config.get("rotate_refresh_token", False):
            self._tap._config["refresh_token"] = f"mock_{uuid.uuid4().hex[:16]}"

        if self._tap.config_file is not None:
            with open(self._tap.config_file, "w") as outfile:
                json.dump(self._tap._config, outfile, indent=4)
