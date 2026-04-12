"""
Roundel JWT OAuth provider (Target/Roundel API).

Uses resource-owner password grant with JWT as the password—a vendor-specific
flow, not RFC 7523. For the standard JWT Bearer flow, use provider "jwt_bearer".
"""

import base64
import uuid
from typing import Dict

from src.config.config_models import AuthConfig, JWTConfig


class RoundelJWTProvider:
    """
    JWT provider for Roundel/Target OAuth flows that use resource-owner password
    grant with JWT as the password.
    """

    def build_jwt_payload(
        self,
        auth_config: AuthConfig,
        jwt_config: JWTConfig,
        current_time: int,
    ) -> Dict[str, object]:
        """Build Roundel JWT payload: v (version), t (timestamp), n (nonce)."""
        random_uuid = str(uuid.uuid4()).replace("-", "").lower()
        return {
            "v": jwt_config.version,
            "t": current_time,
            "n": f"{current_time}{random_uuid}",
        }

    def build_jwt_headers(
        self,
        jwt_config: JWTConfig,
    ) -> Dict[str, str]:
        """Build JWT headers from jwt_config."""
        return jwt_config.headers

    def build_token_exchange_data(
        self,
        jwt_token: str,
        auth_config: AuthConfig,
        jwt_config: JWTConfig,
    ) -> Dict[str, str]:
        """Build token exchange form data: token_exchange + username/password."""
        token_data = jwt_config.token_exchange.copy()
        token_data.update({"username": auth_config.issuer, "password": jwt_token})
        return token_data

    def build_request_headers(
        self,
        auth_config: AuthConfig,
    ) -> Dict[str, str]:
        """Roundel flows require Basic Auth with client_id:client_secret."""
        credentials = f"{auth_config.client_id}:{auth_config.client_secret}"
        basic_auth = base64.b64encode(credentials.encode()).decode()
        return {
            "Authorization": f"Basic {basic_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
