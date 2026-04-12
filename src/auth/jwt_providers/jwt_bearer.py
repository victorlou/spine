"""
RFC 7523 JWT Bearer Grant provider.

Implements the standard OAuth 2.0 JWT profile (RFC 7523): using a JWT as an
authorization grant to obtain an access token. Use for Google, Salesforce,
Microsoft Azure, and any RFC 7523-compliant API.

Reference: https://datatracker.ietf.org/doc/html/rfc7523
"""

from typing import Dict

from src.config.config_models import AuthConfig, JWTConfig


class JwtBearerProvider:
    """
    JWT provider for RFC 7523 JWT Bearer Grant.

    Uses grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer with the JWT
    as the assertion. Client authentication (Basic Auth) is optional per RFC.
    """

    def build_jwt_payload(
        self,
        auth_config: AuthConfig,
        jwt_config: JWTConfig,
        current_time: int,
    ) -> Dict[str, object]:
        """Build RFC 7523 JWT payload: iss, sub, aud, iat, exp, scope."""
        return {
            "iss": auth_config.issuer,
            "sub": auth_config.issuer,
            "aud": str(auth_config.token_url),
            "iat": current_time,
            "exp": current_time + 3600,
            "scope": jwt_config.token_exchange.get("scope"),
        }

    def build_jwt_headers(
        self,
        jwt_config: JWTConfig,
    ) -> Dict[str, str]:
        """Build JWT headers, including optional kid."""
        if jwt_config.headers.get("kid"):
            return {"kid": jwt_config.headers["kid"], "typ": "JWT"}
        return {"typ": "JWT"}

    def build_token_exchange_data(
        self,
        jwt_token: str,
        auth_config: AuthConfig,
        jwt_config: JWTConfig,
    ) -> Dict[str, str]:
        """Build token exchange form data using JWT Bearer grant (RFC 7523)."""
        return {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt_token,
        }

    def build_request_headers(
        self,
        auth_config: AuthConfig,
    ) -> Dict[str, str]:
        """RFC 7523 does not require Basic Auth for the token request."""
        return {"Content-Type": "application/x-www-form-urlencoded"}
