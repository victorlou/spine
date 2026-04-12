"""Base protocol for JWT OAuth providers."""

from typing import Dict, Protocol

from src.config.config_models import AuthConfig, JWTConfig


class JWTProvider(Protocol):
    """
    Protocol for JWT OAuth token exchange providers.

    Defines the interface for RFC 7523 (jwt_bearer) and vendor-specific providers
    (e.g. roundel). Each provider implements how to build the JWT payload, token
    exchange request body, and request headers for its specific OAuth flow.
    """

    def build_jwt_payload(
        self,
        auth_config: AuthConfig,
        jwt_config: JWTConfig,
        current_time: int,
    ) -> Dict[str, object]:
        """
        Build the JWT payload (claims) for signing.

        Args:
            auth_config: Authentication configuration
            jwt_config: JWT-specific configuration
            current_time: Current Unix timestamp

        Returns:
            Dictionary of JWT claims
        """
        ...  # pragma: no cover

    def build_token_exchange_data(
        self,
        jwt_token: str,
        auth_config: AuthConfig,
        jwt_config: JWTConfig,
    ) -> Dict[str, str]:
        """
        Build the form data for the token exchange request.

        Args:
            jwt_token: The signed JWT
            auth_config: Authentication configuration
            jwt_config: JWT-specific configuration

        Returns:
            Form data dict for the token exchange POST request
        """
        ...  # pragma: no cover

    def build_jwt_headers(
        self,
        jwt_config: JWTConfig,
    ) -> Dict[str, str]:
        """
        Build the JWT headers (e.g. kid, typ) for the signed JWT.

        Args:
            jwt_config: JWT-specific configuration

        Returns:
            Headers dict passed to jwt.encode
        """
        ...  # pragma: no cover

    def build_request_headers(
        self,
        auth_config: AuthConfig,
    ) -> Dict[str, str]:
        """
        Build the HTTP headers for the token exchange request.

        Args:
            auth_config: Authentication configuration

        Returns:
            Headers dict for the token exchange POST request
        """
        ...  # pragma: no cover
