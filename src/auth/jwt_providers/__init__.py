"""JWT OAuth provider implementations."""

from typing import Dict

from src.auth.jwt_providers.base import JWTProvider
from src.auth.jwt_providers.jwt_bearer import JwtBearerProvider
from src.auth.jwt_providers.roundel import RoundelJWTProvider

JWT_BEARER_PROVIDER = JwtBearerProvider()
PROVIDERS: Dict[str, JWTProvider] = {
    "jwt_bearer": JWT_BEARER_PROVIDER,  # RFC 7523 standard, default
    "roundel": RoundelJWTProvider(),
}


def get_provider(provider_name: str) -> JWTProvider:
    """
    Get the JWT provider for the given provider name.

    Args:
        provider_name: The provider identifier (e.g. "jwt_bearer", "roundel")

    Returns:
        The JWT provider instance

    Raises:
        ValueError: If the provider is not registered
    """
    if provider_name not in PROVIDERS:
        raise ValueError(
            f"Unknown JWT provider: {provider_name}. Available: {list(PROVIDERS.keys())}"
        )
    return PROVIDERS[provider_name]


__all__ = [
    "PROVIDERS",
    "JWTProvider",
    "JwtBearerProvider",
    "RoundelJWTProvider",
    "get_provider",
]
