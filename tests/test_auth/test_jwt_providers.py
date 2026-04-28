"""Unit tests for JWT OAuth providers."""

import pytest

from src.auth.jwt_providers import get_provider
from src.auth.jwt_providers.jwt_bearer import JwtBearerProvider
from src.auth.jwt_providers.roundel import RoundelJWTProvider
from src.config.config_models import AuthConfig, JWTConfig


class TestGetProvider:
    """Tests for get_provider registry."""

    def test_get_jwt_bearer_provider(self) -> None:
        """Should return JwtBearerProvider for 'jwt_bearer'."""
        provider = get_provider("jwt_bearer")
        assert isinstance(provider, JwtBearerProvider)

    def test_get_roundel_provider(self) -> None:
        """Should return RoundelJWTProvider for 'roundel'."""
        provider = get_provider("roundel")
        assert isinstance(provider, RoundelJWTProvider)

    def test_get_unknown_provider_raises(self) -> None:
        """Should raise ValueError for unknown provider."""
        with pytest.raises(ValueError, match="Unknown JWT provider: unknown"):
            get_provider("unknown")


class TestJwtBearerProvider:
    """Tests for JwtBearerProvider (RFC 7523 standard)."""

    @pytest.fixture
    def auth_config(self) -> AuthConfig:
        return AuthConfig(
            type="oauth_jwt",
            token_url="https://oauth2.googleapis.com/token",
            issuer="test@project.iam.gserviceaccount.com",
            private_key="dummy",
            jwt_config=JWTConfig(
                provider="jwt_bearer",
                token_exchange={"scope": "https://www.googleapis.com/auth/adwords"},
            ),
        )

    @pytest.fixture
    def jwt_config(self) -> JWTConfig:
        return JWTConfig(
            provider="jwt_bearer",
            token_exchange={"scope": "https://www.googleapis.com/auth/adwords"},
        )

    def test_build_jwt_payload(self, auth_config: AuthConfig, jwt_config: JWTConfig) -> None:
        """Should build RFC 7523 JWT payload with iss, sub, aud, iat, exp, scope."""
        provider = JwtBearerProvider()
        payload = provider.build_jwt_payload(auth_config, jwt_config, current_time=1000)

        assert payload["iss"] == "test@project.iam.gserviceaccount.com"
        assert payload["sub"] == "test@project.iam.gserviceaccount.com"
        assert payload["aud"] == "https://oauth2.googleapis.com/token"
        assert payload["iat"] == 1000
        assert payload["exp"] == 4600  # 1000 + 3600
        assert payload["scope"] == "https://www.googleapis.com/auth/adwords"

    def test_build_jwt_headers_without_kid(self, jwt_config: JWTConfig) -> None:
        """Should return typ only when kid is not in config."""
        jwt_config.headers = {"alg": "RS256", "typ": "JWT"}
        provider = JwtBearerProvider()
        headers = provider.build_jwt_headers(jwt_config)
        assert headers == {"typ": "JWT"}

    def test_build_jwt_headers_with_kid(self, jwt_config: JWTConfig) -> None:
        """Should include kid when present in config."""
        jwt_config.headers = {"alg": "RS256", "typ": "JWT", "kid": "key-123"}
        provider = JwtBearerProvider()
        headers = provider.build_jwt_headers(jwt_config)
        assert headers == {"kid": "key-123", "typ": "JWT"}

    def test_build_token_exchange_data(
        self, auth_config: AuthConfig, jwt_config: JWTConfig
    ) -> None:
        """Should build JWT Bearer grant assertion (RFC 7523)."""
        provider = JwtBearerProvider()
        data = provider.build_token_exchange_data("signed-jwt-token", auth_config, jwt_config)

        assert data["grant_type"] == "urn:ietf:params:oauth:grant-type:jwt-bearer"
        assert data["assertion"] == "signed-jwt-token"

    def test_build_request_headers(self, auth_config: AuthConfig) -> None:
        """Should return Content-Type only, no Basic Auth."""
        provider = JwtBearerProvider()
        headers = provider.build_request_headers(auth_config)
        assert headers == {"Content-Type": "application/x-www-form-urlencoded"}
        assert "Authorization" not in headers


class TestRoundelJWTProvider:
    """Tests for RoundelJWTProvider."""

    @pytest.fixture
    def auth_config(self) -> AuthConfig:
        return AuthConfig(
            type="oauth_jwt",
            token_url="https://oauth.example.com/token",
            issuer="my-issuer",
            client_id="client-123",
            client_secret="secret-456",
            private_key="dummy",
            jwt_config=JWTConfig(
                provider="roundel",
                version="2.0",
                headers={"alg": "RS256", "typ": "JWT"},
                token_exchange={"grant_type": "password", "scope": "profile email openid"},
            ),
        )

    @pytest.fixture
    def jwt_config(self) -> JWTConfig:
        return JWTConfig(
            provider="roundel",
            version="2.0",
            headers={"alg": "RS256", "typ": "JWT"},
            token_exchange={"grant_type": "password", "scope": "profile email openid"},
        )

    def test_build_jwt_payload(self, auth_config: AuthConfig, jwt_config: JWTConfig) -> None:
        """Should build Roundel payload with v, t, n."""
        provider = RoundelJWTProvider()
        payload = provider.build_jwt_payload(auth_config, jwt_config, current_time=2000)

        assert payload["v"] == "2.0"
        assert payload["t"] == 2000
        assert "n" in payload
        assert payload["n"].startswith("2000")
        assert len(payload["n"]) >= 36  # timestamp + uuid without dashes (32)

    def test_build_jwt_payload_nonce_is_unique(
        self, auth_config: AuthConfig, jwt_config: JWTConfig
    ) -> None:
        """Should generate unique nonce each call."""
        provider = RoundelJWTProvider()
        p1 = provider.build_jwt_payload(auth_config, jwt_config, current_time=2000)
        p2 = provider.build_jwt_payload(auth_config, jwt_config, current_time=2000)
        assert p1["n"] != p2["n"]

    def test_build_jwt_headers(self, jwt_config: JWTConfig) -> None:
        """Should return jwt_config headers."""
        provider = RoundelJWTProvider()
        headers = provider.build_jwt_headers(jwt_config)
        assert headers == {"alg": "RS256", "typ": "JWT"}

    def test_build_token_exchange_data(
        self, auth_config: AuthConfig, jwt_config: JWTConfig
    ) -> None:
        """Should merge token_exchange with username and password."""
        provider = RoundelJWTProvider()
        data = provider.build_token_exchange_data("my-jwt-token", auth_config, jwt_config)

        assert data["grant_type"] == "password"
        assert data["scope"] == "profile email openid"
        assert data["username"] == "my-issuer"
        assert data["password"] == "my-jwt-token"

    def test_build_request_headers(self, auth_config: AuthConfig) -> None:
        """Should include Basic Auth and Content-Type."""
        provider = RoundelJWTProvider()
        headers = provider.build_request_headers(auth_config)

        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Basic ")
        assert headers["Content-Type"] == "application/x-www-form-urlencoded"
