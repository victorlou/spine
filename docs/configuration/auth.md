# Authentication

## Table of Contents

- [OAuth JWT](#oauth-jwt)
- [Bearer Token](#bearer-token)
- [API Key](#api-key)

## OAuth JWT

OAuth JWT authentication uses a signed JWT to obtain an access token. The pipeline supports multiple **providers**, each with different JWT formats and token exchange flows.

### Provider: jwt_bearer (default)

RFC 7523 JWT Bearer Grant—the standard OAuth 2.0 JWT flow. Use for Google, Salesforce, Microsoft Azure, and any RFC 7523-compliant API. No `client_id` or `client_secret` needed for the token request.

```yaml
auth:
  type: "oauth_jwt"
  token_url: "https://oauth2.googleapis.com/token"
  issuer: "${CLIENT_EMAIL}"
  private_key: "${PRIVATE_KEY_BASE64}"
  jwt_config:
    provider: "jwt_bearer"  # optional, default
    token_exchange:
      scope: "https://www.googleapis.com/auth/adwords"
```

### Provider: roundel

Roundel/Target API. Uses resource-owner password grant with JWT as the password—a vendor-specific flow. Requires `client_id` and `client_secret`.

```yaml
auth:
  type: "oauth_jwt"
  token_url: "https://oauth.example.com/token"
  client_id: "${CLIENT_ID}"
  client_secret: "${CLIENT_SECRET}"
  issuer: "${ISSUER}"
  private_key: "${PRIVATE_KEY_BASE64}"
  jwt_config:
    provider: "roundel"
    version: "2.0"
    algorithm: "RS256"
    headers:
      "alg": "RS256"
      "typ": "JWT"
    token_exchange:
      "grant_type": "password"
      "scope": "profile email openid"
```

## Bearer Token

**Static token**
```yaml
auth:
  type: "bearer_token"
  bearer_token: "${API_TOKEN}"
  header_name: "Authorization"
  header_format: "Bearer {token}"
```

**Auto-refreshing token**
```yaml
auth:
  type: "bearer_token"
  bearer_token: "${INITIAL_TOKEN}"  # Optional
  token_url: "https://api.example.com/oauth/refresh"
  client_id: "${CLIENT_ID}"
  client_secret: "${CLIENT_SECRET}"
  refresh_token: "${REFRESH_TOKEN}"
  header_name: "Authorization"
  header_format: "Bearer {token}"
```

## API Key

```yaml
auth:
  type: "api_key"
  client_id: "${API_KEY}"
  header_name: "X-API-Key"
```
