import os

from jose import JWTError, jwt
from langgraph_sdk import Auth

# Configuration (must match eerly_studio)
JWT_ALG = os.getenv("JWT_ALG", "HS256")
JWT_ISSUER = os.getenv("JWT_ISSUER", "eerly_studio-application")
JWT_AUDIENCE = os.getenv("JWT_AUDIENCE", "langgraph")
JWT_ACCESS_SECRET = os.getenv("JWT_ACCESS_SECRET", "dev-change-me")

# When AUTH_TYPE=noop, skip JWT validation (for local dev & CI E2E tests)
AUTH_TYPE = os.getenv("AUTH_TYPE", "custom")

auth = Auth()


@auth.authenticate
async def authenticate(headers: dict) -> dict:
    """
    Standalone Auth Handler.
    Verifies the JWT token issued by eerly_studio without database access.
    When AUTH_TYPE=noop, all requests are treated as authenticated (no JWT required).
    """
    # Noop mode: allow all requests without authentication
    if AUTH_TYPE == "noop":
        return {"identity": "anonymous", "display_name": "Anonymous User", "is_authenticated": True, "permissions": []}

    auth_header = headers.get("authorization", "") or headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        # Return anonymous user if no token
        return {"identity": "anonymous", "display_name": "Anonymous User", "is_authenticated": False, "permissions": []}

    token = auth_header[7:].strip()

    try:
        payload = jwt.decode(
            token,
            JWT_ACCESS_SECRET,
            algorithms=[JWT_ALG],
            issuer=JWT_ISSUER,
            audience=JWT_AUDIENCE,
        )
    except JWTError:
        # Invalid token -> anonymous
        return {"identity": "anonymous", "display_name": "Anonymous User", "is_authenticated": False, "permissions": []}

    # Check token type
    if payload.get("typ") != "access":
        return {"identity": "anonymous", "display_name": "Anonymous User", "is_authenticated": False, "permissions": []}

    # Extract user info claims
    user_id = str(payload.get("user_id"))  # Ensure string for Aegra identity
    username = payload.get("username", "Unknown")
    permissions = payload.get("permissions", [])

    return {
        "identity": user_id,
        "display_name": username,
        "is_authenticated": True,
        "permissions": permissions,
        # Custom fields for context
        "email": payload.get("email"),
        "org_id": payload.get("org_id"),
        "sid": payload.get("sid"),
    }
