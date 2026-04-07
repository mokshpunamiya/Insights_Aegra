# 🔐 Authentication & Security

Insights Aegra features a full, production-grade authentication system implemented in the Eerly Studio service layer. It uses **JWT-based access + refresh token** flow backed by PostgreSQL session storage.

---

## Authentication Architecture

```
Client
  │
  │  POST /login (form: username, password)
  ▼
Auth Service (FastAPI)
  │
  ├── Verifies password against Argon2 hash in AppUser table
  │
  ├── Creates AppSession record (tracks device, IP, expiry)
  │
  └── Returns:
        {
          "access_token": "eyJ....",    # Short-lived JWT (30 min)
          "refresh_token": "eyJ...",    # Long-lived JWT (43200 min)
          "token_type": "bearer"
        }
```

---

## Token Lifecycle

| Token | Lifespan | Storage | Use |
| :--- | :--- | :--- | :--- |
| **Access Token** | 30 min (configurable) | Client memory | Sent as `Authorization: Bearer <token>` on every request |
| **Refresh Token** | 30 days (configurable) | Secure cookie / client storage | Used to obtain a new access token |

> Tokens are **HMAC-signed JWS** using HS256. Secrets are loaded from `.env` — never committed to the repository.

---

## API Endpoints

### `POST /register`

Creates a new user account. Optionally protected by a server-side API key (`REGISTER_API_KEY`).

```bash
curl -X POST http://localhost:2024/register \
  -F "username=moksh" \
  -F "display_name=Moksh Punamiya" \
  -F "password=securepassword" \
  -F "email=moksh@example.com"
```

> Set `REGISTER_API_KEY` in `.env` to restrict who can create accounts. Pass it as `X-Api-Key` header.

---

### `POST /login`

Authenticates a user and issues access + refresh tokens.

```bash
curl -X POST http://localhost:2024/login \
  -F "login_id=moksh" \
  -F "password=securepassword" \
  -F "remember_me=false"
```

**Response:**
```json
{
  "access_token": "eyJ...",
  "refresh_token": "eyJ...",
  "token_type": "bearer"
}
```

---

### `POST /refresh-token`

Exchanges a valid refresh token for a new access token. Requires the same device IP + User-Agent fingerprint (session binding).

```bash
curl -X POST http://localhost:2024/refresh-token \
  -F "refresh_token=eyJ..."
```

---

### `POST /logout`

Revokes the current session. Pass `invalidate_all=true` to revoke all sessions for the user.

```bash
curl -X POST http://localhost:2024/logout \
  -H "Authorization: Bearer eyJ..." \
  -F "invalidate_all=false"
```

---

## Aegra Auth Hook (`my_auth.py`)

Aegra's Agent Protocol endpoints (runs, threads) are protected via a custom auth handler registered in `aegra.json`:

```json
"auth": { "path": "eerly_studio.my_auth:auth" }
```

The handler validates the `Authorization: Bearer <token>` header on every agent API call, decodes the JWT, and injects user context.

**To customize:**

Edit `eerly_studio/src/eerly_studio/my_auth.py`. The `auth` object must be an Aegra-compatible auth handler implementing `on_request()`.

---

## Security Hardening Checklist

- [ ] **Secrets**: All JWT secrets, API keys, and DB credentials are in `.env` — never committed
- [ ] **REGISTER_API_KEY**: Set this in production to prevent unauthorized account creation
- [ ] **Argon2 hashing**: Passwords are hashed with Argon2id (via `passlib[argon2]`) — never stored in plaintext
- [ ] **Session Binding**: Refresh tokens are bound to `(IP, User-Agent)` pairs — prevents token theft
- [ ] **SQL Injection**: All DB access uses SQLAlchemy ORM with parameterized queries — no raw SQL
- [ ] **No PII in logs**: Passwords, tokens, and user emails are never logged
- [ ] **CORS**: Configure `CORS_ORIGINS` in `.env` to restrict cross-origin requests to known frontends

---

## Environment Variables

```env
# JWT Secrets — CHANGE THESE IN PRODUCTION
JWT_ACCESS_SECRET=dev-change-me
JWT_REFRESH_SECRET=dev-change-me-2
JWT_ALG=HS256
JWT_ISSUER=eerly_studio-application
JWT_AUDIENCE=langgraph

# Token Expiry (in minutes)
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_MINUTES=43200  # 30 days

# Registration Protection (optional)
REGISTER_API_KEY=your-secret-internal-key
```

> ⚠️ **Critical**: Replace `JWT_ACCESS_SECRET` and `JWT_REFRESH_SECRET` with long, cryptographically random strings before deploying to production. Use `openssl rand -hex 32` to generate them.
