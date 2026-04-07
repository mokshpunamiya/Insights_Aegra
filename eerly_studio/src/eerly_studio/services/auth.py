import hashlib
from datetime import UTC, datetime, timedelta

from eerly_studio.core.config import settings
from jose import JWTError, jwt
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

JWT_ALG = settings.JWT_ALG
JWT_ISSUER = settings.JWT_ISSUER
JWT_AUDIENCE = settings.JWT_AUDIENCE
JWT_ACCESS_SECRET = settings.JWT_ACCESS_SECRET
JWT_REFRESH_SECRET = settings.JWT_REFRESH_SECRET

ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES
REFRESH_TOKEN_EXPIRE_MINUTES = settings.REFRESH_TOKEN_EXPIRE_MINUTES


def utcnow() -> datetime:
    return datetime.now(UTC)


def hash_password(pw: str) -> str:
    return pwd_context.hash(pw)


def verify_password(pw: str, pw_hash: str) -> bool:
    return pwd_context.verify(pw, pw_hash)


def create_access_token(claims: dict, expires_delta: timedelta | None = None) -> str:
    now = utcnow()
    exp = now + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    payload = {
        **claims,
        "typ": "access",
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, JWT_ACCESS_SECRET, algorithm=JWT_ALG)


def create_refresh_token(claims: dict, expires_delta: timedelta | None = None) -> str:
    now = utcnow()
    exp = now + (expires_delta or timedelta(minutes=REFRESH_TOKEN_EXPIRE_MINUTES))
    payload = {
        **claims,
        "typ": "refresh",
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, JWT_REFRESH_SECRET, algorithm=JWT_ALG)


def decode_access_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, JWT_ACCESS_SECRET, algorithms=[JWT_ALG], issuer=JWT_ISSUER, audience=JWT_AUDIENCE)
    except JWTError as e:
        raise ValueError("Invalid access token") from e
    if payload.get("typ") != "access":
        raise ValueError("Wrong token type")
    return payload


def decode_refresh_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, JWT_REFRESH_SECRET, algorithms=[JWT_ALG], issuer=JWT_ISSUER, audience=JWT_AUDIENCE)
    except JWTError as e:
        raise ValueError("Invalid refresh token") from e
    if payload.get("typ") != "refresh":
        raise ValueError("Wrong token type")
    return payload


def refresh_token_fingerprint(raw_refresh_token: str) -> str:
    return hashlib.sha256(raw_refresh_token.encode("utf-8")).hexdigest()
