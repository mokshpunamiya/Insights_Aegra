from datetime import timedelta

from eerly_studio.core.database import get_db
from eerly_studio.core.orm import AppSession, AppUser
from eerly_studio.services.auth import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    REFRESH_TOKEN_EXPIRE_MINUTES,
    create_access_token,
    create_refresh_token,
    decode_access_token,
    decode_refresh_token,
    hash_password,
    refresh_token_fingerprint,
    utcnow,
    verify_password,
)
from fastapi import APIRouter, Depends, Form, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="", tags=["Authentication"])


def _client_ip(request: Request) -> str | None:
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else None


@router.post("/register")
async def register(
    username: str = Form(...),
    display_name: str = Form(...),
    password: str = Form(...),
    email: str | None = Form(default=None),
    org_id: str | None = Form(default=None),
    x_api_key: str | None = Header(default=None, alias="X-Api-Key"),
    db: AsyncSession = Depends(get_db),
):
    # Validate API key when REGISTER_API_KEY is configured
    from eerly_studio.core.config import settings as studio_settings

    if studio_settings.REGISTER_API_KEY is not None and (
        not x_api_key or x_api_key != studio_settings.REGISTER_API_KEY
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API key",
        )
    username_norm = username.strip().lower()
    email_norm = email.strip().lower() if email else None

    q = await db.execute(select(AppUser).where((AppUser.username == username_norm) | (AppUser.email == email_norm)))
    if q.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Username or email already exists")

    user = AppUser(
        username=username_norm,
        display_name=display_name.strip(),
        email=email_norm,
        org_id=org_id,
        password_hash=hash_password(password),
        permissions=[],
    )
    db.add(user)
    await db.commit()

    return JSONResponse({"message": "User created"}, status_code=status.HTTP_201_CREATED)


@router.post("/login")
async def login(
    request: Request,
    login_id: str = Form(...),
    password: str = Form(...),
    remember_me: bool = Form(default=False),
    user_agent: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
):
    login_norm = login_id.strip().lower()
    ip = _client_ip(request)

    q = await db.execute(select(AppUser).where((AppUser.username == login_norm) | (AppUser.email == login_norm)))
    user = q.scalar_one_or_none()

    if not user or user.disabled_at is not None:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not user.password_hash or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    access_exp = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_exp = timedelta(days=28) if remember_me else timedelta(minutes=REFRESH_TOKEN_EXPIRE_MINUTES)

    refresh_token = create_refresh_token({"sub": str(user.user_id), "user_id": user.user_id}, expires_delta=refresh_exp)

    now = utcnow()
    sess = AppSession(
        user_id=user.user_id,
        refresh_token_hash=refresh_token_fingerprint(refresh_token),
        user_agent=user_agent,
        ip_address=ip,
        issued_at=now,
        last_activity_at=now,
        expires_at=now + refresh_exp,
        is_valid=True,
        refresh_count=0,
    )
    db.add(sess)
    await db.commit()
    await db.refresh(sess)

    access_token = create_access_token(
        {
            "sub": str(user.user_id),
            "user_id": user.user_id,
            "username": user.username,
            "email": user.email,
            "org_id": user.org_id,
            "permissions": user.permissions or [],
            "sid": sess.session_id,
        },
        expires_delta=access_exp,
    )

    return JSONResponse(
        {"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}, status_code=200
    )


@router.post("/refresh-token")
async def refresh_token(
    request: Request,
    refresh_token: str = Form(...),
    user_agent: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
):
    ip = _client_ip(request)
    try:
        payload = decode_refresh_token(refresh_token)
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    user_id = payload.get("user_id")
    if not isinstance(user_id, int):
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    rt_hash = refresh_token_fingerprint(refresh_token)
    sq = await db.execute(
        select(AppSession).where(
            and_(
                AppSession.user_id == user_id,
                AppSession.user_agent == user_agent,
                AppSession.ip_address == ip,
                AppSession.is_valid,
                AppSession.refresh_token_hash == rt_hash,
            )
        )
    )
    sess = sq.scalar_one_or_none()
    if not sess:
        raise HTTPException(status_code=401, detail="No valid session found")

    now = utcnow()
    if sess.expires_at < now:
        sess.is_valid = False
        sess.revoked_at = now
        await db.commit()
        raise HTTPException(status_code=401, detail="Refresh token expired")

    uq = await db.execute(select(AppUser).where(AppUser.user_id == user_id))
    user = uq.scalar_one_or_none()
    if not user or user.disabled_at is not None:
        raise HTTPException(status_code=401, detail="User inactive")

    new_access = create_access_token(
        {
            "sub": str(user.user_id),
            "user_id": user.user_id,
            "username": user.username,
            "email": user.email,
            "org_id": user.org_id,
            "permissions": user.permissions or [],
            "sid": sess.session_id,
        }
    )

    sess.last_activity_at = now
    sess.refresh_count += 1
    await db.commit()

    return {"access_token": new_access, "token_type": "bearer"}


@router.post("/logout")
async def logout(
    authorization: str = Header(...),
    invalidate_all: bool = Form(default=False),
    db: AsyncSession = Depends(get_db),
):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    token = authorization[len("Bearer ") :].strip()
    try:
        payload = decode_access_token(token)
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid access token")

    user_id = payload.get("user_id")
    sid = payload.get("sid")
    if not isinstance(user_id, int):
        raise HTTPException(status_code=401, detail="Invalid access token")

    now = utcnow()
    if invalidate_all:
        await db.execute(
            update(AppSession)
            .where(and_(AppSession.user_id == user_id, AppSession.is_valid))
            .values(is_valid=False, revoked_at=now)
        )
    elif isinstance(sid, int):
        await db.execute(
            update(AppSession)
            .where(and_(AppSession.session_id == sid, AppSession.user_id == user_id, AppSession.is_valid))
            .values(is_valid=False, revoked_at=now)
        )
    else:
        raise HTTPException(status_code=400, detail="Missing session id")

    await db.commit()
    return {"message": "Logged out"}
