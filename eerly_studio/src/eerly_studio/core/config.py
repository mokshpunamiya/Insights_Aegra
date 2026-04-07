import os
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env", override=True)

class Settings:

    DATABASE_URL_AUTH: str | None = os.getenv("DATABASE_URL_AUTH")
    DATABASE_URL: str | None = os.getenv("DATABASE_URL")

    JWT_ALG: str = os.getenv("JWT_ALG", "HS256")
    JWT_ISSUER: str = os.getenv("JWT_ISSUER", "eerly_studio-application")
    JWT_AUDIENCE: str = os.getenv("JWT_AUDIENCE", "langgraph")
    JWT_ACCESS_SECRET: str = os.getenv("JWT_ACCESS_SECRET", "dev-change-me")
    JWT_REFRESH_SECRET: str = os.getenv("JWT_REFRESH_SECRET", "dev-change-me-2")

    # --- Registration Protection ---
    REGISTER_API_KEY: str | None = os.getenv("REGISTER_API_KEY")  # Set to protect /register endpoint

    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
    REFRESH_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("REFRESH_TOKEN_EXPIRE_MINUTES", "43200"))

    # --- Logging ---
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    ENV_MODE: str = os.getenv("ENV_MODE", "LOCAL")  # DEVELOPMENT, PRODUCTION, LOCAL
    LOG_VERBOSITY: str = os.getenv("LOG_VERBOSITY", "standard")  # standard, verbose


settings = Settings()
