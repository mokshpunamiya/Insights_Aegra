# Eerly Studio Authentication Service

This package handles user authentication and session management for Eerly Studio, integrated with Aegra.

## Features
- JWT Authentication (Access/Refresh Tokens)
- User Management (AppUser)
- Session Tracking (AppSession)
- Database Integration (SQLAlchemy + AsyncPG)

## Setup
1. Define environment variables (see root `.env.example`).

## Testing

This project includes a standardized test configuration `aegra.test.json` that disables authentication for running E2E tests.

### Running Tests

1. Start the server using the test configuration:
   
   **Python:**
   ```bash
   AEGRA_CONFIG=aegra.test.json python run_server.py
   ```

   **Docker:**
   Ensure `aegra.test.json` is mounted and set the environment variable:
   ```bash
   AEGRA_CONFIG=aegra.test.json docker compose up
   ```

2. Run the tests:
   ```bash
   # Run all tests
   uv run pytest

   # Run specific test file
   uv run pytest tests/e2e/test_streaming/test_streaming_error_e2e.py
   ```
