# ⚙️ Configuration Guide

This guide covers all configuration options for Insights Aegra — from environment variables and database settings to LLM providers and observability.

---

## Quick Start

Copy the environment template and fill in your credentials:
```bash
cp .env.example .env
```

---

## Environment Variables Reference

### Core Application

| Variable | Required | Default | Description |
| :--- | :---: | :--- | :--- |
| `PROJECT_NAME` | No | `Aegra` | Display name for the service |
| `AEGRA_CONFIG` | **Yes** | `aegra.json` | Path to the agent graph config file |
| `DEBUG` | No | `false` | Enable debug mode |

---

### Database

Insights Aegra requires a **PostgreSQL** database. Configure using either a full connection string (recommended for production) or individual fields.

**Option 1 — Connection String (Recommended)**
```env
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/insights_aegra
```

**Option 2 — Individual Fields**
```env
POSTGRES_DB=insights_aegra
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=user
POSTGRES_PASSWORD=password
```

> **Note:** LangGraph's checkpointer uses `postgresql://` (psycopg driver) while SQLAlchemy uses `postgresql+asyncpg://`. Aegra handles this conversion automatically.

---

### Connection Pools

| Variable | Default | Description |
| :--- | :--- | :--- |
| `SQLALCHEMY_POOL_SIZE` | `2` | Pool size for metadata/auth DB connections |
| `SQLALCHEMY_MAX_OVERFLOW` | `0` | Max extra connections above pool size |
| `LANGGRAPH_MIN_POOL_SIZE` | `1` | Min connections for LangGraph agent runtime |
| `LANGGRAPH_MAX_POOL_SIZE` | `6` | Max connections for LangGraph agent runtime |

---

### LLM Providers

The `insights` graph is designed to work with **Azure OpenAI / Mistral** endpoints. Set the key relevant to your deployment:

```env
OPENAI_API_KEY=sk-...
AZURE_OPENAI_API_KEY=...
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com
AZURE_OPENAI_API_VERSION=2024-02-01
```

---

### Vector Store (Qdrant)

The Data Engineer agent uses Qdrant for RAG-based schema retrieval:

```env
QDRANT_URL=http://localhost:6333
QDRANT_API_KEY=
QDRANT_COLLECTION_NAME=insights_schema
```

---

### Sandboxed Execution (E2B)

The Data Scientist agent runs Python in isolated E2B cloud sandboxes:

```env
E2B_API_KEY=e2b_...
```

Get your key at [e2b.dev](https://e2b.dev).

---

### Server

| Variable | Default | Description |
| :--- | :--- | :--- |
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `2024` | Server port |
| `SERVER_URL` | `http://localhost:2024` | Public-facing URL |

---

### Logging

| Variable | Default | Options | Description |
| :--- | :--- | :--- | :--- |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` | Log verbosity level |
| `ENV_MODE` | `LOCAL` | `LOCAL`, `DEVELOPMENT`, `PRODUCTION` | `PRODUCTION` outputs structured JSON logs |
| `LOG_VERBOSITY` | `standard` | `standard`, `verbose` | `verbose` adds request correlation IDs |

---

### Observability (OpenTelemetry)

Aegra supports distributed tracing via fan-out to multiple backends:

```env
OTEL_SERVICE_NAME=insights-aegra
# Comma-separated list: LANGFUSE, PHOENIX, GENERIC (or leave empty)
OTEL_TARGETS=LANGFUSE
```

**Langfuse** (recommended for LLM trace visualization):
```env
LANGFUSE_BASE_URL=https://cloud.langfuse.com
LANGFUSE_PUBLIC_KEY=pk-...
LANGFUSE_SECRET_KEY=sk-...
```

**Arize Phoenix** (local tracing dashboard):
```env
PHOENIX_COLLECTOR_ENDPOINT=http://127.0.0.1:6006/v1/traces
```

---

## `aegra.json` — Graph Registration

This file tells Aegra which agent graphs to expose on the HTTP API:

```json
{
  "graphs": {
    "insights": "./eerly_studio/src/eerly_studio/insights/graph.py:graph"
  },
  "auth": {
    "path": "eerly_studio.my_auth:auth"
  },
  "http": {
    "app": "eerly_studio.main:app",
    "enable_custom_route_auth": false
  }
}
```

| Key | Description |
| :--- | :--- |
| `graphs` | Map of graph IDs to their Python module path and exported `graph` variable |
| `auth.path` | Python import path to your custom auth handler |
| `http.app` | FastAPI sub-application to mount alongside Aegra |
| `http.enable_custom_route_auth` | Whether to protect custom HTTP routes with auth |
