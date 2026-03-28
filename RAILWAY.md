# Deploying on Railway

This project is a **multi-service** Python monorepo. Railway deploys **one Docker image per service**. There is **no** single container that runs the whole stack.

## 1. Prerequisites

- A [Railway](https://railway.com) account and a **new Project**.
- **Docker** builds only from the Dockerfiles under `services/*/Dockerfile` (repo root is the build context).

Install nothing extra in the repo: `requirements.txt` is baked into each image.

## 2. Add Redis

1. In the project, **New** → **Database** → **Add Redis** (or compatible).
2. Railway injects **`REDIS_URL`** into linked services. This codebase reads **`REDIS_URL`** when **`STREAM_URL`** is not set (`services/common/app/config.py` → `get_stream_url()`).
3. For **each** application service below, either:
   - Add a **variable** `STREAM_URL` referencing `${{Redis.REDIS_URL}}`, or  
   - Rely on **`REDIS_URL`** alone (no `STREAM_URL` needed).

Link Redis to every service that publishes or consumes streams (all except nothing—all Python services use the broker or publisher).

## 3. Create one Railway service per microservice

Create **10** empty services from the **same GitHub repository**. Name them so private DNS is predictable (examples: `api`, `auth`, `cache`, `database`, `event-processor`, `synthetic-generator`, `rca`, `remediation`, `payment`, `notification`).

For **each** service:

1. **Settings → Source**: connect the repo.
2. **Settings → Build**:
   - Builder: **Dockerfile**.
   - **Dockerfile path**: match the table below (paths relative to repo root).
   - **Root directory** / context: repository root (default), so `COPY . .` in the Dockerfile sees `requirements.txt` and `services/`.
3. **Settings → Deploy**:
   - Health check path: **`/health`** (already suggested by root `railway.toml`).
4. Optional: set **`RAILWAY_DOCKERFILE_PATH`** instead of using the UI if you prefer config-as-code per service.

| Railway service name (example) | Dockerfile path |
|--------------------------------|-----------------|
| `api` | `services/api_service/Dockerfile` |
| `auth` | `services/auth_service/Dockerfile` |
| `cache` | `services/cache_service/Dockerfile` |
| `database` | `services/database_service/Dockerfile` |
| `event-processor` | `services/event_processor_service/Dockerfile` |
| `synthetic-generator` | `services/synthetic_generator_service/Dockerfile` |
| `rca` | `services/rca_service/Dockerfile` |
| `remediation` | `services/remediation_service/Dockerfile` |
| `payment` | `services/payment_service/Dockerfile` |
| `notification` | `services/notification_service/Dockerfile` |

## 4. Networking between services

Railway injects **`PORT`** at runtime. Each Dockerfile runs Uvicorn with **`--host :: --port ${PORT:-…}`**, so on Railway every process listens on the **same numeric `PORT`** inside its own container.

**Option A — Private networking (recommended)**  
Use HTTP (not HTTPS) and the internal host **`http://<service-slug>.railway.internal:<PORT>`**.  
Replace `<PORT>` with the value Railway assigns (check any service’s deploy logs after first successful boot). Often every service shares the same port number (e.g. `8080`).

Example for **api-service** variables:

```text
AUTH_SERVICE_URL=http://auth.railway.internal:8080
CACHE_SERVICE_URL=http://cache.railway.internal:8080
DATABASE_SERVICE_URL=http://database.railway.internal:8080
EVENT_PROCESSOR_SERVICE_URL=http://event-processor.railway.internal:8080
RCA_SERVICE_URL=http://rca.railway.internal:8080
SYNTHETIC_GENERATOR_SERVICE_URL=http://synthetic-generator.railway.internal:8080
PAYMENT_SERVICE_URL=http://payment.railway.internal:8080
NOTIFICATION_SERVICE_URL=http://notification.railway.internal:8080
```

Use your **exact** Railway service names as the first segment of the hostname (`auth`, `cache`, …).  
You can also build these with [Reference variables](https://docs.railway.com/variables#reference-variables) if you expose a numeric port variable on each target service.

**Option B — Public URLs**  
Generate a public domain for each service and set `AUTH_SERVICE_URL=https://${{auth.RAILWAY_PUBLIC_DOMAIN}}`, etc. Simpler mentally; traffic leaves the private mesh.

**Browsers** must call the **api** public URL only (or the same host that serves `index.html`). The dashboard cannot use `*.railway.internal` from the user’s laptop.

## 5. Dependency order (Docker Compose vs Railway)

Locally, Compose enforces `depends_on`. On Railway **all services start in parallel**. If the API starts before Redis or auth, you may see transient 502s until dependencies are healthy; Redis client code retries in `get_stream_backend()`.

## 6. Users and dashboard URL

- Assign a **public domain** to **`api`** only (or put a CDN in front of it).
- Open `https://<your-api-domain>/` — the API serves `index.html` at `/` and proxies observability routes.

`index.html` uses the page **origin** for API calls in production, so same-host deployment works without extra env on the client.

## 7. Optional / cost controls

- **`SYNTHETIC_GENERATOR_ENABLED=false`** on `synthetic-generator` (or stop that service) to reduce load.
- **Prometheus** from `docker-compose.yml` is **not** required for the app; skip it on Railway unless you add a separate Prometheus service.

## 8. Files added for Railway

| File | Purpose |
|------|---------|
| `railway.toml` | Shared deploy defaults (`/health` check, restart policy). |
| `env.railway.example` | Variable checklist and examples. |
| `RAILWAY.md` | This guide. |
| `.dockerignore` | Smaller, faster Docker builds. |

Dockerfiles now honor **`PORT`** and bind **`::`** for [Railway private networking](https://docs.railway.com/private-networking).
