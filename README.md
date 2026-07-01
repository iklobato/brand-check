# GotchaPed

Brand availability checker: domain (RDAP across 50+ TLDs), INPI trademarks (all
sources), and social-handle availability (maigret-style, 50+ networks). Results
stream in real time over a WebSocket, backed by Celery workers with a live "Live
jobs" sidebar. Data lives in managed Postgres (`pg_trgm`); jobs run on Valkey.

## Layout

Flat modules, one concern each (no web framework: stdlib `http.server`):

| File | Responsibility |
|------|----------------|
| `config.py` | `Settings` dataclass from env + INPI schema constants (dependency-free root) |
| `text.py` | pure helpers: normalize, SQL quoting, availability decode, WebSocket frame |
| `models.py` | Postgres pool + `MarcaRepository` (INPI search) + `ResultCache` (durable cache) |
| `checks.py` | outbound checks: RDAP domains, social handles, live-site probe |
| `tasks.py` | Celery app + tasks + Valkey job orchestration (enqueue/publish/pump) |
| `server.py` | HTTP handler, routing, WebSocket, `serve()` |
| `app.py` | composition root (re-exports `celery`, runs `serve()`) |
| `templates/page.html` | the UI (HTML + JS), rendered via Jinja |

## Run

- Web: `python app.py` (serves on `$PORT`, default 8080)
- Worker: `python -m celery -A app worker --pool=threads --concurrency=32 -Q checks,celery`

## Environment

`DATABASE_URL` (Postgres, required for search), `VALKEY_URL` + `CELERY_BROKER_URL`
+ `CELERY_RESULT_BACKEND` (real-time jobs), `HOST`/`PORT`, optional `TLDS` and
`FUZZY_SIM`. Deploys as a Docker container (see `Dockerfile` / `app.yaml`).
