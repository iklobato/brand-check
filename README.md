# GotchaPed

Brand availability checker: domain (RDAP across 50+ TLDs), INPI trademarks (all sources),
and social-handle availability (maigret-style, 50+ networks). Single stdlib `app.py`;
data lives in managed Postgres (`pg_trgm`). Deploys as a Docker container.

Run: `DATABASE_URL=postgres://... python app.py` (serves on `$PORT`, default 8080).
