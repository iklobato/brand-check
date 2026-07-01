"""
Dependency-free root: the frozen Settings dataclass built from env, its
module-level singleton `settings`, and the shared INPI schema/table-name
constants. Imported by every other module; imports nothing itself.
"""

import os
from dataclasses import dataclass

# Domains checked for every search (override with TLDS env var, comma-separated).
# Broad-but-curated: generic + startup + Brazilian. Each is one concurrent RDAP call.
_DEFAULT_TLDS = (
    # generic
    "com",
    "net",
    "org",
    "info",
    "biz",
    "pro",
    "name",
    # startup / tech / brandable
    "io",
    "co",
    "ai",
    "app",
    "dev",
    "me",
    "xyz",
    "online",
    "site",
    "store",
    "shop",
    "tech",
    "cloud",
    "link",
    "live",
    "life",
    "world",
    "group",
    "agency",
    "digital",
    "studio",
    "design",
    "solutions",
    "club",
    "space",
    "website",
    "blog",
    "news",
    "media",
    "tv",
    "cc",
    "vip",
    "global",
    # country / regional
    "us",
    "eu",
    "uk",
    "co.uk",
    "de",
    "fr",
    "es",
    "ca",
    "in",
    # Brazil
    "com.br",
    "net.br",
    "org.br",
    "app.br",
)


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    database_url: str
    fuzzy_sim: float
    celery_broker_url: str
    celery_result_backend: str
    valkey_url: str
    tlds: tuple[str, ...]
    job_channel: str = "jobs"
    job_ttl: int = 3600
    rdap_timeout: int = 6
    site_timeout: int = 6
    site_workers: int = 32
    site_retries: int = 1
    social_workers: int = 32
    social_timeout: int = 6
    domain_workers: int = 32
    max_related_rows: int = 8
    expiry_soon_days: int = 180

    @classmethod
    def from_env(cls) -> "Settings":
        broker = os.environ.get("CELERY_BROKER_URL", "")
        return cls(
            host=os.environ.get("HOST", "127.0.0.1"),
            port=int(os.environ.get("PORT", "8000")),
            database_url=os.environ.get("DATABASE_URL", ""),
            fuzzy_sim=float(os.environ.get("FUZZY_SIM", "0.5")),
            celery_broker_url=broker,
            celery_result_backend=os.environ.get("CELERY_RESULT_BACKEND", broker),
            valkey_url=os.environ.get("VALKEY_URL", broker),
            tlds=tuple(
                t.strip()
                for t in os.environ.get("TLDS", ",".join(_DEFAULT_TLDS)).split(",")
                if t.strip()
            ),
        )


settings = Settings.from_env()

# Real INPI dados-abertos schema (verified from the CSV headers): comma-delimited,
# UTF-8, every file keyed by codigo_interno. See dicionario_marcas.odt.
MAIN_TABLE = "marcas"
NORM_COL = "nome_norm"
DELIMITER = ","
JOIN_COL = "codigo_interno"  # links every table to the mark
NAME_COL = "elemento_nominativo"  # the mark's text (in marcas)
STATUS_COL = "descricao_situacao"  # the mark's current status (in marcas)
NICE_CLASS_COL = "classe_nice"  # in nice, used for class filtering

ENRICH_TABLES = (
    "nice",
    "nacionais",
    "viena",
    "depositantes",
    "despachos",
    "prioridades",
)
