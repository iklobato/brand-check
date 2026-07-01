"""
INPI marca + domain availability search, as a small local web app.

Run it, open the page, type a name. Everything (data download, ingest into
SQLite, trademark search across all INPI sources, and domain check) lives here.

  python app.py            # serves http://127.0.0.1:8000

Data source: https://dadosabertos.inpi.gov.br/index/marcas/  (CSV, refreshed ~daily)
Fuzzy look-alike matching needs `rapidfuzz` (pip install rapidfuzz); without it
search degrades to exact + substring. This is a first-pass screen, not legal clearance.
"""

import base64
import csv
import functools
import glob
import hashlib
import http.client
import json
import math
import os
import re
import socket
import sqlite3
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import redis
from celery import Celery, group

csv.field_size_limit(1 << 24)

HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "8000"))
DB_PATH = os.environ.get("INPI_DB", "inpi.db")
DATA_DIR = os.environ.get("INPI_DIR", "inpi_csv")

# --------------------------------------------------------------------------- #
# Celery + Valkey: background checks run on the celery-worker component of the
# same App Platform app; live job progress flows over Valkey pub/sub -> /ws/jobs.
# --------------------------------------------------------------------------- #
CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "")
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)
VALKEY_URL = os.environ.get("VALKEY_URL", CELERY_BROKER_URL)  # pub/sub + job counters
JOB_CHANNEL = "jobs"
JOB_TTL = 3600

celery = Celery(
    "brandcheck",
    broker=CELERY_BROKER_URL or None,
    backend=CELERY_RESULT_BACKEND or None,
)
celery.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,  # fair dispatch: slow HTTP tasks don't head-of-line block
    broker_connection_retry_on_startup=True,
    result_expires=JOB_TTL,
)

_rds = redis.from_url(VALKEY_URL, decode_responses=True) if VALKEY_URL else None

BASE_URL = "https://dadosabertos.inpi.gov.br/download/marcas/"
CORE_FILES = ("MARCAS_DADOS_BIBLIOGRAFICOS.csv", "MARCAS_CLASSIFICACOES_NICE.csv")
ALL_FILES = CORE_FILES + (
    "MARCAS_CLASSIFICACOES_NACIONAIS.csv",
    "MARCAS_CLASSIFICACOES_VIENA.csv",
    "MARCAS_DEPOSITANTES.csv",
    "MARCAS_DESPACHOS.csv",
    "MARCAS_PRIORIDADES.csv",
)

BATCH = 50_000
CHUNK = 1 << 20
DOWNLOAD_TIMEOUT = 120
DOWNLOAD_RETRIES = 6
RDAP_TIMEOUT = (
    6  # per-registry; a slow one returns "unknown" rather than dragging the search
)
MAIN_TABLE = "marcas"
NORM_COL = "nome_norm"
MAX_RELATED_ROWS = 8
DEFAULT_THRESHOLD = 82
FUZZY_LIMIT = 50
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
TLDS = tuple(
    t.strip()
    for t in os.environ.get("TLDS", ",".join(_DEFAULT_TLDS)).split(",")
    if t.strip()
)

# Real INPI dados-abertos schema (verified from the CSV headers): comma-delimited,
# UTF-8, every file keyed by codigo_interno. See dicionario_marcas.odt.
DELIMITER = ","
JOIN_COL = "codigo_interno"  # links every table to the mark
NAME_COL = "elemento_nominativo"  # the mark's text (in marcas)
STATUS_COL = "descricao_situacao"  # the mark's current status (in marcas)
NICE_CLASS_COL = "classe_nice"  # in nice, used for class filtering

# file_kw matches the CSV filename; table is the SQLite table it lands in.
DATASETS = [
    {"file_kw": "BIBLIOGRAFICOS", "table": MAIN_TABLE},
    {"file_kw": "NICE", "table": "nice"},
    {"file_kw": "NACIONAIS", "table": "nacionais"},
    {"file_kw": "VIENA", "table": "viena"},
    {"file_kw": "DEPOSITANTES", "table": "depositantes"},
    {"file_kw": "DESPACHOS", "table": "despachos"},
    {"file_kw": "PRIORIDADES", "table": "prioridades"},
]

_build = {"running": False, "lines": []}


def log(message: str) -> None:
    _build["lines"].append(message)
    print(message, flush=True)


# --------------------------------------------------------------------------- #
# normalize / schema helpers
# --------------------------------------------------------------------------- #
def normalize(name: str) -> str:
    """Lowercase, strip accents, drop non-alphanumerics for fuzzy-ish matching."""
    stripped = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", stripped.lower())


_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"


def trigrams(norm: str, pad: bool) -> list[str]:
    """Distinct 3-grams of `norm`. pad=True wraps in '$' so endpoint edits stay visible
    (fuzzy); pad=False keeps interior-only grams (substring prefilter)."""
    text = f"${norm}$" if pad else norm
    return (
        list({text[i : i + 3] for i in range(len(text) - 2)}) if len(text) >= 3 else []
    )


def edit1_neighbors(norm: str) -> set[str]:
    """Damerau-Levenshtein distance-1 strings over the nome_norm alphabet.
    Used only for len<=3 queries, where padded trigrams can be disjoint."""
    out = {norm}
    for i in range(len(norm)):
        out.add(norm[:i] + norm[i + 1 :])  # deletion
        for ch in _ALPHABET:
            out.add(norm[:i] + ch + norm[i + 1 :])  # substitution
    for i in range(len(norm) + 1):
        for ch in _ALPHABET:
            out.add(norm[:i] + ch + norm[i:])  # insertion
    for i in range(len(norm) - 1):
        out.add(norm[:i] + norm[i + 1] + norm[i] + norm[i + 2 :])  # transposition
    return out


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', "") + '"'


def _sanitize(column: str) -> str:
    cleaned = re.sub(r"\W+", "_", column.strip().lower()).strip("_")
    return cleaned or "col"


def _sanitize_header(header: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    result = []
    for column in header:
        base = _sanitize(column)
        if base in seen:
            seen[base] += 1
            base = f"{base}_{seen[base]}"
        else:
            seen[base] = 1
        result.append(base)
    return result


def _open_csv(path: str):
    """INPI dados-abertos CSVs are comma-delimited UTF-8 (latin-1 fallback)."""
    for encoding in ("utf-8", "latin-1"):
        try:
            handle = open(path, encoding=encoding, newline="")
            handle.readline()
            handle.seek(0)
            return csv.reader(handle, delimiter=DELIMITER)
        except UnicodeDecodeError:
            handle.close()
    raise ValueError(f"Could not decode {path} as utf-8 or latin-1")


# --------------------------------------------------------------------------- #
# download + ingest (triggered from the UI)
# --------------------------------------------------------------------------- #
_RETRYABLE = (
    urllib.error.URLError,
    TimeoutError,
    ConnectionError,
    http.client.IncompleteRead,
)


def _download_file(url: str, dest: str, force: bool) -> None:
    """Download with HTTP Range resume + size verification (multi-GB files drop mid-stream)."""
    name = os.path.basename(dest)
    if os.path.exists(dest) and os.path.getsize(dest) > 0 and not force:
        log(f"skip (complete): {name}")
        return
    part = dest + ".part"
    if force and os.path.exists(part):
        os.remove(part)

    total = None
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        have = os.path.getsize(part) if os.path.exists(part) else 0
        headers = {"User-Agent": "inpi-app/1.0"}
        if have:
            headers["Range"] = f"bytes={have}-"
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers=headers), timeout=DOWNLOAD_TIMEOUT
            ) as response:
                resuming = response.status == 206  # server honored Range
                content_length = int(response.headers.get("Content-Length", 0)) or None
                total = (
                    (have + content_length)
                    if (resuming and content_length)
                    else content_length
                )
                done = have if resuming else 0
                next_mark = (done // (200 << 20) + 1) * (200 << 20)
                log(f"downloading {name} {'(resume)' if resuming else ''}...")
                with open(part, "ab" if resuming else "wb") as out:
                    while True:
                        chunk = response.read(CHUNK)
                        if not chunk:
                            break
                        out.write(chunk)
                        done += len(chunk)
                        if done >= next_mark:
                            log(f"  {name}: {done / 1e6:.0f} MB")
                            next_mark += 200 << 20
            if total is None or os.path.getsize(part) >= total:
                break
            log(
                f"  {name}: connection closed at {done / 1e6:.0f} MB, resuming ({attempt}/{DOWNLOAD_RETRIES})"
            )
        except _RETRYABLE as exc:
            log(f"  {name}: {exc} - retry {attempt}/{DOWNLOAD_RETRIES}")
    else:
        raise RuntimeError(f"{name}: failed after {DOWNLOAD_RETRIES} attempts")

    got = os.path.getsize(part)
    if total is not None and got < total:
        raise RuntimeError(f"{name}: incomplete {got} < {total} bytes")
    os.replace(part, dest)
    log(f"{name}: done ({got / 1e6:.0f} MB)")


def download(dest_dir: str, all_files: bool, force: bool) -> None:
    os.makedirs(dest_dir, exist_ok=True)
    for name in ALL_FILES if all_files else CORE_FILES:
        _download_file(BASE_URL + name, os.path.join(dest_dir, name), force)


def _tune_for_bulk_load(db: sqlite3.Connection) -> None:
    # ponytail: journal/synchronous OFF is safe; ingest is a full rebuild from CSVs.
    for pragma in (
        "journal_mode=OFF",
        "synchronous=OFF",
        "temp_store=MEMORY",
        "cache_size=-200000",
    ):
        db.execute(f"PRAGMA {pragma}")


def _bulk_insert(db: sqlite3.Connection, statement: str, rows) -> int:
    total, batch = 0, []
    for record in rows:
        batch.append(record)
        if len(batch) >= BATCH:
            db.executemany(statement, batch)
            total += len(batch)
            batch.clear()
    if batch:
        db.executemany(statement, batch)
        total += len(batch)
    return total


def _ingest_one(
    db: sqlite3.Connection, path: str, spec: dict
) -> tuple[int, str, str | None]:
    table = spec["table"]
    reader = _open_csv(path)
    header = next(reader)
    columns = _sanitize_header(header)
    width = len(columns)

    if JOIN_COL not in columns:
        raise KeyError(f"{JOIN_COL} missing in {os.path.basename(path)}: {columns}")
    proc_col = JOIN_COL
    is_main = table == MAIN_TABLE
    name_idx = columns.index(NAME_COL) if is_main else None
    key_col = NORM_COL if is_main else (NICE_CLASS_COL if table == "nice" else None)

    insert_cols = [_q(c) for c in columns]
    if is_main:
        insert_cols.append(_q(NORM_COL))
    db.execute(f"DROP TABLE IF EXISTS {_q(table)}")
    db.execute(
        f"CREATE TABLE {_q(table)} ({', '.join(c + ' TEXT' for c in insert_cols)})"
    )
    placeholders = ",".join("?" * len(insert_cols))
    statement = (
        f"INSERT INTO {_q(table)} ({', '.join(insert_cols)}) VALUES ({placeholders})"
    )

    def rows():
        for row in reader:
            padded = (row + [""] * width)[:width]
            yield (*padded, normalize(padded[name_idx])) if is_main else tuple(padded)

    count = _bulk_insert(db, statement, rows())
    db.commit()
    return count, proc_col, key_col


def _build_name_index(db: sqlite3.Connection) -> None:
    """Padded-trigram inverted index over DISTINCT mark names. Powers substring
    (interior grams + instr verify) and fuzzy look-alike (shared grams -> rapidfuzz
    rerank), replacing the in-memory rapidfuzz cache. Idempotent: drops and rebuilds."""
    log("building name index (vocab + trigrams) ...")
    db.execute("DROP TABLE IF EXISTS name_gram")
    db.execute("DROP TABLE IF EXISTS name_vocab")
    db.execute(
        "CREATE TABLE name_vocab (norm_id INTEGER PRIMARY KEY, nome_norm TEXT NOT NULL, len INTEGER NOT NULL)"
    )
    db.execute(
        f"INSERT INTO name_vocab(nome_norm, len) "
        f"SELECT DISTINCT {_q(NORM_COL)}, length({_q(NORM_COL)}) FROM {_q(MAIN_TABLE)} "
        f"WHERE {_q(NORM_COL)} IS NOT NULL AND {_q(NORM_COL)} <> ''"
    )
    db.execute("CREATE TABLE name_gram (gram TEXT NOT NULL, norm_id INTEGER NOT NULL)")

    reader = db.cursor()  # separate cursor: stream vocab while inserting grams
    reader.execute("SELECT norm_id, nome_norm FROM name_vocab")

    def gram_rows():
        for norm_id, norm in reader:
            for gram in trigrams(norm, pad=True):
                yield (gram, norm_id)

    grams = _bulk_insert(
        db, "INSERT INTO name_gram(gram, norm_id) VALUES (?, ?)", gram_rows()
    )
    vocab = db.execute("SELECT count(*) FROM name_vocab").fetchone()[0]
    log(f"  {vocab:,} distinct names, {grams:,} trigram postings; indexing ...")
    db.execute("CREATE UNIQUE INDEX idx_name_vocab_norm ON name_vocab(nome_norm)")
    db.execute("CREATE INDEX idx_name_gram ON name_gram(gram, norm_id)")
    db.commit()


def build_name_index_on(db_path: str) -> None:
    """Backfill the trigram index onto an existing DB in place (no re-download)."""
    db = sqlite3.connect(db_path)
    _tune_for_bulk_load(db)
    _build_name_index(db)
    db.close()
    log("name index ready")


def ingest(db_path: str, source_dir: str) -> None:
    files = {
        os.path.basename(p).upper(): p
        for p in glob.glob(os.path.join(source_dir, "*.csv"))
    }
    db = sqlite3.connect(db_path)
    _tune_for_bulk_load(db)
    db.execute("DROP TABLE IF EXISTS _meta")
    db.execute("CREATE TABLE _meta (tbl TEXT, proc_col TEXT, key_col TEXT)")

    ingested = []
    for spec in DATASETS:
        match = next((p for name, p in files.items() if spec["file_kw"] in name), None)
        if not match:
            continue
        log(f"loading {os.path.basename(match)} ...")
        count, proc_col, key_col = _ingest_one(db, match, spec)
        db.execute(
            "INSERT INTO _meta VALUES (?,?,?)", (spec["table"], proc_col, key_col)
        )
        db.execute(
            f"CREATE INDEX {_q('idx_' + spec['table'] + '_proc')} "
            f"ON {_q(spec['table'])} ({_q(proc_col)})"
        )
        log(f"  {count:,} rows")
        ingested.append(spec["table"])

    if MAIN_TABLE not in ingested:
        db.close()
        raise RuntimeError("MARCAS_DADOS_BIBLIOGRAFICOS.csv not found")

    _build_name_index(db)
    # idx_nome_norm is created LAST: it is the db_ready() sentinel, so it must not
    # exist until the name index is also built.
    db.execute(f"CREATE INDEX idx_nome_norm ON {_q(MAIN_TABLE)} ({_q(NORM_COL)})")
    db.commit()
    db.close()
    log(f"done: {', '.join(ingested)}")


def _build_job(all_files: bool) -> None:
    _build["running"] = True
    _build["lines"] = []
    try:
        download(DATA_DIR, all_files, force=False)
        ingest(DB_PATH, DATA_DIR)
        log("Database ready.")
    except Exception as exc:  # surfaced to the UI status panel
        log(f"ERROR: {exc}")
    finally:
        _build["running"] = False


def start_build(all_files: bool) -> None:
    if not _build["running"]:
        threading.Thread(target=_build_job, args=(all_files,), daemon=True).start()


# --------------------------------------------------------------------------- #
# search
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# data layer: managed Postgres (pg_trgm). Replaces the local-SQLite search engine;
# the trigram tables (name_vocab/name_gram) and the download/ingest path are gone.
# --------------------------------------------------------------------------- #
DATABASE_URL = os.environ.get("DATABASE_URL", "")
FUZZY_SIM = float(
    os.environ.get("FUZZY_SIM", "0.5")
)  # pg_trgm similarity floor (~matches the old rapidfuzz-82 result counts)
ENRICH_TABLES = (
    "nice",
    "nacionais",
    "viena",
    "depositantes",
    "despachos",
    "prioridades",
)
_pool_holder: dict = {"pool": None}
_pool_lock = threading.Lock()


def _pool():
    if _pool_holder["pool"] is None:
        with _pool_lock:
            if _pool_holder["pool"] is None:
                from psycopg.rows import dict_row
                from psycopg_pool import ConnectionPool

                _pool_holder["pool"] = ConnectionPool(
                    DATABASE_URL,
                    min_size=1,
                    max_size=8,
                    open=True,
                    kwargs={"row_factory": dict_row},
                )
    return _pool_holder["pool"]


def db_ready() -> bool:
    if not DATABASE_URL:
        return False
    try:
        with _pool().connection() as db:
            db.execute("SELECT 1 FROM marcas LIMIT 1")
        return True
    except Exception:  # pool/DB not reachable yet
        return False


def _load_meta(db: sqlite3.Connection) -> dict[str, tuple[str, str | None]]:
    return {
        row[0]: (row[1], row[2])
        for row in db.execute("SELECT tbl, proc_col, key_col FROM _meta")
    }


def _class_members(db, ncl: str) -> set:
    """codigo_interno values registered in one NICE class."""
    return {
        row[0]
        for row in db.execute(
            f"SELECT {JOIN_COL} FROM nice WHERE {NICE_CLASS_COL} = ?", (ncl,)
        )
    }


def _substring_norms(db, query: str) -> list[str]:
    """Distinct mark names containing `query`: interior-trigram prefilter + instr verify."""
    grams = trigrams(query, pad=False)
    if not grams:  # query shorter than 3 chars: rare, fall back to a scan
        return [
            r[0]
            for r in db.execute(
                "SELECT nome_norm FROM name_vocab WHERE instr(nome_norm, ?) > 0",
                (query,),
            )
        ]
    placeholders = ",".join("?" * len(grams))
    rows = db.execute(
        f"SELECT v.nome_norm FROM name_gram g JOIN name_vocab v ON v.norm_id = g.norm_id "
        f"WHERE g.gram IN ({placeholders}) GROUP BY g.norm_id HAVING count(*) = ?",
        (*grams, len(grams)),
    )
    return [norm for (norm,) in rows if query in norm]


def _length_bounds(length: int, threshold: int) -> tuple[int, int]:
    """Name lengths that can reach fuzz.ratio >= threshold vs a query of this length.
    ratio = 2*M/(L1+L2) >= r is a hard necessary condition, so this prunes candidates
    without dropping any real match (recall-safe)."""
    r = threshold / 100
    return math.floor(r * length / (2 - r)), math.ceil(length * (2 - r) / r)


def _fuzzy_candidate_norms(db, query: str, threshold: int) -> list[str]:
    """Distinct names that could be look-alikes of `query` (rapidfuzz reranks after).
    K=1 (any shared padded trigram) keeps full recall; the length filter is the prune.
    """
    if len(query) >= 4:
        grams = trigrams(query, pad=True)
        if not grams:
            return []
        lo, hi = _length_bounds(len(query), threshold)
        placeholders = ",".join("?" * len(grams))
        rows = db.execute(
            f"SELECT DISTINCT v.nome_norm FROM name_gram g JOIN name_vocab v "
            f"ON v.norm_id = g.norm_id WHERE g.gram IN ({placeholders}) "
            f"AND v.len BETWEEN ? AND ?",
            (*grams, lo, hi),
        )
        return [norm for (norm,) in rows]
    # len<=3: padded trigrams can be disjoint, so enumerate the edit-1 neighborhood.
    neighbors = tuple(edit1_neighbors(query))
    placeholders = ",".join("?" * len(neighbors))
    rows = db.execute(
        f"SELECT nome_norm FROM name_vocab WHERE nome_norm IN ({placeholders})",
        neighbors,
    )
    return [norm for (norm,) in rows]


def _matched_norms(db, query: str, fuzzy: bool, threshold: int) -> dict[str, int]:
    """nome_norm -> score: 100 for exact/substring, fuzz.ratio for look-alikes."""
    scored = {query: 100}  # exact; contributes nothing later if no such mark exists
    for norm in _substring_norms(db, query):
        scored[norm] = 100
    if fuzzy:
        candidates = _fuzzy_candidate_norms(db, query, threshold)
        if candidates:
            try:
                from rapidfuzz import fuzz, process

                for norm, score, _ in process.extract(
                    query,
                    candidates,
                    scorer=fuzz.ratio,
                    score_cutoff=threshold,
                    limit=None,
                ):
                    if scored.get(norm, -1) < score:
                        scored[norm] = int(score)
            except ImportError:
                pass
    return scored


def _scored_candidates(db, query: str, fuzzy: bool, threshold: int) -> dict[str, int]:
    """codigo_interno -> score. Expands matched names to every mark row sharing that name."""
    scored: dict[str, int] = {}
    for norm, score in _matched_norms(db, query, fuzzy, threshold).items():
        for (cid,) in db.execute(
            f"SELECT {_q(JOIN_COL)} FROM {_q(MAIN_TABLE)} WHERE {_q(NORM_COL)} = ?",
            (norm,),
        ):
            if scored.get(cid, -1) < score:
                scored[cid] = score
    return scored


def _mark_row(db, main_proc, processo) -> dict:
    row = db.execute(
        f"SELECT * FROM {_q(MAIN_TABLE)} WHERE {_q(main_proc)} = ? LIMIT 1", (processo,)
    ).fetchone()
    return dict(row)


def _enrich(db, meta, processo) -> dict:
    related = {}
    for table, (proc_col, _) in meta.items():
        if table == MAIN_TABLE:
            continue
        rows = db.execute(
            f"SELECT * FROM {_q(table)} WHERE {_q(proc_col)} = ? LIMIT {MAX_RELATED_ROWS + 1}",
            (processo,),
        ).fetchall()
        if rows:
            related[table] = (
                [dict(r) for r in rows[:MAX_RELATED_ROWS]],
                len(rows) > MAX_RELATED_ROWS,
            )
    return related


def check(
    name: str, ncl: str | None, fuzzy: bool = True, threshold: int = DEFAULT_THRESHOLD
) -> list[dict]:
    """Matching marks (exact, substring, pg_trgm fuzzy) enriched from every source.
    One indexed search query + one batched query per related table (no per-row round-trips).
    """
    query = normalize(name)
    if not query:
        return []
    params: dict = {"q": query, "like": f"%{query}%"}
    conds = [f"{_q(NORM_COL)} = %(q)s", f"{_q(NORM_COL)} LIKE %(like)s"]
    if fuzzy:
        conds.append(f"{_q(NORM_COL)} %% %(q)s")  # trigram similarity, GIN-accelerated
    where = "(" + " OR ".join(conds) + ")"
    if ncl is not None:
        where += (
            f" AND {_q(JOIN_COL)} IN "
            f"(SELECT {_q(JOIN_COL)} FROM nice WHERE {_q(NICE_CLASS_COL)} = %(ncl)s)"
        )
        params["ncl"] = ncl
    sql = (
        f"SELECT *, CASE WHEN {_q(NORM_COL)} = %(q)s OR {_q(NORM_COL)} LIKE %(like)s "
        f"THEN 100 ELSE round(similarity({_q(NORM_COL)}, %(q)s)*100)::int END AS score "
        f"FROM {_q(MAIN_TABLE)} WHERE {where}"
    )
    with _pool().connection() as db:
        db.execute(
            "SELECT set_config('pg_trgm.similarity_threshold', %s, false)",
            (str(FUZZY_SIM),),
        )
        rows = db.execute(sql, params).fetchall()
        cids = [r[JOIN_COL] for r in rows]
        related_by_cid: dict = {}
        if cids:
            for table in ENRICH_TABLES:
                for er in db.execute(
                    f"SELECT * FROM {_q(table)} WHERE {_q(JOIN_COL)} = ANY(%s)", (cids,)
                ).fetchall():
                    related_by_cid.setdefault(er[JOIN_COL], {}).setdefault(
                        table, []
                    ).append(er)

    results = []
    for r in rows:
        related = {
            t: (er[:MAX_RELATED_ROWS], len(er) > MAX_RELATED_ROWS)
            for t, er in related_by_cid.get(r[JOIN_COL], {}).items()
        }
        results.append(
            {
                "mark": r,
                "processo": r[JOIN_COL],
                "score": r["score"],
                "related": related,
            }
        )
    results.sort(key=lambda hit: hit["score"], reverse=True)
    return results


_rdap_servers: dict[str, str] = {}
_rdap_servers_lock = threading.Lock()
RDAP_BOOTSTRAP_URL = "https://data.iana.org/rdap/dns.json"
RDAP_FALLBACK = "https://rdap.org"


def _load_rdap_servers() -> dict[str, str]:
    """IANA bootstrap: TLD -> its authoritative RDAP base. Fetched once, cached.
    Spreads lookups across each registry instead of hammering one shared bootstrap."""
    if _rdap_servers:
        return _rdap_servers
    with _rdap_servers_lock:
        if _rdap_servers:
            return _rdap_servers
        try:
            with urllib.request.urlopen(RDAP_BOOTSTRAP_URL, timeout=15) as response:
                services = json.load(response).get("services", [])
            for tlds, urls in ((e[0], e[1]) for e in services):
                base = urls[0].rstrip("/")
                for tld in tlds:
                    _rdap_servers[tld] = base
            log(f"RDAP bootstrap: {len(_rdap_servers)} TLDs mapped")
        except (
            urllib.error.URLError,
            TimeoutError,
            OSError,
            ValueError,
            KeyError,
        ) as exc:
            log(f"RDAP bootstrap load failed ({exc}); using {RDAP_FALLBACK}")
        return _rdap_servers


def _rdap_url(domain: str) -> str:
    tld = domain.rsplit(".", 1)[
        -1
    ]  # registries are keyed by the last label (br -> registro.br)
    base = _load_rdap_servers().get(tld, RDAP_FALLBACK)
    return f"{base}/domain/{urllib.parse.quote(domain)}"


def _rdap_lookup(domain: str) -> dict:
    """Authoritative RDAP lookup. available True=404, False=registered (200), None=no answer.
    Parses the expiry date from the same response (no extra request)."""
    request = urllib.request.Request(
        _rdap_url(domain),
        headers={"User-Agent": "inpi-app/1.0", "Accept": "application/rdap+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=RDAP_TIMEOUT) as response:
            data = json.load(response)
    except urllib.error.HTTPError as exc:
        return {"available": True if exc.code == 404 else None, "expires": None}
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return {"available": None, "expires": None}
    events = {e.get("eventAction"): e.get("eventDate") for e in data.get("events", [])}
    return {"available": False, "expires": events.get("expiration")}


def check_domain(name: str, tld: str) -> dict:
    """Availability for one TLD via authoritative RDAP, falling back to a DNS probe."""
    domain = f"{normalize(name)}.{tld}"
    look = _rdap_lookup(domain)
    if look["available"] is not None:
        detail = "available (RDAP)" if look["available"] else "registered (RDAP)"
        return {
            "domain": domain,
            "available": look["available"],
            "expires": look["expires"],
            "detail": detail,
        }
    try:  # no RDAP answer (TLD without RDAP, or rate-limited): DNS can only prove "taken"
        socket.getaddrinfo(domain, None)
        return {
            "domain": domain,
            "available": False,
            "expires": None,
            "detail": "resolves in DNS",
        }
    except socket.gaierror:
        return {
            "domain": domain,
            "available": None,
            "expires": None,
            "detail": "no RDAP/DNS answer",
        }


SITE_TIMEOUT = 6
SITE_WORKERS = 32  # thread pool for batch site checks (the dot statuses)
SITE_RETRIES = 1  # extra attempts on a transient connection failure
_BROWSER_UA = "Mozilla/5.0 (compatible; inpi-app/1.0)"


def _screenshot_url(url: str) -> str:
    # WordPress mShots: free, no account/token (thum.io's free tier is paywalled).
    return f"https://s.wordpress.com/mshots/v1/{urllib.parse.quote(url, safe='')}?w=900"


class _ChainRedirect(urllib.request.HTTPRedirectHandler):
    """Records every redirect hop so we can show the full curl -L chain."""

    def __init__(self) -> None:
        self.chain: list[dict] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self.chain.append({"status": code, "from": req.full_url, "to": newurl})
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _curl_text(
    start_url: str, chain: list[dict], final_status, final_url: str, title: str
) -> str:
    lines = [f"$ curl -IL {start_url}"]
    for hop in chain:
        lines.append(f"  {hop['status']} {hop['from']}")
        lines.append(f"       -> Location: {hop['to']}")
    lines.append(f"  {final_status} {final_url}")
    if title:
        lines.append(f"       <title> {title}")
    return "\n".join(lines)


def _probe_site(domain: str) -> dict | str | None:
    """One probe: https then http, following redirects. Returns a result dict, the
    string 'timeout' (unresponsive host: don't retry), or None (transient: retry)."""
    timed_out = False
    for scheme in ("https", "http"):
        start = f"{scheme}://{domain}"
        handler = _ChainRedirect()
        opener = urllib.request.build_opener(handler)
        request = urllib.request.Request(start, headers={"User-Agent": _BROWSER_UA})
        try:
            with opener.open(request, timeout=SITE_TIMEOUT) as response:
                body = response.read(40000).decode("utf-8", "ignore")
                status, final_url = response.status, response.url
        except urllib.error.HTTPError as exc:
            status, final_url, body = (
                exc.code,
                start,
                "",
            )  # 4xx/5xx is a definitive answer
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if isinstance(getattr(exc, "reason", exc), (TimeoutError, socket.timeout)):
                timed_out = True  # unresponsive; retrying just burns the timeout again
            continue
        match = re.search(r"<title[^>]*>(.*?)</title>", body, re.IGNORECASE | re.DOTALL)
        title = match.group(1).strip()[:140] if match else ""
        return {
            "domain": domain,
            "ok": status == 200,
            "status": status,
            "finalUrl": final_url,
            "chain": handler.chain,
            "title": title,
            "curl": _curl_text(start, handler.chain, status, final_url, title),
            "screenshot": _screenshot_url(final_url),
        }
    return "timeout" if timed_out else None


def site_check(domain: str) -> dict:
    """Live-site check; retry only on transient (non-timeout) connection failures."""
    for _ in range(SITE_RETRIES + 1):
        result = _probe_site(domain)
        if isinstance(result, dict):
            return result
        if result == "timeout":
            break  # unresponsive host: don't retry
    return {
        "domain": domain,
        "ok": False,
        "status": None,
        "finalUrl": None,
        "chain": [],
        "title": "",
        "curl": f"$ curl -IL https://{domain}\n  (no response after {SITE_RETRIES + 1} tries)",
        "screenshot": None,
    }


def site_check_batch(domains: list[str]) -> dict:
    """Run many site checks at once on a fixed thread pool (the dots fill fast)."""
    if not domains:
        return {}
    with ThreadPoolExecutor(max_workers=min(SITE_WORKERS, len(domains))) as pool:
        return dict(zip(domains, pool.map(site_check, domains)))


# --------------------------------------------------------------------------- #
# social handle availability (maigret-style: per-site status_code / message check)
# --------------------------------------------------------------------------- #
SOCIAL_WORKERS = 32
SOCIAL_TIMEOUT = 6


# check: "status" -> 200 taken / 404 free; "absent" -> "not found" text present = free;
# "present" -> marker text present = taken. Mirrors maigret's errorType status_code/message.
def _s(name, url, check="status", **kw):
    return {"name": name, "url": url, "check": check, **kw}


SOCIAL_SITES = [
    _s("GitHub", "https://github.com/{}"),
    _s("GitLab", "https://gitlab.com/{}"),
    _s("Bitbucket", "https://bitbucket.org/{}/"),
    _s("Instagram", "https://www.instagram.com/{}/"),
    _s("TikTok", "https://www.tiktok.com/@{}"),
    _s("YouTube", "https://www.youtube.com/@{}"),
    _s("X", "https://x.com/{}"),
    _s("Facebook", "https://www.facebook.com/{}"),
    _s("Pinterest", "https://www.pinterest.com/{}/"),
    _s("Snapchat", "https://www.snapchat.com/add/{}"),
    _s("LinkedIn", "https://www.linkedin.com/in/{}"),
    _s(
        "Reddit",
        "https://www.reddit.com/user/{}/",
        "absent",
        absent="nobody on Reddit goes by that name",
    ),
    _s("Telegram", "https://t.me/{}", "present", present="tgme_page_title"),
    _s("Threads", "https://www.threads.net/@{}"),
    _s("Mastodon", "https://mastodon.social/@{}"),
    _s("Bluesky", "https://bsky.app/profile/{}.bsky.social"),
    _s("SoundCloud", "https://soundcloud.com/{}"),
    _s("Spotify", "https://open.spotify.com/user/{}"),
    _s("Bandcamp", "https://{}.bandcamp.com"),
    _s("Mixcloud", "https://www.mixcloud.com/{}/"),
    _s("Last.fm", "https://www.last.fm/user/{}"),
    _s("Vimeo", "https://vimeo.com/{}"),
    _s("Dailymotion", "https://www.dailymotion.com/{}"),
    _s("Twitch", "https://m.twitch.tv/{}"),
    _s("Dev.to", "https://dev.to/{}"),
    _s("Medium", "https://medium.com/@{}"),
    _s("Dribbble", "https://dribbble.com/{}"),
    _s("Behance", "https://www.behance.net/{}"),
    _s("DeviantArt", "https://www.deviantart.com/{}"),
    _s("500px", "https://500px.com/p/{}"),
    _s("Flickr", "https://www.flickr.com/people/{}"),
    _s("Imgur", "https://imgur.com/user/{}"),
    _s("Tumblr", "https://{}.tumblr.com"),
    _s("Patreon", "https://www.patreon.com/{}"),
    _s("Ko-fi", "https://ko-fi.com/{}"),
    _s("Buy Me a Coffee", "https://www.buymeacoffee.com/{}"),
    _s("Linktree", "https://linktr.ee/{}"),
    _s("Gumroad", "https://{}.gumroad.com"),
    _s("Fiverr", "https://www.fiverr.com/{}"),
    _s("Product Hunt", "https://www.producthunt.com/@{}"),
    _s("Keybase", "https://keybase.io/{}"),
    _s("Kaggle", "https://www.kaggle.com/{}"),
    _s("Replit", "https://replit.com/@{}"),
    _s("CodePen", "https://codepen.io/{}"),
    _s("npm", "https://www.npmjs.com/~{}"),
    _s("PyPI", "https://pypi.org/user/{}/"),
    _s("Docker Hub", "https://hub.docker.com/u/{}"),
    _s(
        "Steam",
        "https://steamcommunity.com/id/{}",
        "present",
        present="g_rgProfileData",
    ),
    _s(
        "Hacker News",
        "https://news.ycombinator.com/user?id={}",
        "absent",
        absent="No such user.",
    ),
    _s("Goodreads", "https://www.goodreads.com/{}"),
    _s("Wattpad", "https://www.wattpad.com/user/{}"),
    _s("Chess.com", "https://www.chess.com/member/{}"),
    _s("VK", "https://vk.com/{}"),
    _s("Quora", "https://www.quora.com/profile/{}"),
    _s("About.me", "https://about.me/{}"),
    _s("Gravatar", "https://en.gravatar.com/{}"),
    _s("Etsy", "https://www.etsy.com/shop/{}"),
]


def _social_verdict(site: dict, status: int, body: str) -> bool | None:
    """True = handle taken, False = free, None = couldn't tell (blocked/ambiguous)."""
    if status in (404, 410):
        return False
    check = site["check"]
    if check == "status":
        return True if status == 200 else None
    if check == "absent":  # maigret "message": the not-found text -> handle is free
        return site["absent"] not in body
    if check == "present":  # a profile-only marker -> handle is taken
        return site["present"] in body
    return None


def _social_probe(username: str, site: dict) -> dict:
    url = site["url"].format(username)
    request = urllib.request.Request(url, headers={"User-Agent": _BROWSER_UA})
    try:
        with urllib.request.urlopen(request, timeout=SOCIAL_TIMEOUT) as response:
            status = response.status
            body = (
                response.read(60000).decode("utf-8", "ignore")
                if site["check"] != "status"
                else ""
            )
    except urllib.error.HTTPError as exc:
        status, body = exc.code, ""
    except (urllib.error.URLError, TimeoutError, OSError):
        return {"network": site["name"], "url": url, "exists": None}
    return {
        "network": site["name"],
        "url": url,
        "exists": _social_verdict(site, status, body),
    }


_SOCIAL_VERDICT = {
    True: ("taken", "bad"),
    False: ("available", "good"),
    None: ("unknown", "warn"),
}
_SOCIAL_RANK = {"available": 0, "unknown": 1, "taken": 2}


def social_check(username: str) -> list[dict]:
    """maigret-style handle availability across networks; free-first sorted."""
    if not username:
        return []
    with ThreadPoolExecutor(max_workers=min(SOCIAL_WORKERS, len(SOCIAL_SITES))) as pool:
        rows = list(pool.map(functools.partial(_social_probe, username), SOCIAL_SITES))
    for r in rows:
        r["verdict"], r["level"] = _SOCIAL_VERDICT[r["exists"]]
    rows.sort(key=lambda r: _SOCIAL_RANK[r["verdict"]])
    return rows


# --------------------------------------------------------------------------- #
# Celery tasks: each wraps a pure check and publishes its result + live progress
# to Valkey (pub/sub for the browser, a shared counter for cross-worker done/total).
# --------------------------------------------------------------------------- #
def _publish(job_id: str, kind: str, name: str, total: int, unit: str, result) -> None:
    if not _rds:
        return
    done = _rds.incr(f"job:{job_id}:done")
    _rds.expire(f"job:{job_id}:done", JOB_TTL)
    _rds.publish(
        JOB_CHANNEL,
        json.dumps(
            {
                "job": job_id,
                "kind": kind,
                "name": name,
                "unit": unit,
                "done": done,
                "total": total,
                "result": result,
                "ts": time.time(),
            }
        ),
    )
    if (
        done >= total
    ):  # closed job -> keep a resultless summary so a reloading sidebar backfills
        _rds.zadd(
            "jobs:recent",
            {
                json.dumps(
                    {
                        "job": job_id,
                        "kind": kind,
                        "name": name,
                        "done": done,
                        "total": total,
                        "result": None,
                        "ts": time.time(),
                    }
                ): time.time()
            },
        )
        _rds.zremrangebyrank("jobs:recent", 0, -51)


@celery.task(name="domain_task", soft_time_limit=15)
def domain_task(name, tld, job_id, total):
    r = _domain_info(name, tld)
    _publish(job_id, "domains", name, total, tld, r)
    return r


@celery.task(name="social_task", soft_time_limit=15)
def social_task(username, site, job_id, total):
    r = _social_probe(username, site)
    r["verdict"], r["level"] = _SOCIAL_VERDICT[r["exists"]]
    _publish(job_id, "social", username, total, site["name"], r)
    return r


@celery.task(name="site_task", soft_time_limit=20)
def site_task(domain, job_id, total):
    r = site_check(domain)
    _publish(job_id, "sites", domain, total, domain, r)
    return r


def _new_job(prefix: str) -> str:
    jid = f"{prefix}:{uuid.uuid4().hex[:12]}"
    if _rds:
        _rds.set(f"job:{jid}:done", 0, ex=JOB_TTL)
    return jid


def enqueue_domains(name: str) -> str | None:
    if not _rds:
        return None
    jid = _new_job("dom")
    group(domain_task.s(name, t, jid, len(TLDS)) for t in TLDS).apply_async(
        queue="checks"
    )
    return jid


def enqueue_social(username: str) -> str | None:
    if not _rds or not username:
        return None
    jid = _new_job("soc")
    group(
        social_task.s(username, s, jid, len(SOCIAL_SITES)) for s in SOCIAL_SITES
    ).apply_async(queue="checks")
    return jid


def enqueue_sites(domains) -> str | None:
    domains = list(domains)[:80]
    if not _rds or not domains:
        return None
    jid = _new_job("site")
    group(site_task.s(d, jid, len(domains)) for d in domains).apply_async(
        queue="checks"
    )
    return jid


def cnpj_detail(cnpj: str) -> dict:
    """Company registry lookup (BrasilAPI). Only CNPJ (14 digits); CPF is private."""
    digits = re.sub(r"\D", "", cnpj or "")
    if len(digits) != 14:
        return {"cnpj": cnpj, "error": "not a company CNPJ"}
    request = urllib.request.Request(
        f"https://brasilapi.com.br/api/cnpj/v1/{digits}",
        headers={"User-Agent": "inpi-app/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            data = json.load(response)
    except urllib.error.HTTPError as exc:
        return {"cnpj": digits, "error": f"lookup failed ({exc.code})"}
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return {"cnpj": digits, "error": "registry unavailable"}
    return {
        "cnpj": digits,
        "razao_social": data.get("razao_social"),
        "nome_fantasia": data.get("nome_fantasia"),
        "situacao": data.get("descricao_situacao_cadastral"),
        "atividade": data.get("cnae_fiscal_descricao"),
        "uf": data.get("uf"),
        "municipio": data.get("municipio"),
        "abertura": data.get("data_inicio_atividade"),
    }


def _rdap_registrar(data: dict) -> str | None:
    """Pull the registrar's display name from the RDAP entity vCard (jCard) array."""
    for entity in data.get("entities", []):
        if "registrar" in entity.get("roles", []):
            for field in entity.get("vcardArray", [None, []])[1]:
                if field and field[0] == "fn":
                    return field[3]
    return None


def domain_detail(domain: str) -> dict:
    """Full RDAP record for one domain (registrar, dates, status, nameservers)."""
    request = urllib.request.Request(
        _rdap_url(domain),
        headers={"User-Agent": "inpi-app/1.0", "Accept": "application/rdap+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=RDAP_TIMEOUT) as response:
            data = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return {"domain": domain, "available": True}
        return {"domain": domain, "available": None, "error": f"RDAP {exc.code}"}
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return {
            "domain": domain,
            "available": None,
            "error": "no authoritative RDAP answer",
        }

    events = {e.get("eventAction"): e.get("eventDate") for e in data.get("events", [])}
    return {
        "domain": domain,
        "available": False,
        "registrar": _rdap_registrar(data),
        "registered": events.get("registration"),
        "expires": events.get("expiration"),
        "updated": events.get("last changed"),
        "status": data.get("status", []),
        "nameservers": [
            ns.get("ldhName") for ns in data.get("nameservers", []) if ns.get("ldhName")
        ],
    }


# --------------------------------------------------------------------------- #
# json api
# --------------------------------------------------------------------------- #
EXPIRY_SOON_DAYS = 180  # a registered domain dropping within this window is "expiring"


def _days_until(iso: str | None) -> int | None:
    try:
        return (date.fromisoformat(str(iso)[:10]) - date.today()).days
    except (ValueError, TypeError):
        return None


def _domain_verdict(available: bool | None, expires: str | None) -> tuple[str, str]:
    """(verdict, level): available / expiring (registered but dropping soon) / taken / unknown."""
    if available is True:
        return "available", "good"
    if available is None:
        return "unknown", "warn"
    days = _days_until(expires)
    if days is not None and days < EXPIRY_SOON_DAYS:
        return "expiring", "warn"
    return "taken", "bad"


# Availability derived from the mark's INPI status (descricao_situacao). A live
# registration blocks the name (taken); extinct/archived/refused frees it (free);
# anything still in process is pending. Keyword match on the Portuguese status text.
_FREE_STATUS = ("extint", "arquiv", "indefer", "nulid", "cancel", "anulad")
_TAKEN_STATUS = ("vigor", "concedid", "registrad")
AVAILABILITY_LEVEL = {"taken": "bad", "pending": "warn", "free": "good"}
AVAILABILITY_RANK = {
    "free": 0,
    "pending": 1,
    "taken": 2,
}  # available (freed) marks first


def _availability(status: str) -> str:
    s = (status or "").lower()
    if any(k in s for k in _TAKEN_STATUS):
        return "taken"
    if any(k in s for k in _FREE_STATUS):
        return "free"
    return "pending"


def _hit_json(hit: dict, target: str) -> dict:
    mark = hit["mark"]
    mark_name = str(mark.get(NAME_COL) or "(figurative / no text element)")
    processo = mark.get("numero_inpi") or hit["processo"]
    score = hit["score"]
    if normalize(mark_name) == target:
        flag, level = "IDENTICAL", "bad"
    elif score >= 100:
        flag, level = "overlap", "warn"
    else:
        flag, level = f"~{score}%", "warn"
    status = mark.get(STATUS_COL) or ""
    availability = _availability(status)
    related = {}
    for table, (rows, truncated) in hit["related"].items():
        items = [
            {k: v for k, v in row.items() if k != JOIN_COL and v not in ("", None)}
            for row in rows
        ]
        related[table] = {"rows": items, "truncated": truncated}
    return {
        "flag": flag,
        "level": level,
        "name": mark_name,
        "processo": processo,
        "status": status,
        "availability": availability,
        "availLevel": AVAILABILITY_LEVEL[availability],
        "deposito": mark.get("data_deposito") or "",
        "concessao": mark.get("data_concessao") or "",
        "vigencia": mark.get("data_vigencia") or "",  # registration validity end
        "related": related,
    }


def _domain_info(name: str, tld: str) -> dict:
    info = check_domain(name, tld)
    info["verdict"], info["level"] = _domain_verdict(
        info["available"], info.get("expires")
    )
    return info


DOMAIN_WORKERS = 32


def search_json(name: str) -> dict:
    """One brand name -> INPI marks. Domains stream separately via /api/domains (real time)."""
    target = normalize(name)
    marks = [_hit_json(hit, target) for hit in check(name, None, fuzzy=True)]
    marks.sort(key=lambda m: AVAILABILITY_RANK[m["availability"]])
    return {"name": name, "marks": marks}


def domains_stream(name: str):
    """Yield each TLD's verdict the instant its worker finishes: a 32-consumer
    ThreadPoolExecutor drained with as_completed, so results arrive in real time."""
    from concurrent.futures import as_completed

    with ThreadPoolExecutor(max_workers=min(DOMAIN_WORKERS, len(TLDS))) as pool:
        futures = [pool.submit(_domain_info, name, tld) for tld in TLDS]
        for future in as_completed(futures):
            yield future.result()


def build_status() -> dict:
    """Filesystem-derived progress so it works regardless of which process downloads."""
    downloading = [
        os.path.basename(p)[:-5]
        for p in sorted(glob.glob(os.path.join(DATA_DIR, "*.part")))
    ]
    files = [
        {"name": os.path.basename(p), "mb": round(os.path.getsize(p) / 1e6)}
        for p in sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    ]
    return {
        "ready": db_ready(),
        "building": _build["running"] or bool(downloading),
        "files": files,
        "downloading": downloading,
        "lines": _build["lines"][-20:],
    }


PAGE = """<!doctype html><html lang="en" data-theme="dark"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Brand check</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/vis-timeline@7.7.3/styles/vis-timeline-graph2d.min.css">
<script src="https://cdn.jsdelivr.net/npm/vis-timeline@7.7.3/standalone/umd/vis-timeline-graph2d.min.js"></script>
<style>
 main{max-width:1180px}
 #jobsbar{position:fixed;top:0;right:0;width:230px;height:100vh;overflow:auto;z-index:50;
   background:var(--pico-card-background-color);border-left:1px solid var(--pico-muted-border-color);
   font-size:.76rem;padding-bottom:1rem}
 #jobsbar:empty{display:none}
 .jb-h{position:sticky;top:0;background:var(--pico-card-sectioning-background-color);
   padding:.4rem .5rem;font-weight:600;border-bottom:1px solid var(--pico-muted-border-color)}
 .jb-row{padding:.3rem .5rem;border-bottom:1px solid var(--pico-muted-border-color)}
 .jb-t{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
 .jb-bar{height:5px;background:var(--pico-muted-border-color);border-radius:3px;margin-top:.25rem;overflow:hidden}
 .jb-fill{height:100%;background:#e67e22;transition:width .2s}
 .jb-fill.done{background:#27ae60}
 @media(max-width:1100px){#jobsbar{display:none}}
 #q{font-size:1rem}
 .badge{color:#fff;padding:.08rem .42rem;border-radius:5px;font-size:.72rem;font-weight:600;white-space:nowrap}
 .muted{color:var(--pico-muted-color)}
 .vtl{margin:.3rem 0;font-size:.78rem}
 .vtl .vis-item{border-color:#9aa0a6;background:var(--pico-card-background-color)}
 .vtl .vis-item.dt-good{background:#dff5e3;border-color:#27ae60}
 .vtl .vis-item.dt-bad{background:#fbe0dd;border-color:#c0392b}
 .vtl .vis-item.dt-opp{background:#fdebd6;border-color:#e67e22}
 .vtl .vis-item.dt-arch{background:#ececec;border-color:#9aa0a6}
 .tl-legend{display:flex;flex-wrap:wrap;gap:.4rem;margin:.35rem 0;font-size:.7rem}
 .sdot{width:.6rem;height:.6rem;border-radius:50%;flex:none;background:#cbd0d4}
 .sdot.loading{background:#cbd0d4;animation:pulse 1s infinite}
 .sdot.on{background:#27ae60}
 .sdot.off{background:#c0392b}
 .sdot.na{background:transparent;border:1px dashed #cbd0d4}
 @keyframes pulse{50%{opacity:.35}}
 .shot{display:block;width:100%;max-width:520px;border:1px solid var(--pico-muted-border-color);border-radius:8px;margin:.4rem 0}
 .curl{font-size:.72rem;background:#1e1e1e;color:#d6d6d6;padding:.5rem .7rem;border-radius:6px;overflow:auto;white-space:pre}
 .hist{margin:.4rem 0;font-size:.82rem}
 .histrows{display:flex;flex-wrap:wrap;gap:.5rem;margin:.4rem 0}
 .histrows label{display:flex;align-items:center;gap:.3rem;cursor:pointer}
 .hname{cursor:pointer;text-decoration:underline dotted;font-weight:600}
 .histbtns{display:flex;gap:.5rem}
 .histbtns button{width:auto;padding:.2rem .7rem;font-size:.78rem;margin:0}
 .cmp2-head{display:flex;align-items:center;gap:.8rem;margin:.3rem 0}
 .cmp2-head h3{margin:0}
 .cmp2-head button,.heathead button{width:auto;padding:.15rem .7rem;font-size:.78rem;margin:0}
 .scores{display:grid;grid-template-columns:repeat(var(--n),1fr);gap:.6rem;margin:.5rem 0}
 .score{padding:.55rem .75rem;border:1px solid var(--pico-muted-border-color);border-radius:10px}
 .score.win{border-color:#27ae60;box-shadow:inset 0 0 0 1px #27ae60;background:rgba(39,174,96,.06)}
 .sname{font-size:1.05rem;margin-bottom:.2rem}
 .sstat{font-size:.82rem;margin-top:.25rem}
 .heathead{display:flex;align-items:center;gap:.8rem;flex-wrap:wrap;margin:.7rem 0 .2rem}
 .heathead h4{margin:0}
 .legend{font-size:.72rem;color:var(--pico-muted-color);display:flex;align-items:center;gap:.3rem;flex-wrap:wrap}
 .heat{display:flex;flex-direction:column;gap:1px;margin:.2rem 0}
 .hrow{display:grid;grid-template-columns:7rem repeat(var(--n),1fr);gap:.4rem;align-items:center;padding:.08rem .2rem;border-radius:4px}
 .hrow.diff{background:var(--pico-code-background-color)}
 .hrow.head .hname{font-weight:600;font-size:.8rem;text-align:center}
 .hlbl{font-size:.78rem;color:var(--pico-muted-color);white-space:nowrap}
 .heat .agree{display:none}
 .heat.all .agree{display:grid}
 .hdot{width:.85rem;height:.85rem;border-radius:50%;display:inline-block;margin:0 auto;cursor:pointer}
 .hdot.good{background:#27ae60}.hdot.warn{background:#e67e22}.hdot.bad{background:#c0392b}
 .hcell{text-align:center;color:var(--pico-muted-color)}
 .tmgrid{display:grid;grid-template-columns:repeat(var(--n),1fr);gap:.6rem;margin:.3rem 0}
 .tmchips{display:flex;flex-direction:column;gap:.2rem;margin-top:.3rem}
 .tmchip{cursor:pointer;font-size:.78rem;border:1px solid var(--pico-muted-border-color);border-radius:5px;padding:.12rem .45rem}
 .tmchip:hover{background:var(--pico-code-background-color)}
 a.scard{text-decoration:none;color:inherit}
 ul.spec{list-style:none;display:flex;flex-wrap:wrap;gap:.25rem;margin:.1rem 0;padding:0}
 ul.spec li{font-size:.74rem;background:var(--pico-code-background-color);border:1px solid var(--pico-muted-border-color);
   border-radius:5px;padding:.06rem .4rem}
 .cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:.3rem;margin-top:.5rem}
 .card{display:flex;align-items:center;gap:.35rem;cursor:pointer;font-size:.78rem;
   padding:.25rem .5rem;border:1px solid var(--pico-muted-border-color);border-left:4px solid #9aa0a6;
   border-radius:6px;white-space:nowrap;overflow:hidden}
 .card:hover{background:var(--pico-code-background-color)}
 .card.taken{border-left-color:#c0392b}
 .card.pending{border-left-color:#e67e22}
 .card.free{border-left-color:#27ae60}
 .card .badge{font-size:.6rem;padding:.04rem .3rem}
 .card b{overflow:hidden;text-overflow:ellipsis}
 .card .cl{font-weight:600;color:var(--pico-muted-color);flex:none}
 .card .flag{margin-left:auto;font-size:.68rem;color:var(--pico-muted-color);flex:none}
 #modal article{max-width:980px;width:94%}
 #modalTitle strong{font-size:1.05rem}
 .msub{display:flex;align-items:center;gap:.5rem;margin:.1rem 0 .7rem;color:var(--pico-muted-color)}
 .facts{display:grid;grid-template-columns:max-content 1fr;gap:.2rem .8rem;margin:0 0 .4rem}
 .facts>dt{font-weight:600;color:var(--pico-muted-color)}
 .facts>dd{margin:0}
 .src{border:1px solid var(--pico-muted-border-color);border-radius:8px;padding:.45rem .7rem;margin:.55rem 0;
   background:var(--pico-card-sectioning-background-color)}
 .src>h6{margin:0 0 .35rem;font-size:.72rem;text-transform:uppercase;letter-spacing:.04em;color:var(--pico-muted-color)}
 .fields{display:grid;grid-template-columns:max-content 1fr;gap:.12rem .7rem;margin:0;font-size:.82rem}
 .fields>dt{font-weight:600;color:var(--pico-muted-color);white-space:nowrap}
 .fields>dd{margin:0;word-break:break-word}
 .fields+.fields{margin-top:.35rem;padding-top:.35rem;border-top:1px dashed var(--pico-muted-border-color)}
 .src .more{font-size:.74rem;color:var(--pico-muted-color);margin:.3rem 0 0}
</style></head><body><main class="container">
<hgroup><h1>Brand check</h1>
<p>Type one brand name. You get domain availability and every matching INPI trademark (all sources, all classes) right here. First-pass only, not legal clearance.</p></hgroup>
<div id="app"><article aria-busy="true">Loading...</article></div>
</main>
<aside id="jobsbar"></aside>
<dialog id="modal"><article>
  <header><button aria-label="Close" rel="prev" id="modalClose"></button><span id="modalTitle"></span></header>
  <div id="modalBody"></div>
</article></dialog>
<script>
const COLORS={bad:'#c0392b',warn:'#e67e22',good:'#27ae60'};
const app=document.getElementById('app');
function esc(s){const d=document.createElement('div');d.textContent=s==null?'':String(s);return d.innerHTML;}
function badge(t,l){return `<span class="badge" style="background:${COLORS[l]}">${esc(t)}</span>`;}

async function tick(){
  const s=await (await fetch('/api/status')).json();
  if(s.ready){showSearch();return;}
  app.innerHTML=setupHtml(s);
  if(s.building)setTimeout(tick,3000);
}
function setupHtml(s){
  const files=s.files.map(f=>`<li>${esc(f.name)} - ${f.mb} MB</li>`).join('');
  const dl=s.downloading.length?`<p>downloading: ${s.downloading.map(esc).join(', ')}...</p>`:'';
  return `<article><h3>Data setup</h3>
   <p>INPI has no live API, so the bulk data (all sources, ~12 GB) is downloaded once.</p>
   <button onclick="build()" ${s.building?'disabled aria-busy="true"':''}>${s.building?'Building...':'Download & build'}</button>
   ${dl}<ul>${files}</ul></article>`;
}
async function build(){await fetch('/build',{method:'POST'});tick();}

function showSearch(){
  app.innerHTML=`
   <form id="f" role="search"><input id="q" name="q" placeholder="brand name (e.g. rafaela)" autocomplete="off" autofocus></form>
   <div id="hist"></div>
   <div id="out"></div>`;
  document.getElementById('f').addEventListener('submit',onSearch);
  renderHistory();
  // delegate clicks once on the stable #out (single-search cards AND the compare view)
  document.getElementById('out').addEventListener('click',ev=>{
    if(ev.target.id==='cmpBack'){runSearch(document.getElementById('q').value.trim()||CMP[0]&&CMP[0].name);return;}
    if(ev.target.id==='tldToggle'){const heat=document.getElementById('heat');
      const on=heat.classList.toggle('all');
      ev.target.textContent=on?'show only differences':('show all '+ev.target.dataset.n);return;}
    const cmp=ev.target.closest('[data-cmp]');                 // compare conflict chip
    if(cmp){const[col,i]=cmp.dataset.cmp.split('.').map(Number);MARKS=CMP[col].marks;openModal(i);return;}
    const dom=ev.target.closest('[data-domain]');              // single card OR heatmap dot
    if(dom){openDomainModal(dom.dataset.domain);return;}
    const card=ev.target.closest('.card[data-i]');             // single-search mark card
    if(card){MARKS=OUT_MARKS;openModal(+card.dataset.i);}
  });
  // history strip: click a name to re-run, tick names + Compare, or Clear
  document.getElementById('hist').addEventListener('click',ev=>{
    if(ev.target.id==='clrBtn'){localStorage.removeItem('bah_history');renderHistory();return;}
    if(ev.target.id==='cmpBtn'){
      const names=[...document.querySelectorAll('#hist input:checked')].map(c=>c.value);
      if(names.length<2||names.length>4){alert('Pick 2 to 4 names to compare');return;}
      openCompare(names);return;}
    const n=ev.target.closest('.hname');
    if(n)runSearch(n.dataset.name);
  });
}
const SITE={};
let OUT_MARKS=[],CMP=[];
async function onSearch(e){e.preventDefault();runSearch(document.getElementById('q').value.trim());}
const CUR={name:'',dom:null,social:null,site:null};
let DOM_ITEMS=[],SOCIAL_ITEMS=[],DOM_TOTAL=0;
const JOBS={},RESULTS={};   // RESULTS[jobId] = [result,...] buffered even before CUR is set (no lost early results)
const SOC_RANK={available:0,unknown:1,taken:2};
async function runSearch(name){
  const out=document.getElementById('out');
  if(!name){out.innerHTML='';return;}
  document.getElementById('q').value=name;
  out.innerHTML='<article aria-busy="true">Searching '+esc(name)+'...</article>';
  const d=await (await fetch('/api/search?name='+encodeURIComponent(name))).json();
  OUT_MARKS=d.marks;
  CUR.name=name; CUR.dom=d.domJob; CUR.social=d.socialJob; CUR.site=null;
  DOM_ITEMS=RESULTS[d.domJob]||[]; SOCIAL_ITEMS=RESULTS[d.socialJob]||[];
  DOM_TOTAL=(JOBS[d.domJob]||{}).total||0;
  out.innerHTML=render(d);
  renderDomains(); renderSocial();   // paint whatever the workers already delivered
  saveHistory(d);renderHistory();
  if(DOM_TOTAL&&DOM_ITEMS.length>=DOM_TOTAL)startSiteChecks();
  if(!d.domJob){const sum=document.getElementById('domsum');if(sum)sum.innerHTML='<span class="muted">workers offline</span>';}
}
// one WebSocket for ALL jobs from ALL workers: drives the sidebar + grid + social + dots
let JOBWS=null;
function openJobs(){
  const proto=location.protocol==='https:'?'wss:':'ws:';
  try{ JOBWS=new WebSocket(proto+'//'+location.host+'/ws/jobs'); }catch(e){ return; }
  JOBWS.onmessage=ev=>{ let m; try{m=JSON.parse(ev.data);}catch(e){return;} onJob(m); };
  JOBWS.onclose=()=>setTimeout(openJobs,2500);   // auto-reconnect
  JOBWS.onerror=()=>{ try{JOBWS.close();}catch(e){} };
}
function onJob(m){
  if(!m||!m.job)return;
  JOBS[m.job]={kind:m.kind,name:m.name,done:m.done,total:m.total,ts:m.ts||Date.now()/1000};
  renderSidebar();
  if(m.result==null)return;   // backfill/summary rows carry no result
  (RESULTS[m.job]=RESULTS[m.job]||[]).push(m.result);   // buffer by job so nothing is lost to timing
  if(m.kind==='domains'&&m.job===CUR.dom){ DOM_TOTAL=m.total; DOM_ITEMS=RESULTS[m.job]; renderDomains();
    if(DOM_ITEMS.length>=m.total)startSiteChecks(); }
  else if(m.kind==='social'&&m.job===CUR.social){ SOCIAL_ITEMS=RESULTS[m.job]; renderSocial(); }
  else if(m.kind==='sites'&&m.job===CUR.site){ paintDot(m.result.domain,m.result); }
}
function renderDomains(){
  const grid=document.getElementById('domcards'), sum=document.getElementById('domsum');
  if(!grid)return;
  const items=[...DOM_ITEMS].sort((a,b)=>DOM_RANK[a.verdict]-DOM_RANK[b.verdict]);
  grid.innerHTML=items.map(domCard).join('');
  if(sum)sum.innerHTML=domSummaryHtml(domCounts(items))+(DOM_ITEMS.length<DOM_TOTAL?' <span aria-busy="true"></span>':'');
  const h=loadHistory(); if(h[0]&&h[0].name===CUR.name){h[0].dom=domCounts(items);try{localStorage.setItem('bah_history',JSON.stringify(h));}catch(e){}}
}
function startSiteChecks(){
  if(CUR.site)return;
  const registered=DOM_ITEMS.filter(x=>x.verdict==='taken'||x.verdict==='expiring').map(x=>x.domain);
  if(!registered.length)return;
  fetch('/api/sites',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({domains:registered})})
    .then(r=>r.json()).then(d=>{CUR.site=d.job;}).catch(()=>{});
}
function renderSocial(){
  const el=document.getElementById('social'); if(!el)return;
  const rows=[...SOCIAL_ITEMS].sort((a,b)=>SOC_RANK[a.verdict]-SOC_RANK[b.verdict]);
  const av=rows.filter(r=>r.verdict==='available').length, tk=rows.filter(r=>r.verdict==='taken').length, un=rows.length-av-tk;
  const cards=rows.map(r=>{
    const cls=r.verdict==='available'?'free':(r.verdict==='taken'?'taken':'pending');
    return `<a class="card scard ${cls}" href="${esc(r.url)}" target="_blank" rel="noopener" title="${esc(r.url)}">${badge(r.verdict.toUpperCase(),r.level)} <b>${esc(r.network)}</b></a>`;
  }).join('');
  el.innerHTML=`<p>${badge(av+' available','good')} ${badge(tk+' taken','bad')} ${un?badge(un+' unknown','warn'):''}</p><div class="cards">${cards}</div>`;
}
function renderSidebar(){
  const el=document.getElementById('jobsbar'); if(!el)return;
  const jobs=Object.entries(JOBS).sort((a,b)=>b[1].ts-a[1].ts).slice(0,40);
  el.innerHTML=`<div class="jb-h">Live jobs${jobs.length?` <small>(${jobs.length})</small>`:''}</div>`
    +(jobs.length?jobs.map(([id,j])=>{
      const pct=Math.round(100*j.done/(j.total||1)), done=j.done>=j.total;
      return `<div class="jb-row"><div class="jb-t">${badge(j.kind,done?'good':'warn')} <b>${esc(j.name)}</b> <small>${j.done}/${j.total}</small></div>`
        +`<div class="jb-bar"><div class="jb-fill${done?' done':''}" style="width:${pct}%"></div></div></div>`;
    }).join(''):`<p class="muted" style="padding:.4rem .5rem">no jobs yet</p>`);
}
// compare needs the full domain list per name (not real-time): a blocking fetch
async function fetchDomains(name){ try{return await (await fetch('/api/domains?name='+encodeURIComponent(name))).json();}catch(e){return [];} }
function loadHistory(){try{return JSON.parse(localStorage.getItem('bah_history'))||[];}catch(e){return[];}}
function saveHistory(d){
  const e=summarize(d);
  const h=loadHistory().filter(x=>x.name!==e.name);
  h.unshift(e);
  try{localStorage.setItem('bah_history',JSON.stringify(h.slice(0,20)));}catch(err){}
}
function renderHistory(){
  const h=loadHistory(),el=document.getElementById('hist');
  if(!el)return;
  if(!h.length){el.innerHTML='';return;}
  el.innerHTML=`<details class="hist"><summary>History (${h.length}) &mdash; tick 2-4 to compare</summary><div class="histrows">`
    +h.map(e=>`<label><input type="checkbox" value="${esc(e.name)}"> `
      +`<span class="hname" data-name="${esc(e.name)}">${esc(e.name)}</span> `
      +`${badge(e.dom.available+' free','good')}${badge((e.mark.taken||0)+' tm','bad')}</label>`).join('')
    +`</div><div class="histbtns"><button id="cmpBtn" class="secondary">Compare</button> `
    +`<button id="clrBtn" class="secondary outline">Clear</button></div></details>`;
}
function paintDot(dom,s){
  SITE[dom]=s;
  for(const dot of document.querySelectorAll('.sdot[data-d="'+dom+'"]')){
    dot.classList.remove('loading');dot.classList.add(s.ok?'on':'off');
    dot.title=s.ok?('live site: '+(s.title||s.finalUrl)):'registered, no website';}
}
function classesOf(m){
  const out=new Set();
  for(const r of (m.related.nice||{rows:[]}).rows) if(r.classe_nice) out.add('NCL '+r.classe_nice);
  for(const r of (m.related.nacionais||{rows:[]}).rows) if(r.classe) out.add('cl '+r.classe);
  return [...out].join(', ');
}
function ownerOf(m){
  const r=(m.related.depositantes||{rows:[]}).rows[0];
  return r&&r.nome?r.nome:'';
}
const AVAIL_LABEL={taken:'TAKEN',pending:'PENDING',free:'FREE'};
let MARKS=[];
const DOM_CLASS={available:'free',expiring:'pending',taken:'taken',unknown:'pending'};
const DOM_RANK={available:0,expiring:1,unknown:2,taken:3};
function domCounts(list){
  const dom={available:0,expiring:0,taken:0,unknown:0};
  for(const x of list||[])dom[x.verdict]=(dom[x.verdict]||0)+1;
  return dom;
}
function summarize(d){
  const mark={free:0,pending:0,taken:0};
  for(const m of d.marks)mark[m.availability]=(mark[m.availability]||0)+1;
  return {name:d.name,t:Date.now(),dom:domCounts(d.domains),mark};
}
function domSummaryHtml(dom){
  return `${badge(dom.available+' available','good')} `
    +`${dom.expiring?badge(dom.expiring+' expiring','warn')+' ':''}${badge(dom.taken+' taken','bad')} `
    +`${dom.unknown?badge(dom.unknown+' unknown','warn'):''}`;
}
function render(d){
  const s=summarize(d);
  let h=`<h3>${esc(d.name)}</h3>`;
  // Domains: streamed in real time (each TLD card appears as its worker finishes)
  h+=`<h4>Domains</h4>`;
  h+=`<p id="domsum"><span aria-busy="true">checking domains...</span></p>`;
  h+=`<div id="domcards" class="cards"></div>`;
  // Social handles (filled in the background, maigret-style)
  h+='<h4>Social handles <small>(maigret-style, best-effort)</small></h4>'
    +'<div id="social"><span class="muted">checking handles &hellip;</span></div>';
  // INPI marks
  h+='<h4>INPI trademark <small>(all classes)</small></h4>';
  if(!d.marks.length)return h+`<p>${badge('CLEAR','good')} No identical, overlapping, or similar mark found.</p>`;
  const sum=`<p>${d.marks.length} marks &middot; ${badge(s.mark.free+' available','good')} `
    +`${badge(s.mark.pending+' pending','warn')} ${badge(s.mark.taken+' in force','bad')}</p>`;
  return h+sum+`<div class="cards">`+d.marks.map(card).join('')+'</div>';
}
function domCard(x){
  const reg=x.verdict==='taken'||x.verdict==='expiring';
  return `<div class="card ${DOM_CLASS[x.verdict]}" data-domain="${esc(x.domain)}" title="click for registration + site snapshot">
    <span class="sdot ${reg?'loading':'na'}" data-d="${esc(x.domain)}"></span>
    ${badge(x.verdict.toUpperCase(),x.level)}
    <b>${esc(x.domain)}</b>
  </div>`;
}
function card(m,i){
  const cls=classesOf(m);
  return `<div class="card ${m.availability}" data-i="${i}" title="click for full detail">
    ${badge(AVAIL_LABEL[m.availability],m.availLevel)}
    <b>${esc(m.name)}</b>
    ${cls?`<small class="cl">${esc(cls)}</small>`:''}
    <span class="flag">${esc(m.flag)}</span>
  </div>`;
}
// ---- side-by-side compare: scorecards + diff heatmap (full-width, in #out) ----
function tldOf(dom){return dom.replace(/^[^.]*/,'');}   // rafaela.com.br -> .com.br
function cmpScore(d){const s=summarize(d);return {name:d.name,free:s.dom.available,expiring:s.dom.expiring,total:d.domains.length,inforce:s.mark.taken};}
function renderCompare(results){
  const n=results.length;
  const cards=results.map(cmpScore);
  // winner: most free domains, then fewest in-force marks
  const best=[...cards].sort((a,b)=>b.free-a.free||a.inforce-b.inforce)[0];
  let h=`<div class="cmp2"><div class="cmp2-head"><button class="secondary outline" id="cmpBack">&larr; Back</button><h3>Comparing ${n}</h3></div>`;
  // scorecards
  h+=`<div class="scores" style="--n:${n}">`+cards.map(c=>`<article class="score${c===best?' win':''}">
    <div class="sname">${c===best?'&#9733; ':''}<b>${esc(c.name)}</b></div>
    ${c===best?badge('BEST PICK','good'):'<span class="muted" style="font-size:.75rem">candidate</span>'}
    <div class="sstat"><b>${c.free}</b>/${c.total} domains free</div>
    ${c.expiring?`<div class="sstat muted">${c.expiring} expiring soon</div>`:''}
    <div class="sstat">${c.inforce?badge(c.inforce+' in-force marks','bad'):badge('no in-force marks','good')}</div>
  </article>`).join('')+`</div>`;
  // domain diff heatmap
  const tlds=[];for(const d of results)for(const x of d.domains){const t=tldOf(x.domain);if(!tlds.includes(t))tlds.push(t);}
  const byTld=results.map(d=>{const m={};for(const x of d.domains)m[tldOf(x.domain)]=x;return m;});
  const rank=t=>Math.min(...byTld.map(m=>m[t]?DOM_RANK[m[t].verdict]:99));
  tlds.sort((a,b)=>rank(a)-rank(b));
  let diffs=0;
  const rows=tlds.map(t=>{
    const cells=byTld.map(m=>m[t]),present=cells.filter(Boolean);
    const agree=present.length===n&&present.every(x=>x.verdict===present[0].verdict);
    if(!agree)diffs++;
    const c=cells.map(x=>x?`<span class="hdot ${x.level}" data-domain="${esc(x.domain)}" title="${esc(x.domain)}: ${esc(x.verdict)}"></span>`:'<span class="hcell muted">-</span>').join('');
    return `<div class="hrow ${agree?'agree':'diff'}"><span class="hlbl">${esc(t)}</span>${c}</div>`;
  }).join('');
  h+=`<div class="heathead"><h4>Domains</h4>`
    +`<button class="secondary outline" id="tldToggle" data-n="${tlds.length}">show all ${tlds.length}</button>`
    +`<span class="legend"><span class="hdot good"></span>free <span class="hdot warn"></span>expiring/unknown <span class="hdot bad"></span>taken</span></div>`;
  h+=`<div class="heat" id="heat" style="--n:${n}"><div class="hrow head"><span class="hlbl"></span>`
    +results.map(d=>`<span class="hname">${esc(d.name)}</span>`).join('')+`</div>`+rows+`</div>`;
  h+=diffs?`<p class="muted">${diffs} TLD${diffs>1?'s':''} differ; the rest agree (hidden).</p>`:`<p class="muted">All TLDs agree across these names.</p>`;
  // in-force trademark conflicts per name
  h+=`<h4>In-force trademark conflicts</h4><div class="tmgrid" style="--n:${n}">`;
  h+=results.map((d,col)=>{
    const hits=d.marks.map((m,i)=>({m,i})).filter(x=>x.m.availability==='taken');
    const top=hits.slice(0,6).map(x=>`<span class="tmchip" data-cmp="${col}.${x.i}">${esc(x.m.name)}<small> ${esc(classesOf(x.m)||'')}</small></span>`).join('');
    return `<div class="tmcol"><b>${esc(d.name)}</b> ${hits.length?badge(hits.length+' in force','bad'):badge('clear','good')}`
      +`<div class="tmchips">${top||'<span class="muted">no in-force conflicts</span>'}${hits.length>6?`<span class="muted">+${hits.length-6} more</span>`:''}</div></div>`;
  }).join('')+`</div>`;
  return h+`</div>`;
}
async function openCompare(names){
  const out=document.getElementById('out');
  out.innerHTML='<article aria-busy="true">Comparing '+names.length+' names...</article>';
  CMP=await Promise.all(names.map(async n=>{
    const [s,domains]=await Promise.all([
      fetch('/api/search?name='+encodeURIComponent(n)).then(r=>r.json()).catch(()=>({marks:[]})),
      fetchDomains(n).catch(()=>[])
    ]);
    return {name:n, marks:s.marks||[], domains};
  }));
  out.innerHTML=renderCompare(CMP);
  window.scrollTo(0,0);
}
const FIELD_LABELS={classe:'National class',classe_nice:'NICE class',primeira_subclasse:'Subclass 1',
  segunda_subclasse:'Subclass 2',terceira_subclasse:'Subclass 3',especificacao:'Specification',
  especificacao_trad:'Specification (EN)',edicao_nice:'NICE edition',simbolo:'Symbol',
  classificacao_viena:'Vienna class',revisao_viena:'Vienna revision',nome:'Name',
  cnpj_cpf_titular:'Tax ID',tipo_pfpj_titular:'Holder type',nome_representante_legal:'Legal rep',
  cnpj_cpf_representante_legal:'Legal rep tax ID',tipo_pfpj_representante_legal:'Legal rep type',
  estado:'State',pais:'Country',numero_rpi:'Gazette no.',data_rpi:'Gazette date',
  codigo_despacho:'Code',descricao_despacho:'Dispatch',complemento_despacho:'Note',
  pais_prioridade:'Priority country',numero_prioridade:'Priority no.',data_prioridade:'Priority date'};
const SOURCE_LABELS={nacionais:'National classification',nice:'NICE classification',
  viena:'Vienna (figurative)',depositantes:'Applicant / owner',despachos:'Dispatch history',
  prioridades:'Priority claim'};
function prettyKey(k){return FIELD_LABELS[k]||k.replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());}
function fieldValue(k,v){
  // Specification fields are long ";"-delimited lists of goods/services -> render as chips
  if(k==='especificacao'||k==='especificacao_trad'){
    const parts=String(v).split(';').map(s=>s.trim()).filter(Boolean);
    if(parts.length>1)return `<ul class="spec">`+parts.map(p=>`<li>${esc(p)}</li>`).join('')+`</ul>`;
  }
  return esc(v);
}
function fields(row){
  const items=Object.entries(row).filter(([k,v])=>k!=='numero_inpi'&&v!=='').map(([k,v])=>
    `<dt>${esc(prettyKey(k))}</dt><dd>${fieldValue(k,v)}</dd>`).join('');
  return `<dl class="fields">${items}</dl>`;
}
const fmtDate=s=>s?String(s).slice(0,10):'';
const NOW=new Date();
function daysUntil(d){const t=new Date(String(d||'').slice(0,10));return isNaN(t)?null:Math.round((t-NOW)/864e5);}
function plural(n){return n===1?'day':'days';}
function expiryBadge(d){const n=daysUntil(d);if(n===null)return '';
  if(n<0)return ' '+badge('EXPIRED','bad')+` <small class="muted">${-n} ${plural(-n)} ago</small>`;
  if(n<180)return ' '+badge('EXPIRES SOON','warn')+` <small class="muted">in ${n} ${plural(n)}</small>`;
  return ` <small class="muted">in ${n} ${plural(n)}</small>`;}
function despachoClass(desc){const s=(desc||'').toLowerCase();
  if(s.includes('oposi'))return 'dt-opp';
  if(s.includes('indefer')||s.includes('nulidade')||s.includes('extin')||s.includes('cancel'))return 'dt-bad';
  if(s.includes('deferid')||s.includes('concedid')||s.includes('concess')||s.includes('prorrog'))return 'dt-good';
  if(s.includes('arquiv'))return 'dt-arch';
  return 'dt-neu';}
function openModal(i){
  const m=MARKS[i],cls=classesOf(m),owner=ownerOf(m);
  document.getElementById('modalTitle').innerHTML=
    `${badge(AVAIL_LABEL[m.availability],m.availLevel)} <strong>${esc(m.name)}</strong>`;
  let h=`<div class="msub">${badge(m.flag,m.level)}<span>${esc(m.status||'-')}</span></div>
    <dl class="facts">
      ${cls?`<dt>Class</dt><dd>${esc(cls)}</dd>`:''}
      ${owner?`<dt>Owner</dt><dd>${esc(owner)} <span id="ownerCompany" class="muted"></span></dd>`:''}
      <dt>Process</dt><dd>${esc(m.processo)}</dd>
      ${m.deposito?`<dt>Deposited</dt><dd>${esc(fmtDate(m.deposito))}</dd>`:''}
      ${m.concessao?`<dt>Granted</dt><dd>${esc(fmtDate(m.concessao))}</dd>`:''}
      ${m.vigencia?`<dt>Valid until</dt><dd>${esc(fmtDate(m.vigencia))}${expiryBadge(m.vigencia)}</dd>`:''}
    </dl>`;
  for(const[table,info]of Object.entries(m.related)){
    const n=info.rows.length;
    const inner = table==='despachos'
      ? `<div id="dispatchTimeline" class="vtl"></div><div class="tl-legend">${badge('grant/renewal','good')} ${badge('opposition','warn')} ${badge('rejection/extinction','bad')}</div>`
      : info.rows.map(fields).join('');
    h+=`<section class="src"><h6>${esc(SOURCE_LABELS[table]||table)}${n>1?` &middot; ${n}`:''}</h6>`
      +inner
      +(info.truncated?`<p class="more">+ more rows (showing first ${n})</p>`:'')
      +`</section>`;
  }
  document.getElementById('modalBody').innerHTML=h;
  document.getElementById('modal').showModal();
  if(m.related.despachos)buildDispatchTimeline(m.related.despachos.rows);
  const dep=(m.related.depositantes||{rows:[]}).rows[0];
  if(dep&&String(dep.cnpj_cpf_titular||'').replace(/\\D/g,'').length===14)fetchCompany(dep.cnpj_cpf_titular);
}
function buildDispatchTimeline(rows){
  const el=document.getElementById('dispatchTimeline');
  if(!el)return;
  const items=rows.filter(r=>r.data_rpi).map((r,i)=>({
    id:i, start:r.data_rpi, className:despachoClass(r.descricao_despacho),
    content:esc(String(r.codigo_despacho||'').trim()||('#'+(i+1))),
    title:`${esc(r.data_rpi)} &middot; ${esc(r.descricao_despacho||'')}`
  })).sort((a,b)=>a.start<b.start?-1:1);
  if(!window.vis||!items.length){
    el.innerHTML=rows.map(r=>`<div class="rel"><b>${esc(r.data_rpi||'?')}</b> ${esc(r.descricao_despacho||r.codigo_despacho||'')}</div>`).join('');
    return;
  }
  new vis.Timeline(el, new vis.DataSet(items), {
    stack:true, height:'210px', margin:{item:10}, zoomMin:1000*60*60*24*30,
    tooltip:{followMouse:true, overflowMethod:'flip'}
  });
}
async function fetchCompany(cnpj){
  const el=document.getElementById('ownerCompany');
  if(!el)return;
  el.textContent='checking company...';
  try{
    const c=await (await fetch('/api/cnpj?cnpj='+encodeURIComponent(cnpj))).json();
    if(c.error){el.textContent='';return;}
    const lvl=c.situacao==='ATIVA'?'good':(c.situacao?'bad':'warn');
    el.innerHTML=badge(c.situacao||'?',lvl)+(c.atividade?` <small>${esc(c.atividade)}</small>`:'');
  }catch(e){el.textContent='';}
}
async function openDomainModal(domain){
  document.getElementById('modalTitle').innerHTML=`<strong>${esc(domain)}</strong>`;
  document.getElementById('modalBody').innerHTML='<p aria-busy="true">Loading registration & site data...</p>';
  document.getElementById('modal').showModal();
  const x=await (await fetch('/api/domain?domain='+encodeURIComponent(domain))).json();
  let titleBadge,body;
  if(x.available===true){
    titleBadge=badge('AVAILABLE','good');
    body=`<p>${badge('AVAILABLE','good')} No registration record found &mdash; this domain appears available to register.</p>`;
  }else if(x.available===false){
    const exp=daysUntil(x.expires)!==null&&daysUntil(x.expires)<180;
    titleBadge=exp?badge('EXPIRING','warn'):badge('TAKEN','bad');
    body=`<div class="msub">${exp?badge('EXPIRING','warn'):badge('REGISTERED','bad')}</div><dl class="facts">`
      +(x.registrar?`<dt>Registrar</dt><dd>${esc(x.registrar)}</dd>`:'')
      +(x.registered?`<dt>Registered</dt><dd>${esc(fmtDate(x.registered))}</dd>`:'')
      +(x.expires?`<dt>Expires</dt><dd>${esc(fmtDate(x.expires))}${expiryBadge(x.expires)}</dd>`:'')
      +(x.updated?`<dt>Updated</dt><dd>${esc(fmtDate(x.updated))}</dd>`:'')
      +(x.status&&x.status.length?`<dt>Status</dt><dd>${esc(x.status.join(', '))}</dd>`:'')
      +(x.nameservers&&x.nameservers.length?`<dt>Nameservers</dt><dd>${x.nameservers.map(esc).join('<br>')}</dd>`:'')
      +`</dl><div id="siteSection"><p class="more" aria-busy="true">checking website...</p></div>`;
  }else{
    titleBadge=badge('UNKNOWN','warn');
    body=`<p>${badge('UNKNOWN','warn')} ${esc(x.error||'No authoritative RDAP answer for this TLD.')}</p>`;
  }
  document.getElementById('modalTitle').innerHTML=`${titleBadge} <strong>${esc(domain)}</strong>`;
  document.getElementById('modalBody').innerHTML=body;
  if(x.available===false)loadSite(domain);
}
async function loadSite(domain){
  const sec=document.getElementById('siteSection');
  if(!sec)return;
  let s=SITE[domain];
  if(!s){try{s=await (await fetch('/api/site?domain='+encodeURIComponent(domain))).json();SITE[domain]=s;}catch(e){sec.innerHTML='';return;}}
  if(document.getElementById('siteSection'))document.getElementById('siteSection').outerHTML=siteHtml(s);
  // mShots returns a placeholder while it renders; refresh the image once it's ready
  if(s.ok&&s.screenshot){
    const img=document.querySelector('#siteSection img.shot');
    if(img)setTimeout(()=>{ if(document.body.contains(img))img.src=s.screenshot+'&r=1'; },4000);
  }
}
function siteHtml(s){
  const dot=s.ok?badge('LIVE SITE','good'):badge('NO WEBSITE','bad');
  let h=`<section class="src" id="siteSection"><h6>Website</h6><p>${dot} `;
  h+=s.ok?`<b><a href="${esc(s.finalUrl)}" target="_blank" rel="noopener">${esc(s.finalUrl)}</a></b> &middot; HTTP ${esc(s.status)}${s.title?` &middot; &ldquo;${esc(s.title)}&rdquo;`:''}</p>`
        :`registered, but no HTTP 200 website responded (parked / no site).</p>`;
  if(s.ok&&s.screenshot)h+=`<img class="shot" loading="lazy" src="${esc(s.screenshot)}" alt="snapshot of ${esc(s.domain)}">`;
  if(s.curl)h+=`<details><summary>raw curl &mdash; follows all redirects</summary><pre class="curl">${esc(s.curl)}</pre></details>`;
  return h+`</section>`;
}
document.getElementById('modalClose').addEventListener('click',()=>document.getElementById('modal').close());
document.getElementById('modal').addEventListener('click',ev=>{ if(ev.target.id==='modal')ev.target.close(); });
document.addEventListener('DOMContentLoaded',()=>{tick();openJobs();});
</script></body></html>"""


# --------------------------------------------------------------------------- #
# server
# --------------------------------------------------------------------------- #
_WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _ws_text_frame(text: str) -> bytes:
    """RFC 6455 server->client text frame (FIN+text, unmasked)."""
    data = text.encode("utf-8")
    n = len(data)
    if n < 126:
        header = bytes([0x81, n])
    elif n < 65536:
        header = bytes([0x81, 126]) + n.to_bytes(2, "big")
    else:
        header = bytes([0x81, 127]) + n.to_bytes(8, "big")
    return header + data


# Open /ws/jobs browser sockets; the pub/sub pump fans every job event out to them,
# so every web instance's sidebar sees jobs from every worker/consumer.
_ws_jobs_clients: set = set()
_ws_jobs_lock = threading.Lock()


def _jobs_pubsub_pump() -> None:
    """Subscribe to the Valkey jobs channel and broadcast each event to all sidebars."""
    if not _rds:
        return
    while True:
        try:
            sub = _rds.pubsub(ignore_subscribe_messages=True)
            sub.subscribe(JOB_CHANNEL)
            for message in sub.listen():
                frame = _ws_text_frame(message["data"])
                with _ws_jobs_lock:
                    for wfile in list(_ws_jobs_clients):
                        try:
                            wfile.write(frame)
                            wfile.flush()
                        except OSError:
                            _ws_jobs_clients.discard(wfile)
        except (redis.exceptions.RedisError, OSError):
            time.sleep(2)  # reconnect on broker blip


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive + WebSocket upgrade

    def _send(
        self, body: str, content_type: str = "text/html", status: int = 200
    ) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_json(self, payload) -> None:
        self._send(json.dumps(payload), "application/json")

    def _read_body(self) -> str:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length).decode("utf-8") if length else ""

    def _ws_handshake(self) -> None:
        key = self.headers.get("Sec-WebSocket-Key", "")
        accept = base64.b64encode(
            hashlib.sha1((key + _WS_MAGIC).encode()).digest()
        ).decode()
        self.send_response(101)
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()

    def _ws_jobs(self) -> None:
        """Live feed of ALL jobs from ALL workers: backfill recent, then stream pub/sub."""
        self._ws_handshake()
        try:
            if _rds:
                for raw in _rds.zrange("jobs:recent", 0, -1):
                    self.wfile.write(_ws_text_frame(raw))
                self.wfile.flush()
        except (OSError, redis.exceptions.RedisError):
            pass
        with _ws_jobs_lock:
            _ws_jobs_clients.add(self.wfile)
        try:
            while self.rfile.read(
                1
            ):  # block until the browser disconnects; pump does the writing
                pass
        except OSError:
            pass
        finally:
            with _ws_jobs_lock:
                _ws_jobs_clients.discard(self.wfile)
            self.close_connection = True

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        if (
            path == "/ws/jobs"
            and "websocket" in self.headers.get("Upgrade", "").lower()
        ):
            self._ws_jobs()
            return
        if path in ("/", ""):
            self._send(PAGE)
        elif path == "/api/status":
            self._send_json(build_status())
        elif path == "/api/search":
            name = query.get("name", [""])[0].strip()
            if not name:
                self._send_json({"name": "", "marks": []})
            else:
                payload = search_json(name)
                payload["domJob"] = enqueue_domains(name)
                payload["socialJob"] = enqueue_social(normalize(name))
                self._send_json(payload)
        elif path == "/api/domains":  # blocking full list (compare view, not real-time)
            name = query.get("name", [""])[0].strip()
            self._send_json(list(domains_stream(name)) if name else [])
        elif path == "/api/domain":
            domain = urllib.parse.parse_qs(parsed.query).get("domain", [""])[0].strip()
            self._send_json(domain_detail(domain) if domain else {"error": "no domain"})
        elif path == "/api/cnpj":
            cnpj = urllib.parse.parse_qs(parsed.query).get("cnpj", [""])[0].strip()
            self._send_json(cnpj_detail(cnpj) if cnpj else {"error": "no cnpj"})
        elif path == "/api/site":
            domain = urllib.parse.parse_qs(parsed.query).get("domain", [""])[0].strip()
            self._send_json(site_check(domain) if domain else {"error": "no domain"})
        elif path == "/api/social":
            name = urllib.parse.parse_qs(parsed.query).get("name", [""])[0].strip()
            self._send_json(social_check(normalize(name)))
        else:
            self._send("not found", status=404)

    def do_POST(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path == "/build":
            start_build(all_files=True)
            self._send_json({"started": True})
        elif path == "/api/sites":
            length = int(self.headers.get("Content-Length", 0))
            domains = json.loads(self.rfile.read(length) or "{}").get("domains", [])[
                :80
            ]
            self._send_json({"job": enqueue_sites([str(d) for d in domains])})
        else:
            self._send("not found", status=404)

    def log_message(self, *args) -> None:  # quiet default access logging
        pass


def serve() -> None:
    if _rds:  # bridge Valkey job events -> browser sidebars
        threading.Thread(target=_jobs_pubsub_pump, daemon=True).start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"INPI search UI on http://{HOST}:{PORT}")
    server.serve_forever()


def _selftest() -> None:
    """In-temp self-check of ingest + enriched + fuzzy search. No network/server."""
    global DB_PATH
    assert normalize("Açmé Café") == "acmecafe"
    assert _availability("Registro de marca em vigor") == "taken"
    assert _availability("Registro Extinto") == "free"
    assert _availability("Pedido Definitivamente Arquivado") == "free"
    assert _availability("Pedido em exame") == "pending"
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:

        def write(fn, header, rows):
            with open(os.path.join(tmp, fn), "w", encoding="utf-8", newline="") as f:
                f.write(header + "\n")
                f.writelines(",".join(r) + "\n" for r in rows)

        write(
            "MARCAS_DADOS_BIBLIOGRAFICOS.csv",
            "codigo_interno,numero_inpi,elemento_nominativo,descricao_situacao",
            [
                ("1", "900000001", "ACME", "Registro em vigor"),
                ("2", "900000002", "ACME PRO", "Pedido"),
                ("3", "900000003", "Zentron", "Registro em vigor"),
                ("4", "900000004", "AKME", "Registro em vigor"),
            ],
        )
        write(
            "MARCAS_CLASSIFICACOES_NICE.csv",
            "codigo_interno,numero_inpi,classe_nice",
            [
                ("1", "900000001", "9"),
                ("2", "900000002", "9"),
                ("3", "900000003", "25"),
                ("4", "900000004", "9"),
            ],
        )
        write(
            "MARCAS_DEPOSITANTES.csv",
            "codigo_interno,numero_inpi,nome",
            [("1", "900000001", "Acme Corp")],
        )
        DB_PATH = os.path.join(tmp, "t.db")
        ingest(DB_PATH, tmp)

        assert db_ready()
        assert len(check("ACME", None, fuzzy=False)) == 2
        assert len(check("ACME", "9", fuzzy=False)) == 2
        assert len(check("ACME", "25", fuzzy=False)) == 0
        assert len(check("Zéntron", None, fuzzy=False)) == 1
        assert "depositantes" in check("ACME", None, fuzzy=False)[0]["related"]
        payload = search_json("ACME")
        acme = next(m for m in payload["marks"] if m["name"] == "ACME")
        assert acme["processo"] == "900000001"  # shows numero_inpi
        assert acme["availability"] == "taken"  # "Registro em vigor"
        ranks = [AVAILABILITY_RANK[m["availability"]] for m in payload["marks"]]
        assert ranks == sorted(ranks)  # available (free) marks sorted first
        assert len(payload["domains"]) == len(TLDS)
        assert "ready" in build_status()

        try:
            import rapidfuzz  # noqa: F401
        except ImportError:
            print("selftest OK (rapidfuzz absent; fuzzy skipped)")
            return
        # AKME (ratio 75) is caught via the padded-trigram index ('me$' shared), not a cache.
        probe = sqlite3.connect(DB_PATH)
        assert probe.execute(
            "SELECT 1 FROM sqlite_master WHERE name='name_gram'"
        ).fetchone(), "trigram index table missing"
        assert "akme" in _fuzzy_candidate_norms(probe, "acme", 70)  # shared 'me$'
        assert "ZZZQWX" not in {  # disjoint, must not be a candidate
            n.upper() for n in _fuzzy_candidate_norms(probe, "acme", 70)
        }
        probe.close()
        # grid batch screening + its RAM cache were removed; single search is the only path
        assert "screen_batch" not in globals()
        assert "warm_grid_cache" not in globals()
        procs = {h["processo"] for h in check("ACME", None, fuzzy=True, threshold=70)}
        assert "4" in procs  # AKME found through the DB path
    print("selftest OK (with fuzzy, trigram index)")


if __name__ == "__main__":
    serve()
