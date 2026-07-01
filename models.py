"""
Managed-Postgres data layer. A lazy, thread-safe module-level connection pool
wrapped by two thin state-holding seams: MarcaRepository (db_ready + the pg_trgm
INPI search/enrichment `check`) and ResultCache (search_cache ensure/get/put).

psycopg imports stay lazy inside methods exactly as in the original.
"""

import threading

from config import (
    ENRICH_TABLES,
    JOIN_COL,
    MAIN_TABLE,
    NICE_CLASS_COL,
    NORM_COL,
    settings,
)
from text import _q, normalize

CACHE_DOMAINS, CACHE_SOCIAL, CACHE_SITES = "domains", "social", "sites"

_pool_holder: dict = {"pool": None}
_pool_lock = threading.Lock()


def _pool():
    if _pool_holder["pool"] is None:
        with _pool_lock:
            if _pool_holder["pool"] is None:
                from psycopg.rows import dict_row
                from psycopg_pool import ConnectionPool

                _pool_holder["pool"] = ConnectionPool(
                    settings.database_url,
                    min_size=1,
                    max_size=8,
                    open=True,
                    kwargs={"row_factory": dict_row},
                )
    return _pool_holder["pool"]


class MarcaRepository:
    """INPI marca search over managed Postgres (pg_trgm)."""

    def __init__(self, database_url: str, fuzzy_sim: float, max_related_rows: int):
        self._database_url = database_url
        self._fuzzy_sim = fuzzy_sim  # pg_trgm similarity floor
        self._max_related_rows = max_related_rows

    def db_ready(self) -> bool:
        if not self._database_url:
            return False
        try:
            with _pool().connection() as db:
                db.execute("SELECT 1 FROM marcas LIMIT 1")
            return True
        except Exception:  # pool/DB not reachable yet
            return False

    def check(
        self,
        name: str,
        ncl: str | None,
        fuzzy: bool = True,
        exact: bool = False,
    ) -> list[dict]:
        """Matching marks enriched from every source. Default: exact + substring + pg_trgm
        fuzzy. exact=True narrows to only nome_norm == normalized(input) (case/accent/space
        insensitive). One indexed query + one batched query per related table.
        """
        query = normalize(name)
        if not query:
            return []
        params: dict = {"q": query, "like": f"%{query}%"}
        conds = [f"{_q(NORM_COL)} = %(q)s"]  # exact normalized match is always included
        if not exact:
            conds.append(f"{_q(NORM_COL)} LIKE %(like)s")  # substring
            if fuzzy:
                conds.append(
                    f"{_q(NORM_COL)} %% %(q)s"
                )  # trigram similarity, GIN-accelerated
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
                (str(self._fuzzy_sim),),
            )
            rows = db.execute(sql, params).fetchall()
            cids = [r[JOIN_COL] for r in rows]
            related_by_cid: dict = {}
            if cids:
                for table in ENRICH_TABLES:
                    for er in db.execute(
                        f"SELECT * FROM {_q(table)} WHERE {_q(JOIN_COL)} = ANY(%s)",
                        (cids,),
                    ).fetchall():
                        related_by_cid.setdefault(er[JOIN_COL], {}).setdefault(
                            table, []
                        ).append(er)

        results = []
        for r in rows:
            related = {
                t: (er[: self._max_related_rows], len(er) > self._max_related_rows)
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


class ResultCache:
    """Durable result cache: same brand searched twice -> serve stored results,
    skip re-running the workers. Keyed by (name, kind) where kind is
    domains|social|sites."""

    def __init__(self, database_url: str):
        self._database_url = database_url

    def ensure(self) -> None:
        with _pool().connection() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS search_cache ("
                "name TEXT, kind TEXT, payload JSONB, updated TIMESTAMPTZ DEFAULT now(), "
                "PRIMARY KEY (name, kind))"
            )

    def get(self, name: str, kind: str):
        """Stored results for this brand+kind, or None on miss/DB error (falls back to live)."""
        import psycopg

        if not (self._database_url and name):
            return None
        try:
            with _pool().connection() as db:
                row = db.execute(
                    "SELECT payload FROM search_cache WHERE name = %s AND kind = %s",
                    (name, kind),
                ).fetchone()
            return row["payload"] if row else None
        except psycopg.Error:
            return None

    def put(self, name: str, kind: str, payload) -> None:
        import psycopg
        from psycopg.types.json import Json

        if not (self._database_url and name and payload):
            return
        try:
            with _pool().connection() as db:
                db.execute(
                    "INSERT INTO search_cache (name, kind, payload, updated) "
                    "VALUES (%s, %s, %s, now()) "
                    "ON CONFLICT (name, kind) DO UPDATE SET "
                    "payload = EXCLUDED.payload, updated = now()",
                    (name, kind, Json(payload)),
                )
        except psycopg.Error as exc:
            print(f"cache_put failed for {name}/{kind}: {exc}")


repo = MarcaRepository(
    settings.database_url, settings.fuzzy_sim, settings.max_related_rows
)
cache = ResultCache(settings.database_url)
