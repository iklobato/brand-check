"""
HTTP layer: the BaseHTTPRequestHandler subclass (all routes/methods, _send /
_send_json with the BrokenPipe guard, the /ws/jobs handshake+loop), INPI search
response assembly, serve(), and the Jinja env that renders templates/page.html
into the module-level PAGE at import.
"""

import base64
import hashlib
import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import jinja2
import redis

from checks import cnpj_detail, domain_detail, domains_stream, site_check, social_check
from config import JOIN_COL, NAME_COL, STATUS_COL, settings
from models import CACHE_DOMAINS, CACHE_SITES, CACHE_SOCIAL, cache, repo
from tasks import (
    _jobs_pubsub_pump,
    _rds,
    _ws_jobs_clients,
    _ws_jobs_lock,
    enqueue_domains,
    enqueue_sites,
    enqueue_social,
)
from text import (
    AVAILABILITY_LEVEL,
    AVAILABILITY_RANK,
    _availability,
    _ws_text_frame,
    normalize,
)

DATABASE_URL = settings.database_url
HOST = settings.host
PORT = settings.port

_WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# The UI is one static template (zero real template variables). Custom delimiters
# so the inline JS's {{ }} / %} / {# fragments stay inert and render() returns the
# file byte-for-byte.
_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(str(Path(__file__).parent / "templates")),
    variable_start_string="[[",
    variable_end_string="]]",
    block_start_string="[%",
    block_end_string="%]",
    comment_start_string="[#",
    comment_end_string="#]",
    keep_trailing_newline=True,
    autoescape=False,
)
PAGE = _env.get_template("page.html").render()


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


def search_json(name: str, exact: bool = False) -> dict:
    """One brand name -> INPI marks. Domains stream separately via /api/domains (real time)."""
    target = normalize(name)
    marks = [
        _hit_json(hit, target)
        for hit in repo.check(name, None, fuzzy=True, exact=exact)
    ]
    marks.sort(key=lambda m: AVAILABILITY_RANK[m["availability"]])
    return {"name": name, "marks": marks, "exact": exact}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive + WebSocket upgrade

    def _send(
        self, body: str, content_type: str = "text/html", status: int = 200
    ) -> None:
        encoded = body.encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            # client (browser/ingress) closed the connection before we finished; the
            # response can't be delivered and there's nothing to retry. Stop keep-alive.
            self.close_connection = True

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
        elif path == "/api/search":
            name = query.get("name", [""])[0].strip()
            exact = query.get("exact", ["0"])[0] in ("1", "true", "on")
            if not name:
                self._send_json({"name": "", "marks": []})
            else:
                payload = search_json(name, exact=exact)
                dom_cached = cache.get(name, CACHE_DOMAINS)
                soc_cached = cache.get(normalize(name), CACHE_SOCIAL)
                if dom_cached is not None:
                    payload["domCached"] = dom_cached
                else:
                    payload["domJob"] = enqueue_domains(name)
                if soc_cached is not None:
                    payload["socialCached"] = soc_cached
                else:
                    payload["socialJob"] = enqueue_social(normalize(name))
                self._send_json(payload)
        elif (
            path == "/api/job"
        ):  # durable result list for a job (reconcile lost pub/sub msgs)
            jid = query.get("id", [""])[0].strip()
            rows = (
                [json.loads(x) for x in _rds.lrange(f"job:{jid}:results", 0, -1)]
                if (_rds and jid)
                else []
            )
            self._send_json(rows)
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
        if path == "/api/sites":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or "{}")
            domains = [str(d) for d in body.get("domains", [])[:80]]
            brand = (body.get("name") or "").strip()
            cached = cache.get(brand, CACHE_SITES) if brand else None
            if cached is not None:
                self._send_json({"cached": cached})
            else:
                self._send_json({"job": enqueue_sites(domains, brand)})
        else:
            self._send("not found", status=404)

    def log_message(self, *args) -> None:  # quiet default access logging
        pass


def serve() -> None:
    if DATABASE_URL:
        try:
            cache.ensure()
        except (
            Exception
        ) as exc:  # startup best-effort; search still works without cache
            print(f"cache table init failed (will retry lazily): {exc}")
    if _rds:  # bridge Valkey job events -> browser sidebars
        threading.Thread(target=_jobs_pubsub_pump, daemon=True).start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"INPI search UI on http://{HOST}:{PORT}")
    server.serve_forever()
