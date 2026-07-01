"""
All outbound availability checks: RDAP (IANA bootstrap, url, lookup,
check_domain, registrar/detail), domain verdict + real-time domains_stream,
live-site probe (redirect-chain, curl text, screenshot, site_check + batch),
maigret-style social probing, and the CNPJ registry lookup.

Module aliases pull timeouts/worker-counts from settings so the function bodies
stay byte-identical.
"""

import functools
import json
import re
import socket
import threading
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from config import settings
from text import normalize

RDAP_TIMEOUT = settings.rdap_timeout
SITE_TIMEOUT = settings.site_timeout
SITE_WORKERS = settings.site_workers  # thread pool for batch site checks
SITE_RETRIES = settings.site_retries  # extra attempts on a transient failure
SOCIAL_TIMEOUT = settings.social_timeout
SOCIAL_WORKERS = settings.social_workers
DOMAIN_WORKERS = settings.domain_workers
EXPIRY_SOON_DAYS = (
    settings.expiry_soon_days
)  # registered but dropping within = "expiring"
TLDS = settings.tlds

_BROWSER_UA = "Mozilla/5.0 (compatible; inpi-app/1.0)"

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
            print(f"RDAP bootstrap: {len(_rdap_servers)} TLDs mapped", flush=True)
        except (
            urllib.error.URLError,
            TimeoutError,
            OSError,
            ValueError,
            KeyError,
        ) as exc:
            print(
                f"RDAP bootstrap load failed ({exc}); using {RDAP_FALLBACK}", flush=True
            )
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
    no_retry = False
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
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, (TimeoutError, socket.timeout, socket.gaierror)):
                no_retry = True  # unresponsive or unresolvable (available domain): retry won't help
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
    return "timeout" if no_retry else None


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


# --------------------------------------------------------------------------- #
# social handle availability (maigret-style: per-site status_code / message check)
# --------------------------------------------------------------------------- #


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


def _domain_info(name: str, tld: str) -> dict:
    info = check_domain(name, tld)
    info["verdict"], info["level"] = _domain_verdict(
        info["available"], info.get("expires")
    )
    return info


def domains_stream(name: str):
    """Yield each TLD's verdict the instant its worker finishes: a 32-consumer
    ThreadPoolExecutor drained with as_completed, so results arrive in real time."""
    from concurrent.futures import as_completed

    with ThreadPoolExecutor(max_workers=min(DOMAIN_WORKERS, len(TLDS))) as pool:
        futures = [pool.submit(_domain_info, name, tld) for tld in TLDS]
        for future in as_completed(futures):
            yield future.result()
