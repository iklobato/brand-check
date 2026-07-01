"""
Celery app instance (exposed so `-A app` resolves via app.celery) + the three
name-pinned tasks + Valkey job orchestration (redis client with
socket_keepalive + health_check_interval, _new_job, _publish, enqueue_*,
durable-cache persist, pub/sub pump).

Owns the shared websocket broadcast registry so the pump and the server Handler
share it without an import cycle.
"""

import json
import threading
import time
import uuid

import redis
from celery import Celery, group

from checks import (
    _SOCIAL_VERDICT,
    SOCIAL_SITES,
    TLDS,
    _domain_info,
    _social_probe,
    site_check,
)
from config import settings
from models import CACHE_DOMAINS, CACHE_SITES, CACHE_SOCIAL, cache
from text import _ws_text_frame

JOB_CHANNEL = settings.job_channel
JOB_TTL = settings.job_ttl
DATABASE_URL = settings.database_url

celery = Celery(
    "brandcheck",
    broker=settings.celery_broker_url or None,
    backend=settings.celery_result_backend or None,
)
celery.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,  # fair dispatch: slow HTTP tasks don't head-of-line block
    broker_connection_retry_on_startup=True,
    result_expires=JOB_TTL,
    # managed Valkey drops idle connections; keepalive stops that surfacing as ConnectionError
    broker_transport_options={"socket_keepalive": True},
    result_backend_transport_options={"socket_keepalive": True},
)

# health_check_interval: redis-py pings a connection idle >30s and transparently reconnects,
# so the pump/_publish never hit "Connection closed by server" on managed Valkey's idle timeout.
_rds = (
    redis.from_url(
        settings.valkey_url,
        decode_responses=True,
        socket_keepalive=True,
        health_check_interval=30,
    )
    if settings.valkey_url
    else None
)

# Open /ws/jobs browser sockets; the pub/sub pump fans every job event out to them,
# so every web instance's sidebar sees jobs from every worker/consumer.
_ws_jobs_clients: set = set()
_ws_jobs_lock = threading.Lock()


# --------------------------------------------------------------------------- #
# Celery tasks: each wraps a pure check and publishes its result + live progress
# to Valkey (pub/sub for the browser, a shared counter for cross-worker done/total).
# --------------------------------------------------------------------------- #
def _publish(job_id: str, kind: str, name: str, total: int, unit: str, result) -> None:
    if not _rds:
        return
    done = _rds.incr(f"job:{job_id}:done")
    _rds.expire(f"job:{job_id}:done", JOB_TTL)
    # durable result list: pub/sub is lossy, so the browser reconciles from this on completion
    _rds.rpush(f"job:{job_id}:results", json.dumps(result))
    _rds.expire(f"job:{job_id}:results", JOB_TTL)
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


def _new_job(prefix: str, brand: str, kind: str) -> str:
    jid = f"{prefix}:{uuid.uuid4().hex[:12]}"
    if _rds:
        _rds.set(f"job:{jid}:done", 0, ex=JOB_TTL)
        _rds.hset(f"job:{jid}:info", mapping={"brand": brand, "kind": kind})
        _rds.expire(f"job:{jid}:info", JOB_TTL)
    return jid


def enqueue_domains(name: str) -> str | None:
    if not _rds:
        return None
    jid = _new_job("dom", name, CACHE_DOMAINS)
    group(domain_task.s(name, t, jid, len(TLDS)) for t in TLDS).apply_async(
        queue="checks"
    )
    return jid


def enqueue_social(username: str) -> str | None:
    if not _rds or not username:
        return None
    jid = _new_job("soc", username, CACHE_SOCIAL)
    group(
        social_task.s(username, s, jid, len(SOCIAL_SITES)) for s in SOCIAL_SITES
    ).apply_async(queue="checks")
    return jid


def enqueue_sites(domains, brand: str = "") -> str | None:
    domains = list(domains)[:80]
    if not _rds or not domains:
        return None
    jid = _new_job("site", brand, CACHE_SITES)
    group(site_task.s(d, jid, len(domains)) for d in domains).apply_async(
        queue="checks"
    )
    return jid


def _persist_job_cache(event: dict) -> None:
    """On a job's completion event, store its full result list under (brand, kind)."""
    if not (_rds and DATABASE_URL):
        return
    total = event.get("total") or 0
    if not (event.get("done") and total and event["done"] >= total):
        return
    job_id = event["job"]
    rows = [json.loads(x) for x in _rds.lrange(f"job:{job_id}:results", 0, -1)]
    if len(rows) < total:
        return  # tail race: a couple results not stored yet, skip (re-runs will cache)
    if not _rds.set(f"job:{job_id}:cached", "1", nx=True, ex=JOB_TTL):
        return  # a duplicate completion event already persisted this job
    info = _rds.hgetall(f"job:{job_id}:info")
    if info.get("brand") and info.get("kind"):
        cache.put(info["brand"], info["kind"], rows)


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
                try:
                    _persist_job_cache(json.loads(message["data"]))
                except (ValueError, TypeError, redis.exceptions.RedisError):
                    pass  # cache is best-effort; never break the live broadcast
        except (redis.exceptions.RedisError, OSError):
            time.sleep(2)  # reconnect on broker blip
