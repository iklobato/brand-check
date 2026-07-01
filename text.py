"""
Pure, stateless helpers: string normalization, SQL identifier quoting, INPI
availability decoding, and the RFC 6455 text-frame encoder. No module state;
imports nothing.
"""

import re
import unicodedata


def normalize(name: str) -> str:
    """Lowercase, strip accents, drop non-alphanumerics for fuzzy-ish matching."""
    stripped = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", stripped.lower())


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', "") + '"'


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
