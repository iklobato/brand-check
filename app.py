"""
Composition root. Re-exports the Celery instance as app.celery so
`python -m celery -A app worker` resolves (and importing app registers the
tasks); imports serve; runs serve() under __main__ so `python app.py` still
boots the web server.
"""

from server import serve
from tasks import celery

__all__ = ["celery", "serve"]

if __name__ == "__main__":
    serve()
