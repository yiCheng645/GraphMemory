"""Portable serving helpers; no dependency on an evaluation directory or .env."""
import hashlib
import os
from pathlib import Path

from http_service import make_server

ROOT = Path(__file__).resolve().parent
CACHE_FILE = "fb374d419588a4632f3f557e76b4b70aebbca790"
CACHE_SHA256 = "446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d"


def encoder():
    # Fail locally instead of unexpectedly downloading at first live Search.
    cache = ROOT / "data/tokenizer-cache"
    if hashlib.sha256((cache / CACHE_FILE).read_bytes()).hexdigest() != CACHE_SHA256:
        raise ValueError("verified o200k_base tokenizer cache required")
    os.environ["TIKTOKEN_CACHE_DIR"] = str(cache)
    import tiktoken
    return tiktoken.get_encoding("o200k_base")


def create_server(router, api_key, port=8005):
    server = make_server(router, port=port, api_key=api_key)
    # Accept the advertised concurrent burst; model dispatch remains capped at 16.
    server.request_queue_size = 64
    server.socket.listen(server.request_queue_size)
    original = server.RequestHandlerClass

    class Handler(original):
        def do_GET(self):
            if self.path == "/healthz":
                self.respond(200, router.health())
            else:
                super().do_GET()
    server.RequestHandlerClass = Handler
    return server


def load_env(path):
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                key, separator, value = line.partition("=")
                if not separator:
                    raise ValueError("Every .env setting must have KEY=value format")
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
