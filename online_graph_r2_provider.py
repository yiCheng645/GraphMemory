"""Durable wire receipts; bounded critical-call retries; optional graph has no retries."""
from contextlib import closing
import base64
import hashlib
import json
import time
import urllib.error
import urllib.request

from memory_service import ApiError, digest, encode
from online_graph_providers_v3 import JsonProvider, _NoRedirect
from online_graph_r2_deadline import REQUEST_DEADLINE, GRAPH_WINDOW
from online_graph_providers_v3 import CallJournal
from contextlib import contextmanager

TRANSIENT = {408, 425, 429, 500, 502, 503, 504, 524}
TRANSPORT = {"TimeoutError", "URLError", "RemoteDisconnected", "SSLError", "SSLEOFError"}



def effective_deadline(kind):
    until = REQUEST_DEADLINE.get()
    if kind in ("extract", "identity") and GRAPH_WINDOW.get():
        GRAPH_WINDOW.get().check()
        graph_until = GRAPH_WINDOW.get().until
        until = min(until, graph_until) if until is not None else graph_until
    return until if until is not None else time.monotonic() + 30


def check_until(until):
    remaining = until - time.monotonic()
    if remaining <= 0:
        raise ApiError(503, "request time budget exhausted before provider dispatch")
    return remaining


@contextmanager
def dispatch_gate(lock, until):
    if not lock.acquire(timeout=check_until(until)):
        raise ApiError(503, "provider queue time budget exhausted")
    try:
        check_until(until)
        yield
    finally:
        lock.release()


class BoundedCallJournal(CallJournal):
    def call(self, provider, scope, kind, endpoint, body, output_tokens, invoke):
        until = effective_deadline(kind)
        with dispatch_gate(self.lock, until):
            def dispatch():
                check_until(until)
                return invoke()
            return super().call(provider, scope, kind, endpoint, body, output_tokens, dispatch)


def read_wire(response, until, limit, timeout_cap=30):
    # HTTPResponse.read1 permits a fresh absolute-deadline check per socket read.
    if not hasattr(response, "read1"):
        raw = response.read(limit + 1)
        check_until(until)
        return raw
    pieces, size = [], 0
    while size <= limit:
        remaining = check_until(until)
        sock = getattr(getattr(getattr(response, "fp", None), "raw", None), "_sock", None)
        if sock is not None:
            sock.settimeout(min(remaining, timeout_cap))
        chunk = response.read1(min(65536, limit + 1 - size))
        check_until(until)
        if not chunk:
            break
        pieces.append(chunk)
        size += len(chunk)
    return b"".join(pieces)


class OnlineProvider(JsonProvider):
    def post(self, scope, kind, body, output_tokens=0):
        until = effective_deadline(kind)
        with dispatch_gate(self.journal.lock, until):
            result = self._post_locked(scope, kind, body, output_tokens)
            check_until(until)
            return result

    def _post_locked(self, scope, kind, body, output_tokens=0):
        if len(encode(body).encode("utf-8")) > self.config.get("max_input_bytes", 1048576):
            raise ApiError(413, "provider payload exceeds declared capacity")
        graph = kind in ("extract", "identity")
        maximum = 1  # Preserve original online R2 no-resend semantics.
        for number in range(maximum):
            # Separate metered attempts, deterministic across platform retries/restarts.
            attempt_scope = digest([scope, kind, self.endpoint, body, number])
            key = digest([self.name, attempt_scope, kind, self.endpoint, body])
            with closing(self.journal.connect()) as db:
                saved = db.execute("select state,error from calls where id=?", (key,)).fetchone()
            if saved and saved[0] != "received":
                # Never resend an inflight/failed attempt; retain its full reserve.
                if saved[0] == "failed" and saved[1] not in TRANSPORT:
                    raise ApiError(503, "prior provider failure requires review")
                continue
            until = REQUEST_DEADLINE.get()
            if graph and GRAPH_WINDOW.get():
                GRAPH_WINDOW.get().check()
                until = min(until if until is not None else float("inf"), GRAPH_WINDOW.get().until)
            remaining = (until - time.monotonic()) if until is not None else 30
            if remaining <= 0:
                raise ApiError(503, "request time budget exhausted")
            timeout = min(self.config.get("timeout_seconds", 30), remaining)

            def invoke():
                req = urllib.request.Request(self.endpoint, data=encode(body).encode("utf-8"),
                    headers={"Authorization": "Bearer " + self.key, "Content-Type": "application/json"})
                started = time.monotonic()
                try:
                    response = urllib.request.build_opener(_NoRedirect()).open(req, timeout=timeout)
                except urllib.error.HTTPError as exc:
                    response = exc
                with response:
                    status = response.status
                    raw = read_wire(response, effective_deadline(kind), 4 * 1024 * 1024, timeout)
                if len(raw) > 4 * 1024 * 1024 or self.key.encode() in raw:
                    raise ValueError("unsafe or oversized provider response")
                return {"http_status": status, "raw_b64": base64.b64encode(raw).decode("ascii"),
                        "raw_sha256": hashlib.sha256(raw).hexdigest(),
                        "seconds": round(time.monotonic() - started, 6)}
            try:
                envelope = self.journal.call(self.name, attempt_scope, kind, self.endpoint,
                                             body, output_tokens, invoke)
            except ApiError:
                with closing(self.journal.connect()) as db:
                    failed = db.execute("select state,error from calls where id=?", (key,)).fetchone()
                if not failed or failed[1] not in TRANSPORT:
                    raise
                continue
            raw = base64.b64decode(envelope["raw_b64"], validate=True)
            if hashlib.sha256(raw).hexdigest() != envelope["raw_sha256"]:
                raise ApiError(503, "saved provider receipt integrity failure")
            if envelope["http_status"] in TRANSIENT:
                if number + 1 < maximum:
                    time.sleep(min(1, max(0, remaining - timeout)))
                continue
            if not 200 <= envelope["http_status"] < 300:
                raise ApiError(503, "upstream provider rejected this operation")
            try:
                payload = json.loads(raw)
                if not isinstance(payload, dict):
                    raise ValueError("provider payload must be an object")
                return payload
            except (ValueError, UnicodeError):
                continue
        raise ApiError(503, "provider attempts exhausted; all receipts and reserves retained")
