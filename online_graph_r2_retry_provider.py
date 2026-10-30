"""Durable bounded reconnects; every new wire attempt has its own cost reserve."""
from contextlib import closing
from datetime import timezone
from email.utils import parsedate_to_datetime
import base64
import hashlib
import http.client
import json
import math
import ssl
import threading
import time
import urllib.error
import urllib.request

from memory_service import ApiError, digest, encode
from online_graph_r2_deadline import REQUEST_DEADLINE
from online_graph_r2_provider import (OnlineProvider, BoundedCallJournal,
    effective_deadline, check_until, dispatch_gate, read_wire, _NoRedirect)

RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504, 524}
PERMANENT_CODES = {"insufficient_quota", "billing_hard_limit_reached", "account_deactivated",
                   "invalid_api_key", "content_filter", "content_policy_violation"}


def retry_policy(config):
    maximum = config.get("max_model_attempts", 3)
    delays = config.get("retry_backoff_seconds", [1, 3][:maximum - 1] if type(maximum) is int else [])
    wait_limit = config.get("retry_max_wait_seconds", 60)
    if type(maximum) is not int or not 1 <= maximum <= 3:
        raise ValueError("max_model_attempts must be 1..3 including the first request")
    if not isinstance(delays, list) or len(delays) != maximum - 1 or any(
            type(x) not in (int, float) or not math.isfinite(x) or not 0 <= x <= 60 for x in delays):
        raise ValueError("one bounded backoff per additional model attempt required")
    if type(wait_limit) not in (int, float) or not math.isfinite(wait_limit) or not 0 <= wait_limit <= 60:
        raise ValueError("retry_max_wait_seconds must be 0..60")
    return {"max_model_attempts": maximum, "retry_backoff_seconds": delays,
            "retry_max_wait_seconds": wait_limit}


def retry_after(value):
    if not value:
        return 0.
    try:
        result = float(value)
        if math.isfinite(result):
            return max(0., result)
    except (TypeError, ValueError):
        pass
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return max(0., date.timestamp() - time.time())
    except (TypeError, ValueError, OverflowError):
        return 0.


def transient_exception(exc):
    if isinstance(exc, ssl.SSLCertVerificationError):
        return False
    if isinstance(exc, urllib.error.URLError):
        return not isinstance(exc.reason, ssl.SSLCertVerificationError)
    return isinstance(exc, (TimeoutError, ConnectionError, ssl.SSLError,
        http.client.RemoteDisconnected, http.client.IncompleteRead, ApiError))


class ReconnectJournal(BoundedCallJournal):
    def __init__(self, path):
        super().__init__(path)
        self._logical_guard, self._logical_locks = threading.Lock(), {}
        with closing(self.connect()) as db, db:
            db.executescript('''
CREATE TABLE IF NOT EXISTS reconnect_jobs(logical_id TEXT PRIMARY KEY,policy TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS reconnect_attempts(
 logical_id TEXT,number INTEGER,call_key TEXT NOT NULL,classification TEXT NOT NULL,
 retry_at REAL,PRIMARY KEY(logical_id,number));
''')

    def logical_lock(self, logical):
        with self._logical_guard:
            return self._logical_locks.setdefault(logical, threading.RLock())

    def bind(self, logical, policy, until):
        with dispatch_gate(self.lock, until), closing(self.connect()) as db, db:
            previous = db.execute("select policy from reconnect_jobs where logical_id=?", (logical,)).fetchone()
            if previous and previous[0] != encode(policy):
                raise ApiError(503, "persisted reconnect policy differs; attempt limits cannot reset")
            db.execute("insert or ignore into reconnect_jobs values(?,?)", (logical, encode(policy)))

    def mark(self, logical, number, call_key, classification, retry_at=None):
        with closing(self.connect()) as db, db:
            db.execute('''insert into reconnect_attempts values(?,?,?,?,?)
              on conflict(logical_id,number) do update set classification=excluded.classification,
              retry_at=coalesce(reconnect_attempts.retry_at,excluded.retry_at)''',
              (logical, number, call_key, classification, retry_at))

    def prior(self, key):
        with closing(self.connect()) as db:
            return db.execute("select state,raw_response,error from calls where id=?", (key,)).fetchone()

    def decision(self, logical, number):
        with closing(self.connect()) as db:
            return db.execute("select classification,retry_at from reconnect_attempts where logical_id=? and number=?",
                              (logical, number)).fetchone()


class RetryingOnlineProvider(OnlineProvider):
    def __init__(self, config, journal, name, **kwargs):
        self.reconnect = retry_policy(config)
        if not isinstance(journal, ReconnectJournal):
            raise ValueError("durable reconnect journal required")
        super().__init__(config, journal, name, **kwargs)

    def _wait(self, ready_at, until):
        remaining = check_until(until)
        delay = max(0., ready_at - time.time())
        if delay > self.reconnect["retry_max_wait_seconds"] or delay >= remaining:
            raise ApiError(503, "provider retry cooldown retained; no early retry")
        finish = time.monotonic() + delay
        while True:
            check_until(until)
            left = finish - time.monotonic()
            if left <= 0:
                return
            time.sleep(min(left, .25))

    def _schedule(self, logical, number, key, classification, delay):
        self.journal.mark(logical, number, key, classification, time.time() + delay)
        return self.journal.decision(logical, number)[1]

    def post(self, scope, kind, body, output_tokens=0):
        if len(encode(body).encode("utf-8")) > self.config.get("max_input_bytes", 1048576):
            raise ApiError(413, "provider payload exceeds declared capacity")
        until = effective_deadline(kind)
        token = REQUEST_DEADLINE.set(until)
        logical = digest(["online-r2-bounded-reconnect-v1", self.name, scope, kind, self.endpoint, body])
        try:
            with dispatch_gate(self.journal.logical_lock(logical), until):
                self.journal.bind(logical, self.reconnect, until)
                maximum = self.reconnect["max_model_attempts"]
                for number in range(maximum):
                    check_until(until)
                    attempt_scope = digest([logical, number])
                    key = digest([self.name, attempt_scope, kind, self.endpoint, body])
                    saved = self.journal.prior(key)
                    if saved and saved[0] != "received":
                        # A new numbered attempt may cost again. The old reserve is never removed.
                        decision = self.journal.decision(logical, number)
                        if decision and decision[0] == "permanent":
                            raise ApiError(503, "permanent provider failure requires intervention")
                        if number + 1 < maximum:
                            base = self.reconnect["retry_backoff_seconds"][number]
                            ready = self._schedule(logical, number, key, "unknown_retryable", base)
                            self._wait(ready, until)
                        continue

                    def invoke():
                        self.journal.mark(logical, number, key, "inflight")
                        req = urllib.request.Request(self.endpoint, data=encode(body).encode("utf-8"),
                            headers={"Authorization": "Bearer " + self.key, "Content-Type": "application/json"})
                        started = time.monotonic()
                        try:
                            # Recompute after waiting for the shared dispatch lock.
                            timeout = min(self.config.get("timeout_seconds", 30), check_until(until))
                            try:
                                response = urllib.request.build_opener(_NoRedirect()).open(req, timeout=timeout)
                            except urllib.error.HTTPError as exc:
                                response = exc
                            with response:
                                status = response.status
                                delay = retry_after(getattr(response, "headers", {}).get("Retry-After"))
                                raw = read_wire(response, until, 4 * 1024 * 1024, timeout)
                            if len(raw) > 4 * 1024 * 1024 or self.key.encode() in raw:
                                raise ValueError("unsafe or oversized provider response")
                            return {"http_status": status, "raw_b64": base64.b64encode(raw).decode("ascii"),
                                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                                "seconds": round(time.monotonic() - started, 6), "retry_after_seconds": delay}
                        except Exception as exc:
                            self.journal.mark(logical, number, key,
                                "transport_retryable" if transient_exception(exc) else "permanent")
                            raise

                    try:
                        envelope = self.journal.call(self.name, attempt_scope, kind, self.endpoint,
                                                     body, output_tokens, invoke)
                    except ApiError:
                        saved = self.journal.prior(key)
                        decision = self.journal.decision(logical, number)
                        if saved is None:  # Budget/queue/time rejection before a native attempt.
                            raise
                        if decision and decision[0] == "permanent":
                            raise
                        if number + 1 < maximum:
                            ready = self._schedule(logical, number, key, "transport_retryable",
                                self.reconnect["retry_backoff_seconds"][number])
                            self._wait(ready, until)
                        continue

                    raw = base64.b64decode(envelope["raw_b64"], validate=True)
                    if hashlib.sha256(raw).hexdigest() != envelope["raw_sha256"]:
                        raise ApiError(503, "saved provider receipt integrity failure")
                    try:
                        payload = json.loads(raw)
                    except (ValueError, UnicodeError):
                        payload = None
                    error = payload.get("error", {}) if isinstance(payload, dict) else {}
                    code = error.get("code") if isinstance(error, dict) else None
                    if not isinstance(code, str):
                        code = None
                    status = envelope["http_status"]
                    if status in RETRYABLE_HTTP and code not in PERMANENT_CODES:
                        if number + 1 < maximum:
                            delay = max(self.reconnect["retry_backoff_seconds"][number],
                                        envelope.get("retry_after_seconds", 0))
                            ready = self._schedule(logical, number, key, "http_retryable", delay)
                            self._wait(ready, until)
                        continue
                    if not 200 <= status < 300 or not isinstance(payload, dict):
                        self.journal.mark(logical, number, key, "permanent")
                        raise ApiError(503, "provider rejected request or returned invalid output; no automatic regeneration")
                    self.journal.mark(logical, number, key, "success")
                    check_until(until)
                    return payload
                raise ApiError(503, "bounded provider attempts exhausted; receipts and all reserves retained")
        finally:
            REQUEST_DEADLINE.reset(token)
