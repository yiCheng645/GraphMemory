"""Same bounded native transport, with a declared application User-Agent."""
from online_graph_r2_retry_provider import *
from online_graph_r2_retry_provider import _NoRedirect


def validate_user_agent(value):
    if not isinstance(value,str) or not 1<=len(value)<=120 or not value.isascii() or any(ord(c)<32 or ord(c)==127 for c in value):
        raise ValueError("bounded printable ASCII application User-Agent required")
    return value


class DeclaredUserAgentProvider(RetryingOnlineProvider):
    def __init__(self,config,journal,name,**kwargs):
        self.user_agent=validate_user_agent(config.get("user_agent","MemoryR2/1.0"))
        journal_name=config.get("journal_name",name)
        if journal_name!=name and (name,journal_name)!=("chat","chat-http-v1"):
            raise ValueError("only the explicit reviewed chat transport migration may use a journal alias")
        super().__init__(config,journal,journal_name,**kwargs)

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
                            headers={"Authorization": "Bearer " + self.key, "Content-Type": "application/json",
                                     "User-Agent": self.user_agent})
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
