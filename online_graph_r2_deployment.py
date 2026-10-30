"""Persist platform-assigned users around the selected original online R2 engine."""
from contextlib import closing
from pathlib import Path
import sqlite3
import threading
import time

from entity_identity_v1 import ROOT, digest, sha
from memory_service import ApiError, require_text
from online_graph_add_v3 import OnlineUserStore, validate_add
from online_graph_r2_reconnect import OnlineRouter as ReconnectRouter, LIVE_ROOT as RETRY_ROOT
from online_graph_r2_reconnect import VERSION, RETRIEVAL_POLICY
from online_graph_r2_deadline import REQUEST_DEADLINE, check_deadline
from online_graph_r2_query import WindowedMemory, BoundedQueryReranker, query_text
from online_graph_r2_search import RoleGraphStoreLongQuery

REVISION = "r2-deployment-20261030"
LIVE_ROOT = RETRY_ROOT / "deploy"


def safe_root(path):
    path = Path(path).resolve()
    if not path.is_relative_to(LIVE_ROOT) or path == LIVE_ROOT:
        raise ValueError("deployment revision requires its own new online R2 instance")
    return path


def user_settings(user_ids, policy):
    if (not isinstance(user_ids, list) or any(not isinstance(u, str) or not u.strip() for u in user_ids)
            or len(set(user_ids)) != len(user_ids)):
        raise ValueError("users must be unique nonempty identifiers")
    policy = dict(policy or {"mode": "allowlist", "max_users": 100000})
    if (set(policy) != {"mode", "max_users"} or policy["mode"] not in ("dynamic", "allowlist")
            or type(policy["max_users"]) is not int or not 1 <= policy["max_users"] <= 100000):
        raise ValueError("explicit bounded dynamic/allowlist user policy required")
    if len(user_ids) > policy["max_users"] or (policy["mode"] == "allowlist" and not user_ids):
        raise ValueError("user allowlist/capacity is inconsistent")
    return policy


class OnlineRouter(ReconnectRouter):
    def __init__(self, root, user_ids, factories, encoder, *, policy_id,
                 user_policy=None, strict_graph=False, fault=None, **kwargs):
        root = safe_root(root)
        self.user_policy = user_settings(user_ids, user_policy)
        self.registry_lock = threading.RLock()
        self.factories, self.encoder = factories, encoder
        self.strict_graph, self.fault = strict_graph, fault
        self.allowed_users = frozenset(user_ids)
        binding = digest([REVISION, policy_id, sorted(user_ids), self.user_policy,
                          {n: sha(ROOT / n) for n in ("online_graph_r2_deployment.py", "online_graph_r2_runtime.py")}])
        # The parent owns the process lock and immutable configuration. Dynamic
        # membership belongs in SQLite, not the parent's fixed user list.
        super().__init__(root, [], factories, encoder, policy_id=binding,
                         strict_graph=strict_graph, fault=fault, **kwargs)
        self.registry_path = root / "user_registry.sqlite3"
        try:
            with closing(self.registry()) as db, db:
                db.execute("CREATE TABLE IF NOT EXISTS users(user_id TEXT PRIMARY KEY)")
                registered = [r[0] for r in db.execute("SELECT user_id FROM users ORDER BY user_id")]
            for uid in dict.fromkeys(registered + user_ids):
                self._register(uid)
        except BaseException:
            self.close()
            raise

    def registry(self):
        return sqlite3.connect(self.registry_path, timeout=1)

    def _register(self, uid):
        # Caller holds registry_lock, or is the sole startup thread. Reserve the
        # durable membership before initialization so an interrupted creation is
        # recovered on restart/replay, rather than forgotten or counted twice.
        if uid in self.routes:
            return self.routes[uid]
        if self.user_policy["mode"] == "allowlist" and uid not in self.allowed_users:
            raise ApiError(404, "no source binding for this user")
        with closing(self.registry()) as db, db:
            exists = db.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone()
            if not exists:
                if db.execute("SELECT count(*) FROM users").fetchone()[0] >= self.user_policy["max_users"]:
                    raise ApiError(429, "online R2 user capacity reached")
                db.execute("INSERT INTO users(user_id) VALUES(?)", (uid,))
        directory = self.root / digest(["user", uid])
        directory.mkdir(exist_ok=True)
        store = OnlineUserStore(directory, uid, encoder=self.encoder,
            strict_graph=self.strict_graph, fault=self.fault, **self.factories(uid))
        previous = store.route
        store.route = RoleGraphStoreLongQuery(WindowedMemory(store.base, check_deadline),
            BoundedQueryReranker(previous.reranker, check_deadline), self.encoder,
            strict_graph=self.strict_graph, readonly=False)
        self.routes[uid] = store
        return store

    def _membership(self, body, *, create):
        if not isinstance(body, dict):
            raise ApiError(422, "request must be an object")
        uid = require_text(body, "user_id")
        try:
            uid.encode("utf-8")
        except UnicodeEncodeError:
            raise ApiError(422, "user_id contains invalid Unicode") from None
        deadline = REQUEST_DEADLINE.get()
        wait = max(0., deadline - time.monotonic()) if deadline else self.request_seconds
        if not self.registry_lock.acquire(timeout=wait):
            raise ApiError(503, "online R2 user registration queue time budget exhausted")
        try:
            check_deadline()
            if self.user_policy["mode"] == "allowlist" and uid not in self.allowed_users:
                raise ApiError(404, "no source binding for this user")
            return self._register(uid) if create else self.routes.get(uid)
        finally:
            self.registry_lock.release()

    def add(self, body):
        token = REQUEST_DEADLINE.set(time.monotonic() + self.request_seconds)
        try:
            validate_add(body)  # Invalid Add must not allocate a new namespace.
            self._membership(body, create=True)
            # Preserve the parent's per-user lock, strict completion, request
            # idempotency, and original Add/graph recovery implementation.
            result = self._call(body, self.original.add)
            return {**result, "revision": REVISION}
        finally:
            REQUEST_DEADLINE.reset(token)

    def _call(self, body, operation):
        # Never reset the total request deadline after registration/queueing.
        existing = REQUEST_DEADLINE.get()
        if existing is None:
            return super()._call(body, operation)
        store = self.engine._route(body)
        check_deadline()
        if not store.lock.acquire(timeout=max(0., existing - time.monotonic())):
            raise ApiError(503, "online R2 user queue time budget exhausted")
        try:
            check_deadline()
            result = operation(body)
            check_deadline()
            return result
        finally:
            store.lock.release()

    def search_with_trace(self, body):
        token = REQUEST_DEADLINE.set(time.monotonic() + self.request_seconds)
        try:
            if not isinstance(body, dict):
                raise ApiError(422, "request must be an object")
            query_text(body)
            k = body.get("top_k", 100)
            if type(k) is not int or not 1 <= k <= 100:
                raise ApiError(422, "top_k must be an integer1..100")
            if self._membership(body, create=False) is None:
                # Empty Search is a read: no model call or directory allocation.
                return {"data": [], "meta": {"route": VERSION,
                    "retrieval_policy": RETRIEVAL_POLICY, "revision": REVISION}}, {
                    "revision": REVISION, "graph_status": "empty_source"}
            response, trace = self._call(body, self.original.search_with_trace)
            response["meta"]["revision"] = REVISION
            trace["revision"] = REVISION
            return response, trace
        finally:
            REQUEST_DEADLINE.reset(token)

    def health(self):
        with self.registry_lock:
            return {**super().health(), "revision": REVISION,
                    "user_registration": self.user_policy["mode"],
                    "max_users": self.user_policy["max_users"]}
