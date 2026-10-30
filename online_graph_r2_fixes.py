"""Port long-query/queue fixes onto the original allowlisted online R2 engine."""
from pathlib import Path
import time

from entity_identity_v1 import ROOT, digest, sha
from memory_service import ApiError
from online_graph_add_v2_r2 import (OnlineRouter as OriginalRouter,
    LIVE_ROOT as R2_ROOT, VERSION, RETRIEVAL_POLICY)
from online_graph_r2_deadline import REQUEST_DEADLINE, check_deadline
from online_graph_r2_query import WindowedMemory, BoundedQueryReranker, MAX_QUERY_CHARS
from online_graph_r2_search import RoleGraphStoreLongQuery

REVISION = "r2-query-timeout-20261030"
LIVE_ROOT = R2_ROOT / "query_timeout_20261030"


def safe_root(path):
    path = Path(path).resolve()
    if not path.is_relative_to(LIVE_ROOT) or path == LIVE_ROOT:
        raise ValueError("patched R2 requires a new instance under role_entry_v2_r2/query_timeout_20261030")
    return path


class OnlineRouter:
    def __init__(self, root, user_ids, factories, encoder, *, policy_id,
                 strict_graph=False, fault=None, request_seconds=1740):
        self.root = safe_root(root)
        if type(request_seconds) not in (int, float) or not 0 < request_seconds <= 1740:
            raise ValueError("bounded positive online R2 request duration required")
        self.request_seconds = request_seconds
        sources = ("online_graph_r2_fixes.py", "online_graph_r2_deadline.py",
            "online_graph_r2_query.py", "online_graph_r2_search.py", "online_graph_r2_provider.py")
        binding = digest([REVISION, policy_id, request_seconds,
                          {n: sha(ROOT / n) for n in sources}])
        self.original = OriginalRouter(self.root, user_ids, factories, encoder,
            policy_id=binding, strict_graph=strict_graph, fault=fault)
        self.engine, self.routes = self.original.engine, self.original.routes
        try:
            for store in self.routes.values():
                previous = store.route
                store.route = RoleGraphStoreLongQuery(
                    WindowedMemory(store.base, check_deadline),
                    BoundedQueryReranker(previous.reranker, check_deadline), previous.encoder,
                    strict_graph=strict_graph, readonly=False)
        except BaseException:
            self.close()
            raise

    def close(self):
        self.original.close()

    def _call(self, body, operation):
        token = REQUEST_DEADLINE.set(time.monotonic() + self.request_seconds)
        acquired, store = False, None
        try:
            check_deadline()
            store = self.engine._route(body)
            remaining = REQUEST_DEADLINE.get() - time.monotonic()
            if remaining <= 0:
                check_deadline()
            acquired = store.lock.acquire(timeout=remaining)
            if not acquired:
                raise ApiError(503, "online R2 user queue time budget exhausted")
            check_deadline()
            result = operation(body)
            check_deadline()
            return result
        finally:
            if acquired:
                store.lock.release()
            REQUEST_DEADLINE.reset(token)

    def add(self, body):
        result = self._call(body, self.original.add)
        return {**result, "revision": REVISION}

    def search_with_trace(self, body):
        response, trace = self._call(body, self.original.search_with_trace)
        response["meta"].update(route=VERSION, retrieval_policy=RETRIEVAL_POLICY, revision=REVISION)
        trace["revision"] = REVISION
        return response, trace

    def search(self, body):
        return self.search_with_trace(body)[0]

    def health(self):
        return {**self.original.health(), "revision": REVISION,
                "max_query_characters": MAX_QUERY_CHARS,
                "request_timeout_seconds": self.request_seconds}
