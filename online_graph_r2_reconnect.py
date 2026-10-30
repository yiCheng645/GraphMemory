"""The original online R2 route, with the user-authorized bounded retry revision."""
from pathlib import Path
from entity_identity_v1 import ROOT, digest, sha
from online_graph_r2_fixes import OnlineRouter as QueryFixedRouter, LIVE_ROOT as QUERY_ROOT
from online_graph_r2_fixes import VERSION, RETRIEVAL_POLICY

REVISION = "r2-bounded-reconnect-20261030"
LIVE_ROOT = QUERY_ROOT / "bounded_reconnect_20261030"


def safe_root(path):
    path = Path(path).resolve()
    if not path.is_relative_to(LIVE_ROOT) or path == LIVE_ROOT:
        raise ValueError("bounded reconnect requires a new explicit online R2 instance")
    return path


class OnlineRouter(QueryFixedRouter):
    def __init__(self, root, user_ids, factories, encoder, *, policy_id, **kwargs):
        root = safe_root(root)
        binding = digest([REVISION, policy_id, {
            name: sha(ROOT / name) for name in ("online_graph_r2_reconnect.py", "online_graph_r2_retry_provider.py")}])
        super().__init__(root, user_ids, factories, encoder, policy_id=binding, **kwargs)

    def add(self, body):
        return {**super().add(body), "revision": REVISION}

    def search_with_trace(self, body):
        response, trace = super().search_with_trace(body)
        response["meta"]["revision"] = REVISION
        trace["revision"] = REVISION
        return response, trace

    def health(self):
        return {**super().health(), "revision": REVISION, "bounded_automatic_reconnect": True}
