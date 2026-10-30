"""Compose the proven online Add engine with the original direct-graph-v1 search."""
from pathlib import Path
from entity_identity_v1 import ROOT, digest, sha
from online_graph_add_v3 import OnlineRouter as AddEngine, LIVE_ROOT as ENGINE_ROOT
from role_graph_system_v2_r2 import RoleGraphStoreR2 as DirectGraphStore, RETRIEVAL_POLICY
VERSION='direct-graph-v2-online-add-r2'

LIVE_ROOT = ENGINE_ROOT / 'role_entry_v2_r2'


def safe_root(path):
    path = Path(path).resolve()
    if not path.is_relative_to(LIVE_ROOT) or path == LIVE_ROOT:
        raise ValueError('new isolated instance under data/live/online_graph_v3/role_entry_v2_r2 required')
    return path


class OnlineRouter:
    def __init__(self, root, user_ids, factories, encoder, *, policy_id, strict_graph=False, fault=None):
        root = safe_root(root)
        # This immutable binding also prevents an existing v2 online instance from
        # silently changing retrieval policy while keeping its data/version label.
        query_code = {n: sha(ROOT / n) for n in ('online_graph_add_v2_r2.py',
                      'role_graph_system_v2_r2.py', 'direct_graph_role_entry_v2_r2.py', 'serve_online_graph_v2_r2.py',
                      'graph_query_support.py', 'formatted_reranker.py', 'evidence_packing.py')}
        binding = digest([VERSION, RETRIEVAL_POLICY, policy_id, query_code])
        self.engine = AddEngine(root, user_ids, factories, encoder, policy_id=binding,
                                strict_graph=strict_graph, fault=fault)
        self.routes = self.engine.routes
        try:
            for store in self.routes.values():
                previous = store.route
                store.route = DirectGraphStore(store.base, previous.reranker, previous.encoder,
                                               strict_graph=strict_graph, readonly=False)
        except Exception:
            self.engine.close()
            raise

    def close(self):
        self.engine.close()

    def add(self, body):
        result = self.engine.add(body)
        return {**result, 'route': VERSION, 'retrieval_policy': RETRIEVAL_POLICY}

    def search_with_trace(self, body):
        store = self.engine._route(body)
        response, trace = store.search_with_trace(body)
        # The shared Add engine labels its internal publication generation; the
        # public route is the actual v1 query implementation selected above.
        response['meta'].update(route=VERSION, retrieval_policy=RETRIEVAL_POLICY)
        return response, trace

    def search(self, body):
        return self.search_with_trace(body)[0]

    def health(self):
        return {**self.engine.health(), 'route': VERSION, 'retrieval_policy': RETRIEVAL_POLICY,
                'ingestion_engine': 'direct-graph-v3-online-add-v1'}
