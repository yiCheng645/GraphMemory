"""Version the HTTP client fix around the unchanged deployed original R2 router."""
from pathlib import Path
from entity_identity_v1 import ROOT,digest,sha
from online_graph_r2_deployment import OnlineRouter as DeployedRouter,LIVE_ROOT as DEPLOY_ROOT
from online_graph_r2_deployment import VERSION,RETRIEVAL_POLICY,user_settings
REVISION="r2-category-unlimited-20261030"
LIVE_ROOT=DEPLOY_ROOT/"c"

def safe_root(path):
    path=Path(path).resolve()
    if not path.is_relative_to(LIVE_ROOT) or path==LIVE_ROOT:
        raise ValueError("new explicit HTTP-client R2 instance required")
    return path

class OnlineRouter(DeployedRouter):
    def __init__(self,root,user_ids,factories,encoder,*,policy_id,**kwargs):
        binding=digest([REVISION,policy_id,{n:sha(ROOT/n) for n in ("online_graph_r2_http_client.py","online_graph_r2_http_provider.py","evidence_packing.py","graph_validation.py","graph_query_support.py","graph_source.py","graph_types.py","identity_quote_whitespace.py","online_graph_r2_concurrent_journal.py","online_graph_r2_identity_retry.py","identity_indexed_citations.py","category_candidate_lookup.py")}])
        super().__init__(safe_root(root),user_ids,factories,encoder,policy_id=binding,**kwargs)
    def add(self,body):
        return {**super().add(body),"revision":REVISION}
    def search_with_trace(self,body):
        response,trace=super().search_with_trace(body)
        response["meta"]["revision"]=REVISION;trace["revision"]=REVISION
        return response,trace
    def health(self):
        return {**super().health(),"revision":REVISION,"declared_http_user_agent":True}
