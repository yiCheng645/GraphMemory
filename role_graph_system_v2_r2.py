"""v2-r2 collection; reuse existing full-pool reranking and literal packing."""
import sqlite3
from memory_service import ApiError,require_text
from entity_identity_v1 import sha
from role_graph_system_v2 import RoleGraphStore,RoleGraphRouter,hydrate
from direct_graph_role_entry_v2_r2 import RoleAwareDirectGraphR2
VERSION='direct-graph-v2-system-r2'
RETRIEVAL_POLICY='direct-graph-v2-role-entry-r2'
class RoleGraphStoreR2(RoleGraphStore):
 def collect(self,body):
  if not isinstance(body,dict):raise ApiError(422,'request must be an object')
  uid=require_text(body,'user_id');query=require_text(body,'query');k=body.get('top_k',100)
  if type(k) is not int or not 1<=k<=100:raise ApiError(422,'top_k must be an integer1..100')
  options=body.get('options',[])
  if not isinstance(options,list) or any(not isinstance(o,str) for o in options):raise ApiError(422,'options must be an array of strings')
  text=query+('\n'+'\n'.join(options) if options else '')
  if len(text)>8192:raise ApiError(413,'query and options exceed8192 characters')
  raw=self.base.search({**body,'top_k':100})['data']
  if len(raw)>100:raise ApiError(503,'baseline exceeded100 candidates')
  docs,messages=hydrate(raw,self.path,uid)
  trace={'version':VERSION,'baseline_count':len(docs),'graph_status':'empty_source' if not docs else 'not_bound','graph_extra_count':0,'baseline_retained':True,'graph_stage_model_calls':0}
  if docs:
   reason=None
   if len(docs)<100:reason='insufficient_candidates'
   elif self.graph_provider is None:reason='not_bound'
   elif self.source_sha256 is None or sha(self.path)!=self.source_sha256:reason='source_changed'
   if reason is None:
    try:
     expanded=RoleAwareDirectGraphR2(self.graph_provider).expand(uid,text,docs,self.path)
     union=expanded['candidates']
     if union[:100]!=docs or not 100<=len(union)<=120:raise ValueError('graph changed baseline or extra limit')
     if expanded['trace']['graph_status']!='ok':reason='missing_background'
     else:
      docs,messages=hydrate(union,self.path,uid);trace.update(graph_status='ok',graph_extra_count=len(docs)-100,graph_trace=expanded['trace'])
    except (ValueError,OSError,sqlite3.Error,KeyError,TypeError,AssertionError,RuntimeError):reason='unavailable_or_invalid'
   if reason is not None:
    trace.update(graph_status='fallback',graph_fallback_reason=reason)
    if self.strict_graph:raise ApiError(503,'graph route unavailable; strict graph mode')
  return docs,messages,trace,text,k

 def search_with_trace(self,body):
  response,trace=super().search_with_trace(body)
  response['meta'].update(route=VERSION,retrieval_policy=RETRIEVAL_POLICY);trace.update(version=VERSION,retrieval_policy=RETRIEVAL_POLICY)
  return response,trace
