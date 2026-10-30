"""Complete role-aware graph search route. No network/provider construction here."""
from pathlib import Path
from contextlib import closing
from copy import deepcopy
import sqlite3,math
from memory_service import ApiError,require_text
from entity_identity_v1 import ro,sha
from graph_source import source_units
from direct_graph_role_entry_v2 import RoleAwareDirectGraph
from formatted_reranker import format_document
from reranker import AlibabaReranker
from evidence_packing import pack,source_text
VERSION='direct-graph-v2-system-v1'

def hydrate(rows,source,user_id):
 if not isinstance(rows,list) or len(rows)>120 or len({r['id'] for r in rows})!=len(rows):raise ApiError(503,'invalid source candidate list')
 docs=[];messages={};cache={}
 with closing(ro(source)) as db:
  for r in rows:
   u=db.execute('select * from units where id=? and user_id=?',(r['id'],user_id)).fetchone()
   if u is None:raise ApiError(503,'candidate outside source user')
   mid=u['message_id']
   if mid not in cache:
    m=db.execute('select * from messages where id=? and user_id=?',(mid,user_id)).fetchone()
    if m is None:raise ApiError(503,'candidate message missing')
    m=dict(m);cache[mid]={a['id']:a for a in source_units(db,user_id,m,0,len(m['content']))};messages[mid]={'id':mid,'session_id':m['session_id'],'role':m['role'],'timestamp_ms':m['timestamp'],'content':m['content']}
   raw=cache[mid].get(r['id'])
   if raw is None or raw['content']!=r['content']:raise ApiError(503,'candidate differs from original raw block')
   a,b=raw['span'];score=r.get('score',0.)
   if type(score) not in (float,int) or not math.isfinite(score):raise ApiError(503,'candidate score invalid')
   docs.append({'id':r['id'],'type':'raw','content':raw['content'],'score':score,'spans':[{'message_id':mid,'start':a,'end':b,'text':messages[mid]['content'][a:b]}]})
 return docs,messages

class RoleGraphStore:
 def __init__(self,base,reranker,encoder,*,graph_provider=None,source_sha256=None,strict_graph=False,readonly=True):
  self.base=base;self.path=Path(base.path).resolve();self.reranker=reranker;self.encoder=encoder
  self.graph_provider=graph_provider;self.source_sha256=source_sha256;self.strict_graph=strict_graph;self.readonly=readonly
 def add(self,body):
  if self.readonly:raise ApiError(405,'this bound source snapshot is read-only')
  # The original Add writes text/vectors only. The pinned graph becomes stale;
  # next search reports source_changed until the existing graph builder publishes
  # a new explicit binding. No implicit LLM extraction or graph writes.
  return self.base.add(body)
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
     expanded=RoleAwareDirectGraph(self.graph_provider).expand(uid,text,docs,self.path)
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
  docs,messages,trace,query,top_k=self.collect(body)
  kept=[];texts=[]
  for i,d in enumerate(docs):
   formatted=format_document(d['content'])
   if formatted is not None:kept.append(i);texts.append(formatted)
  ranked=[]
  if texts:
   # One actual request for the complete ordered union; never combine old scores.
   response=self.reranker.rerank(query,texts,len(texts))
   parsed=AlibabaReranker.parse({'output':{'results':response}},len(texts),len(texts))
   for r in parsed:
    doc=deepcopy(docs[kept[r['index']]]);doc['score']=r['relevance_score'];ranked.append(doc)
  evidence=pack(ranked[:top_k],messages,self.encoder,budget=8192)
  scores={d['id']:d['score'] for d in ranked};data=[]
  for d in evidence['kept']:
   content='--- Evidence '+d['id']+' ---\n'+'\n[... source gap or previously included text ...]\n'.join(source_text(s,messages) for s in d['spans'])
   data.append({'id':d['id'],'content':content,'score':scores[d['id']]})
  if '\n\n'.join(d['content'] for d in data)!=evidence['text']:raise ApiError(503,'evidence serialization mismatch')
  meta={'route':VERSION,'graph_status':trace['graph_status'],'graph_extra_count':trace['graph_extra_count'],'candidate_count':len(docs),'reranker_document_count':len(texts),'evidence_tokens':evidence['tokens'],'evidence_budget':8192}
  if 'graph_fallback_reason' in trace:meta['graph_fallback_reason']=trace['graph_fallback_reason']
  trace.update(reranker_document_count=len(texts),packed_evidence=evidence,ranked=ranked)
  return {'data':data,'meta':meta},trace
 def search(self,body):return self.search_with_trace(body)[0]

class RoleGraphRouter:
 def __init__(self,routes,default=None):self.routes=routes;self.default=default
 def _route(self,body):
  if not isinstance(body,dict):raise ApiError(422,'request must be an object')
  uid=require_text(body,'user_id');store=self.routes.get(uid,self.default)
  if store is None:raise ApiError(404,'no source binding for this user')
  return store
 def add(self,body):return self._route(body).add(body)
 def search(self,body):return self._route(body).search(body)
