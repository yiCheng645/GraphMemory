"""Direct graph v2: role-aware entry only. Never extracts or writes facts."""
from pathlib import Path
from contextlib import closing
from collections import defaultdict
from copy import deepcopy
import re,json
from entity_identity_v1 import ro,sha,digest
from graph_source import source_units
from graph_block_reference_v4 import validate_bundle
from graph_validation import checked_candidate
from graph_query_support import append_raw
VERSION='direct-graph-v2-role-entry'
MAX_SEEDS=5
MAX_ROOTS_PER_SEED=8
PRONOUNS={'i','me','my','mine','myself','we','us','our','ours','you','your','yours','yourself','he','she','it','they','them','their','@user','user','assistant','我','我们','你','你们','他','她','它','他们','用户','助手'}
def norm(s):return ' '.join(s.split()).casefold()

def previous_user(messages,mid):
 """Same-session insertion order, never timestamp ties or cross-session guesses."""
 target=messages[mid]
 rows=sorted((m for m in messages.values() if m['session_id']==target['session_id'] and m['position']<target['position']),key=lambda m:m['position'],reverse=True)
 for m in rows:
  if m['role']=='user':return m
  if m['role']!='assistant':return None
 return None

def select_seeds(candidates,messages,lookup):
 active=[];seen=set();attempts=[]
 def add(doc,origin,mapped_from=None,whole=False):
  mid=doc['message_id'];key=mid
  if key in seen:return False
  roots,detail=lookup(doc,whole)
  attempt={'origin':origin,'block_id':doc.get('id'),'message_id':mid,'mapped_from':mapped_from,'roots':roots,'lookup':detail,'asserted_fact':False}
  attempts.append(attempt)
  if not roots:return False
  seen.add(key);active.append(attempt);return True
 for doc in candidates[:5]:
  if len(active)>=MAX_SEEDS:break
  message=messages[doc['message_id']]
  if message['role']=='user':add(doc,'initial_user')
  elif message['role']=='assistant':
   roots,detail=lookup(doc,False)
   if roots:
    # Explicit entity reference is an entry, not an assistant fact.
    if doc['message_id'] not in seen:
     seen.add(doc['message_id']);a={'origin':'assistant_explicit_entity','block_id':doc['id'],'message_id':doc['message_id'],'mapped_from':None,'roots':roots,'lookup':detail,'asserted_fact':False};active.append(a);attempts.append(a)
   else:
    attempts.append({'origin':'assistant_no_explicit_entity','block_id':doc['id'],'message_id':doc['message_id'],'roots':[],'lookup':detail,'asserted_fact':False})
    user=previous_user(messages,doc['message_id'])
    if user:add({'id':None,'message_id':user['id']},'same_turn_user',mapped_from=doc['message_id'],whole=True)
 for doc in candidates:
  if len(active)>=MAX_SEEDS:break
  if messages[doc['message_id']]['role']=='user':add(doc,'fallback_top100_user')
 return active,attempts

class RoleAwareDirectGraph:
 def __init__(self,provider):
  self.provider=provider;self.path=Path(provider.path);self.expected=provider.expected
 def expand(self,user_id,question,baseline,source_path):
  if len(baseline)!=100 or len({d['id'] for d in baseline})!=100:raise ValueError('100 unique original candidates required')
  source=Path(source_path).resolve();col=digest([str(source),user_id])
  # Retain the existing provider's graph-policy and dataset-adapter guards.
  self.provider.collect(user_id,[],question,'direct_graph',source)
  before_source=sha(source);before_graph=sha(self.path)
  if before_graph!=self.expected:raise ValueError('graph snapshot changed')
  with closing(ro(source)) as s,closing(ro(self.path)) as g:
   messages={r['id']:dict(r) for r in s.execute('select rowid as position,* from messages where user_id=? order by rowid',(user_id,))}
   units={r['id']:dict(r) for r in s.execute('select * from units where user_id=?',(user_id,))}
   docs=[]
   for d in baseline:
    if d['id'] not in units:raise ValueError('candidate outside source/user')
    u=units[d['id']];m=messages[u['message_id']];actual={a['id']:a for a in source_units(s,user_id,m,0,len(m['content']))}[d['id']]
    if actual['content']!=d['content']:raise ValueError('altered raw candidate')
    docs.append({'id':d['id'],'message_id':u['message_id']})
   entities={r['id']:dict(r) for r in g.execute('select * from entities where collection=? and user_id=?',(col,user_id))}
   aliases=list(g.execute('select * from aliases where collection=? and user_id=? order by entity_id,normalized',(col,user_id)))
   protected=set(PRONOUNS)|{norm(e['name']) for e in entities.values() if e['identity']=='speaker'}
   protected|={norm(a['alias']) for a in aliases if entities[a['entity_id']]['identity']=='speaker'}
   def safe(eid):return eid in entities and entities[eid]['identity']!='speaker' and norm(entities[eid]['name']) not in protected
   names=defaultdict(set)
   for a in aliases:
    if safe(a['entity_id']) and norm(a['alias']) not in protected:names[norm(a['alias'])].add(a['entity_id'])
   edges=list(g.execute('select * from relations where collection=? and user_id=? order by id',(col,user_id)))
   byblock=defaultdict(set)
   for r in g.execute('select r.subject,r.object,c.block_id from relations r join citations c on r.id=c.relation_id where r.collection=? and r.user_id=?',(col,user_id)):
    byblock[r['block_id']].update(eid for eid in (r['subject'],r['object']) if eid and safe(eid))
   def lookup(doc,whole):
    m=messages[doc['message_id']];text=m['content'] if whole else units[doc['id']]['content'];roots=set();matched=[];ambiguous=[]
    if m['role']=='user':
     bids=[bid for bid,u in units.items() if u['message_id']==m['id']] if whole else [doc['id']]
     for bid in bids:roots.update(byblock[bid])
    # User entries keep original source-resolved literal behavior. Assistant
    # entries may refer to any unique exact existing alias; ambiguous IDs abstain.
    for name,eids in sorted(names.items(),key=lambda x:(-len(x[0]),x[0])):
     if not re.search(r'(?<!\w)'+re.escape(name)+r'(?!\w)',text,re.I):continue
     if m['role']=='user':eids={eid for eid in eids if entities[eid]['identity']=='source_resolved'}
     if len(eids)==1:roots.update(eids);matched.append({'alias':name,'entity_id':next(iter(eids)),'identity_confirmed':False})
     elif len(eids)>1:ambiguous.append({'alias':name,'candidate_ids':sorted(eids)})
    ordered=sorted(roots);selected=ordered if m['role']=='user' else ordered[:MAX_ROOTS_PER_SEED];return selected,{'role':m['role'],'matched_aliases':matched,'ambiguous_aliases_not_chosen':ambiguous,'roots_before_cap':len(ordered),'root_cap':MAX_ROOTS_PER_SEED,'whole_user_message':whole}
   active,attempts=select_seeds(docs,messages,lookup);roots={}
   for rank,seed in enumerate(active,1):
    for eid in seed['roots']:roots.setdefault(eid,rank)
   hits={};paths=[];excluded_assistant=0
   for e in edges:
    entered=[roots[eid] for eid in (e['subject'],e['object']) if eid in roots]
    if not entered:continue
    c=json.loads(e['candidate']);b=validate_bundle(json.loads(e['bundle']));assert checked_candidate(b,c)==c
    if b['source_db']!=str(source) or b['target']['user_id']!=user_id:raise ValueError('edge source binding changed')
    m=messages.get(e['message_id'])
    if m is None:raise ValueError('edge message outside scope')
    if m['role']!='user':excluded_assistant+=1;continue
    priority=(min(entered),e['id']);actual={u['id']:u for u in source_units(s,user_id,m,0,len(m['content']))}
    paths.append({'relation_id':e['id'],'subject_id':e['subject'],'object_id':e['object'],'predicate':e['predicate'],'message_id':m['id'],'kind':c['kind'],'scope':c['scope'],'new_fact':False})
    for bid in c['source_block_ids']:
     if bid not in actual:raise ValueError('edge citation not its source')
     if bid not in hits or priority<hits[bid][0]:hits[bid]=(priority,actual[bid])
  raw=[u for _,u in sorted(hits.values(),key=lambda h:(h[0],h[1]['id']))]
  pool=append_raw(deepcopy(baseline),raw,source,user_id)
  if sha(source)!=before_source or sha(self.path)!=before_graph:raise ValueError('source or graph changed during query')
  return {'version':VERSION,'candidates':pool,'new_raw_blocks':len(pool)-100,'trace':{'graph_status':'ok' if edges else 'missing_background','active_seeds':active,'seed_attempts':attempts,'seed_roots':list(roots),'paths':paths,'max_active_seeds':5,'excluded_assistant_fact_edges':excluded_assistant,'turn_mapping_basis':'same-session insertion rowid, nearest preceding user; reference only, not semantic confirmation','speaker_hub_traversal':False,'new_facts':0,'new_model_calls':0,'baseline_assistant_blocks_preserved':True}}
