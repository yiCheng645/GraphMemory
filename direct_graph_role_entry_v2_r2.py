"""v2-r2 role entry: preserve v1, fan out ambiguous aliases, merge message blocks."""
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
from direct_graph_role_entry_v2 import previous_user
VERSION='direct-graph-v2-role-entry-r2'
MAX_SEEDS=5

def select_seeds(candidates,messages,lookup):
 groups={}
 for doc in candidates:
  mid=doc['message_id'];group=groups.setdefault(mid,{'id':doc['id'],'message_id':mid,'candidate_blocks':[]});group['candidate_blocks'].append(doc)
 active=[];selected={};attempts=[]
 def add(group,origin,mapped_from=None,whole=False):
  roots,detail=lookup(group,whole);mid=group['message_id']
  attempt={'origin':origin,'block_id':group.get('id'),'block_ids':[d['id'] for d in group.get('candidate_blocks',[])],'message_id':mid,'mapped_from':mapped_from,'roots':list(roots),'lookup':detail,'asserted_fact':False}
  attempts.append(attempt)
  if not roots:return False
  if mid in selected:
   seed=selected[mid];seed['roots']=sorted(set(seed['roots'])|set(roots));seed['entry_sources'].append({'origin':origin,'mapped_from':mapped_from});seed['block_ids']=list(dict.fromkeys(seed['block_ids']+attempt['block_ids']));return True
  if len(active)>=MAX_SEEDS:return False
  attempt=deepcopy(attempt);attempt['entry_sources']=[{'origin':origin,'mapped_from':mapped_from}];active.append(attempt);selected[mid]=attempt;return True
 initial_mids=list(dict.fromkeys(d['message_id'] for d in candidates[:5]))
 for mid in initial_mids:
  group=groups[mid];role=messages[mid]['role']
  if role=='user':add(group,'initial_user_merged_blocks')
  elif role=='assistant':
   if not add(group,'assistant_alias_merged_blocks'):
    user=previous_user(messages,mid)
    if user:add(groups.get(user['id'],{'id':None,'message_id':user['id'],'candidate_blocks':[]}), 'same_turn_user',mid,True)
 for mid,group in groups.items():
  if len(active)>=MAX_SEEDS:break
  if mid not in selected and messages[mid]['role']=='user':add(group,'fallback_top100_user_merged_blocks')
 return active,attempts

class RoleAwareDirectGraphR2:
 def __init__(self,provider):self.provider=provider;self.path=Path(provider.path);self.expected=provider.expected
 def expand(self,user_id,question,baseline,source_path):
  if len(baseline)!=100 or len({d['id'] for d in baseline})!=100:raise ValueError('100 unique original candidates required')
  source=Path(source_path).resolve();col=digest([str(source),user_id]);before_source=sha(source);before_graph=sha(self.path)
  if before_graph!=self.expected:raise ValueError('graph snapshot changed')
  v1hits,v1trace=self.provider.collect(user_id,[d['id'] for d in baseline[:5]],question,'direct_graph',source)
  with closing(ro(source)) as s,closing(ro(self.path)) as g:
   messages={r['id']:dict(r) for r in s.execute('select rowid as position,* from messages where user_id=? order by rowid',(user_id,))};units={r['id']:dict(r) for r in s.execute('select * from units where user_id=?',(user_id,))};docs=[]
   for d in baseline:
    if d['id'] not in units:raise ValueError('candidate outside source/user')
    u=units[d['id']];m=messages[u['message_id']];actual={a['id']:a for a in source_units(s,user_id,m,0,len(m['content']))}[d['id']]
    if actual['content']!=d['content']:raise ValueError('altered raw candidate')
    docs.append({'id':d['id'],'message_id':u['message_id']})
   entities={r['id']:dict(r) for r in g.execute('select * from entities where collection=? and user_id=?',(col,user_id))}
   def safe(eid):return eid in entities and entities[eid]['identity']!='speaker'
   aliases=defaultdict(set)
   for a in g.execute('select * from aliases where collection=? and user_id=? order by entity_id,normalized',(col,user_id)):
    if safe(a['entity_id']):aliases[a['alias']].add(a['entity_id'])
   edges=list(g.execute('select * from relations where collection=? and user_id=? order by id',(col,user_id)));byblock=defaultdict(set)
   for r in g.execute('select r.subject,r.object,c.block_id from relations r join citations c on r.id=c.relation_id where r.collection=? and r.user_id=?',(col,user_id)):
    byblock[r['block_id']].update(eid for eid in (r['subject'],r['object']) if eid and safe(eid))
   def lookup(group,whole):
    m=messages[group['message_id']];bids=[bid for bid,u in units.items() if u['message_id']==m['id']] if whole else [d['id'] for d in group['candidate_blocks']]
    # Match each original block separately: joining nonadjacent chunks could invent an alias.
    texts=[m['content']] if whole else [units[bid]['content'] for bid in bids];roots=set();matched=[];ambiguous=[]
    if m['role']=='user':
     for bid in bids:roots.update(byblock[bid])
    for alias,eids in sorted(aliases.items()):
     if not alias or not any(re.search(r'(?<!\w)'+re.escape(alias)+r'(?!\w)',text,re.I) for text in texts):continue
     eligible=eids if m['role']=='assistant' else {eid for eid in eids if entities[eid]['identity']=='source_resolved'}
     if not eligible:continue
     roots.update(eligible);detail={'alias':alias,'entity_ids':sorted(eligible),'identity_confirmed':False,'entities_merged':False};matched.append(detail)
     if len(eligible)>1:ambiguous.append(detail)
    return sorted(roots),{'role':m['role'],'block_ids_considered':bids,'matched_aliases':matched,'ambiguous_aliases_expanded_separately':ambiguous,'whole_user_message':whole,'extra_name_or_pronoun_filter':False,'entity_cap':None}
   active,attempts=select_seeds(docs,messages,lookup);roots={}
   # Original v1 rank keeps precedence; new message seeds only supplement it.
   original_roots=set(v1trace['seed_roots'])
   for rank,seed in enumerate(active,1):
    for eid in seed['roots']:roots.setdefault(eid,rank)
   if not original_roots.issubset(roots):raise ValueError('v1 entity entry was lost')
   hits={};paths=[];excluded=0
   for i,h in enumerate(v1hits):
    m=messages[h['message_id']]
    if m['role']=='user':hits[h['id']]=((0,i),h)
   for e in edges:
    entered=[roots[eid] for eid in (e['subject'],e['object']) if eid in roots]
    if not entered:continue
    c=json.loads(e['candidate']);b=validate_bundle(json.loads(e['bundle']));assert checked_candidate(b,c)==c
    if b['source_db']!=str(source) or b['target']['user_id']!=user_id:raise ValueError('edge source binding changed')
    m=messages.get(e['message_id'])
    if m is None:raise ValueError('edge message outside scope')
    if m['role']!='user':excluded+=1;continue
    priority=(1,min(entered),e['id']);actual={u['id']:u for u in source_units(s,user_id,m,0,len(m['content']))}
    paths.append({'relation_id':e['id'],'subject_id':e['subject'],'object_id':e['object'],'predicate':e['predicate'],'message_id':m['id'],'kind':c['kind'],'scope':c['scope'],'new_fact':False})
    for bid in c['source_block_ids']:
     if bid not in actual:raise ValueError('edge citation not its source')
     if bid not in hits:hits[bid]=(priority,actual[bid])
  raw=[u for _,u in sorted(hits.values(),key=lambda h:(h[0],h[1]['id']))];pool=append_raw(deepcopy(baseline),raw,source,user_id)
  v1pool=append_raw(deepcopy(baseline),v1hits,source,user_id)
  if pool[:len(v1pool)]!=v1pool:raise ValueError('v1 candidate prefix was lost')
  if sha(source)!=before_source or sha(self.path)!=before_graph:raise ValueError('source or graph changed during query')
  return {'version':VERSION,'candidates':pool,'new_raw_blocks':len(pool)-100,'trace':{'graph_status':'ok' if edges else 'missing_background','active_seeds':active,'seed_attempts':attempts,'seed_roots':list(roots),'paths':paths,'max_active_seeds':5,'v1_roots':sorted(original_roots),'v1_roots_preserved':True,'v1_candidate_prefix_preserved':True,'v1_graph_extra_count':len(v1pool)-100,'ambiguous_aliases_are_separate_entries':True,'message_blocks_merged_before_slot_allocation':True,'extra_name_or_pronoun_filter':False,'speaker_hub_traversal':False,'excluded_assistant_fact_edges':excluded,'new_facts':0,'new_model_calls':0,'baseline_assistant_blocks_preserved':True}}
