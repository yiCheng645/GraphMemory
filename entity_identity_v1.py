"""Reversible, explicitly reviewed entity identity overlay. No semantic auto-merger."""
from pathlib import Path
from contextlib import closing
import hashlib, json, sqlite3
ROOT=Path(__file__).resolve().parent
WORK=ROOT/'data/evaluations/entity-identity-offline-v1'
VERSION='entity-identity-overlay-v1'
class InvalidIdentity(ValueError): pass

def digest(x):
 return hashlib.sha256(json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path): return json.loads(Path(path).read_text(encoding='utf-8-sig'))
def local(path):
 p=Path(path).resolve()
 if not p.is_relative_to(ROOT): raise InvalidIdentity('active D project required')
 return p
def ro(path):
 p=local(path); wal=Path(str(p)+'-wal')
 if wal.exists() and wal.stat().st_size: raise InvalidIdentity('active WAL unsupported')
 db=sqlite3.connect(p.as_uri()+'?mode=ro&immutable=1',uri=True)
 db.row_factory=sqlite3.Row; db.execute('PRAGMA query_only=ON'); return db
def span(text,phrase,message_id):
 if not phrase: raise InvalidIdentity('nonempty mention required')
 a=text.find(phrase)
 if a<0 or text.find(phrase,a+1)>=0: raise InvalidIdentity('unique literal mention required; otherwise supply offsets')
 return dict(message_id=message_id,start=a,end=a+len(phrase),text=phrase)
def prepare(source_path,user_id,mention,entity_type='unresolved',evidence_scope='uncertain'):
 p=local(source_path)
 if evidence_scope not in ('real','draft','fictional','hypothetical','uncertain'): raise InvalidIdentity('source scope required')
 if entity_type not in ('person','organization','place','object','activity','unresolved'): raise InvalidIdentity('supported entity type required')
 if not isinstance(user_id,str) or not user_id or set(mention)!={'message_id','start','end','text'}: raise InvalidIdentity('complete scoped mention required')
 with closing(ro(p)) as db:
  db.execute('BEGIN')
  row=db.execute('SELECT rowid AS position,id,user_id,session_id,role,content,timestamp FROM messages WHERE user_id=? AND id=?',(user_id,mention['message_id'])).fetchone()
  fp=db.execute("SELECT value FROM meta WHERE key='fingerprint'").fetchone()
  if row is None or fp is None: raise InvalidIdentity('source or fingerprint missing')
  source=dict(row); a,b=mention['start'],mention['end']
  if source['role']!='user': raise InvalidIdentity('user target sources only')
  if type(a) is not int or type(b) is not int or not 0<=a<b<=len(source['content']) or source['content'][a:b]!=mention['text']: raise InvalidIdentity('literal mention offsets invalid')
  context=[dict(r) for r in db.execute('SELECT rowid AS position,id,user_id,session_id,role,content,timestamp FROM messages WHERE user_id=? AND session_id=? AND rowid<=? ORDER BY rowid DESC LIMIT 3',(user_id,source['session_id'],source['position']))][::-1]
 body=dict(version=VERSION,source_path=str(p),user_id=user_id,collection=digest([str(p),user_id]),mention=mention,entity_type=entity_type,evidence_scope=evidence_scope,fingerprint=fp[0],source=source,context=context)
 body['id']=digest(body); return body

def validate(p):
 if prepare(p['source_path'],p['user_id'],p['mention'],p['entity_type'],p['evidence_scope'])!=p: raise InvalidIdentity('source or local context changed')
def pair_id(a,b): return digest([VERSION,sorted([a['id'],b['id']])])

def review(a,b,decision,basis,reason):
 """Build an authored review packet. Exact source matching does NOT validate its semantics."""
 return dict(pair_id=pair_id(a,b),decision=decision,basis=basis,reason=reason,reviewer='assistant_full_source_review_not_independent_gold',full_sources_and_context_read=True,entity_identity_only=True,event_identity_asserted=False)

class EntityIndex:
 def __init__(self,path):
  self.path=local(path)
  if not self.path.is_relative_to(WORK) or self.path==WORK: raise InvalidIdentity('isolated experimental namespace required')
 def _open(self):
  db=sqlite3.connect(self.path); db.row_factory=sqlite3.Row; return db
 def create(self):
  self.path.parent.mkdir(parents=True,exist_ok=True)
  with closing(self._open()) as db,db:
   tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
   if tables and 'identity_meta' not in tables: raise InvalidIdentity('do not migrate other indexes')
   db.executescript('CREATE TABLE IF NOT EXISTS identity_meta(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE IF NOT EXISTS entities(id TEXT PRIMARY KEY,collection TEXT,user_id TEXT,type TEXT); CREATE TABLE IF NOT EXISTS mentions(id TEXT PRIMARY KEY,entity_id TEXT,body TEXT,body_hash TEXT); CREATE TABLE IF NOT EXISTS identity_reviews(id TEXT PRIMARY KEY,left_id TEXT,right_id TEXT,decision TEXT,state TEXT,body TEXT,body_hash TEXT); CREATE TABLE IF NOT EXISTS identity_audit(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT,action TEXT,reason TEXT); CREATE TABLE IF NOT EXISTS source_relations(id TEXT PRIMARY KEY,body TEXT,body_hash TEXT);')
   expected={'version':VERSION,'code_sha256':sha(__file__)}; existing=dict(db.execute('SELECT key,value FROM identity_meta'))
   if existing and existing!=expected: raise InvalidIdentity('index code policy changed')
   for k,v in expected.items(): db.execute('INSERT OR IGNORE INTO identity_meta VALUES(?,?)',(k,v))
  return self
 def _policy(self,db):
  if dict(db.execute('SELECT key,value FROM identity_meta'))!={'version':VERSION,'code_sha256':sha(__file__)}: raise InvalidIdentity('index policy changed')
 def _mention(self,db,mid):
  r=db.execute('SELECT * FROM mentions WHERE id=?',(mid,)).fetchone()
  if r is None: raise InvalidIdentity('unknown mention')
  p=json.loads(r['body'])
  if digest(p)!=r['body_hash'] or p['id']!=mid or r['entity_id']!=digest([VERSION,'entity',mid]): raise InvalidIdentity('mention identity changed')
  validate(p); return p
 def add(self,p):
  validate(p); self.create(); eid=digest([VERSION,'entity',p['id']])
  with closing(self._open()) as db,db:
   self._policy(db)
   old=db.execute('SELECT body_hash FROM mentions WHERE id=?',(p['id'],)).fetchone()
   if old and old[0]!=digest(p): raise InvalidIdentity('mention changed')
   db.execute('INSERT OR IGNORE INTO entities VALUES(?,?,?,?)',(eid,p['collection'],p['user_id'],p['entity_type']))
   db.execute('INSERT OR IGNORE INTO mentions VALUES(?,?,?,?)',(p['id'],eid,json.dumps(p,ensure_ascii=False),digest(p)))
  return p['id']
 def _records(self,db,collection):
  self._policy(db); valid=[]; invalid=[]
  selected=list(db.execute("SELECT r.* FROM identity_reviews r JOIN mentions m ON m.id=r.left_id JOIN entities e ON e.id=m.entity_id WHERE r.state='active' AND e.collection=? ORDER BY r.id LIMIT 257",(collection,)))
  if len(selected)>256: raise InvalidIdentity('identity review expansion limit exceeded')
  for row in selected:
   try:
    body=json.loads(row['body']); a=self._mention(db,row['left_id']); b=self._mention(db,row['right_id'])
    last=db.execute('SELECT action FROM identity_audit WHERE id=? ORDER BY seq DESC LIMIT 1',(row['id'],)).fetchone()
    if digest(body)!=row['body_hash'] or body['pair_id']!=pair_id(a,b) or row['id']!=body['pair_id'] or row['decision']!=body['decision'] or last is None or last[0]!='active': raise InvalidIdentity('review or audit changed')
    valid.append((row,a,b,body))
   except (InvalidIdentity,KeyError,ValueError) as e: invalid.append({'id':row['id'],'reason':str(e)})
  return valid,invalid
 @staticmethod
 def _component(mid,rows):
  group={mid}; changed=True
  while changed:
   changed=False
   for row,a,b,body in rows:
    if body['decision']=='same' and (a['id'] in group or b['id'] in group):
     before=len(group); group.update([a['id'],b['id']]); changed|=len(group)>before
     if len(group)>32: raise InvalidIdentity('entity component limit exceeded')
  return group
 def decide(self,left,right,packet):
  if left==right: raise InvalidIdentity('two distinct mentions required')
  with closing(self._open()) as db,db:
   self._policy(db); a=self._mention(db,left); b=self._mention(db,right)
   if a['collection']!=b['collection'] or a['user_id']!=b['user_id']: raise InvalidIdentity('cross-collection or cross-user identity forbidden')
   required={'pair_id','decision','basis','reason','reviewer','full_sources_and_context_read','entity_identity_only','event_identity_asserted'}
   if set(packet)!=required or packet['pair_id']!=pair_id(a,b) or packet['decision'] not in ('same','different','unknown') or packet['full_sources_and_context_read'] is not True or packet['entity_identity_only'] is not True or packet['event_identity_asserted'] is not False or packet['reviewer']!='assistant_full_source_review_not_independent_gold' or not isinstance(packet['reason'],str) or len(packet['reason'])<12: raise InvalidIdentity('complete explicit authored source review required')
   if packet['basis'] not in ('explicit_reference','unambiguous_local_context','explicit_distinct','insufficient_evidence'): raise InvalidIdentity('supported source review basis required')
   if packet['decision']=='same':
    if a['evidence_scope']!='real' or b['evidence_scope']!='real': raise InvalidIdentity('uncertain or fictional source cannot become a real entity link')
    if packet['basis'] not in ('explicit_reference','unambiguous_local_context'): raise InvalidIdentity('name match alone cannot approve identity')
    if a['entity_type']=='unresolved' or a['entity_type']!=b['entity_type']: raise InvalidIdentity('compatible resolved types required')
    if a['source']['session_id']!=b['source']['session_id'] and packet['basis']!='explicit_reference': raise InvalidIdentity('cross-session identity needs explicit source bridge')
   old=db.execute('SELECT state,body_hash FROM identity_reviews WHERE id=?',(packet['pair_id'],)).fetchone()
   if old:
    if old['state']=='revoked' or old['body_hash']!=digest(packet): raise InvalidIdentity('no silent replacement or reactivation')
    return {'id':packet['pair_id'],'state':old['state'],'cached':True}
   rows,invalid=self._records(db,a['collection']); state='active'
   if invalid: raise InvalidIdentity('invalid source/review requires explicit handling before new decisions')
   ga=self._component(left,rows); gb=self._component(right,rows); joined=ga|gb
   if packet['decision']=='different' and right in ga: state='held_conflict'
   if packet['decision']=='same' and any(body['decision']=='different' and a1['id'] in joined and b1['id'] in joined for _,a1,b1,body in rows): state='held_conflict'
   db.execute('INSERT INTO identity_reviews VALUES(?,?,?,?,?,?,?)',(packet['pair_id'],left,right,packet['decision'],state,json.dumps(packet,ensure_ascii=False),digest(packet)))
   db.execute('INSERT INTO identity_audit(id,action,reason) VALUES(?,?,?)',(packet['pair_id'],state,packet['reason']))
   return {'id':packet['pair_id'],'state':state,'cached':False}
 def resolve(self,source_path,user_id,mid):
  with closing(ro(self.path)) as db:
   self._policy(db); p=self._mention(db,mid)
   if p['source_path']!=str(local(source_path)) or p['user_id']!=user_id: raise InvalidIdentity('requested user/collection mismatch')
   rows,invalid=self._records(db,p['collection']); scope=[r for r in rows if r[1]['collection']==p['collection']]
   group=self._component(mid,scope)
   if any(body['decision']=='different' and a['id'] in group and b['id'] in group for _,a,b,body in scope): raise InvalidIdentity('contradictory identity cluster')
   members=[self._mention(db,x) for x in sorted(group)]
   return {'mention_ids':sorted(group),'entity_ids':[digest([VERSION,'entity',x['id']]) for x in members],'mentions':members,'used_identity_links':[r['id'] for r,a,b,body in scope if body['decision']=='same' and a['id'] in group and b['id'] in group],'invalid_links':invalid,'automatic_identity_discovery':False,'event_identity_asserted':False,'canonical_cluster_id_persistent':False}
 def revoke(self,pid,reason):
  if not isinstance(reason,str) or not reason.strip(): raise InvalidIdentity('revocation reason required')
  with closing(self._open()) as db,db:
   self._policy(db)
   if db.execute('SELECT 1 FROM identity_reviews WHERE id=?',(pid,)).fetchone() is None: raise InvalidIdentity('unknown decision')
   db.execute("UPDATE identity_reviews SET state='revoked' WHERE id=?",(pid,)); db.execute('INSERT INTO identity_audit(id,action,reason) VALUES(?,?,?)',(pid,'revoked',reason))
 def add_relation(self,left,right,quote,kind,scope,event_time='unknown'):
  if kind not in ('fact','preference','plan','negated') or scope not in ('real','draft','fictional','hypothetical','uncertain') or event_time!='unknown': raise InvalidIdentity('prototype preserves labels and unknown event time only')
  with closing(self._open()) as db,db:
   self._policy(db); a=self._mention(db,left); b=self._mention(db,right)
   if a['collection']!=b['collection'] or a['mention']['message_id']!=b['mention']['message_id']: raise InvalidIdentity('relation endpoints must occur in the same source message')
   src=a['source']['content']; lo,hi=quote['start'],quote['end']
   if set(quote)!={'message_id','start','end','text'} or quote['message_id']!=a['mention']['message_id'] or type(lo) is not int or type(hi) is not int or not 0<=lo<hi<=len(src) or src[lo:hi]!=quote['text']: raise InvalidIdentity('exact relation source required')
   if any(not lo<=p['mention']['start']<p['mention']['end']<=hi for p in (a,b)): raise InvalidIdentity('relation quote must cover both endpoints')
   body=dict(left_id=left,right_id=right,quote=quote,kind=kind,scope=scope,event_time=event_time,recorded_at=a['source']['timestamp'],semantic_review='authored_fixture_or_manual_only')
   rid=digest(body); db.execute('INSERT OR IGNORE INTO source_relations VALUES(?,?,?)',(rid,json.dumps(body,ensure_ascii=False),digest(body))); return rid
 def relations(self,source_path,user_id,mid):
  resolved=self.resolve(source_path,user_id,mid); result=[]
  with closing(ro(self.path)) as db:
   for row in db.execute('SELECT * FROM source_relations ORDER BY id'):
    body=json.loads(row['body'])
    if body['left_id'] not in resolved['mention_ids'] and body['right_id'] not in resolved['mention_ids']: continue
    if digest(body)!=row['body_hash'] or row['id']!=digest(body): raise InvalidIdentity('relation changed')
    self._mention(db,body['left_id']); self._mention(db,body['right_id']); result.append(body)
  return result
