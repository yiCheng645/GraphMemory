from contextlib import closing

import re

from entity_identity_v1 import ro

from graph_source import source_units

FAMILIES={
 'subscription_state':{'subscribe','subscribes','subscribed','subscription','subscriptions','renew','renews','renewed','cancel','cancels','cancelled','canceled','unsubscribe','unsubscribed'},
 'repair_replacement':{'repair','repairs','repaired','fix','fixes','fixed','replace','replaces','replaced','replacement','upgrade','upgraded','donate','donated'},
 'event_participation':{'attend','attends','attended','participate','participated','see','saw','seen','visit','visited','return','returned'},
 'project_responsibility':{'lead','leads','led','leading','manage','manages','managed','responsible','oversee','oversaw'},
 'acquisition':{'buy','buys','bought','purchase','purchased','acquire','acquired','receive','received','get','got','own','owns'}}

STOP=set('a an the i my me we our you your of in on at to for from and or as is are was were do did have has how many what which currently past months ago'.split())

def words(s):return re.findall('[a-z0-9]+',str(s).lower())

def families(predicate):
 w=set(words(predicate));return sorted(k for k,v in FAMILIES.items() if w&v)

def question_plan(question):
 w=set(words(question));out=[]
 if w&{'subscribe','subscribed','subscription','subscriptions'}:out.append('subscription_state')
 if w&{'repair','repaired','fix','fixed','replace','replaced','replacement','upgrade'}:out.append('repair_replacement')
 if w&{'concert','concerts','events','festival','festivals'} and w&{'attend','attended','participate','participated','order','earliest'}:out.append('event_participation')
 if w&{'project','projects'} and w&{'lead','led','leading','manage','managed','responsible'}:out.append('project_responsibility')
 if w&{'buy','bought','purchase','purchased','acquire','acquired','investment'}:out.append('acquisition')
 return {'families':sorted(set(out)),'query_terms':sorted(w-STOP),'direction':'match original directed predicates; never reverse subject/object','time_policy':'preserve recorded/event times, unknown stays unknown; no inferred dates','unrecognized':not bool(out),'router':'frozen English closed-family lexical prototype; not general semantic accuracy'}

def append_raw(baseline,hits,source,user_id,limit=20):
 if limit!=20 or len(baseline)!=100 or len({d['id'] for d in baseline})!=100:raise ValueError('fixed baseline100/extra20')
 seen={d['id'] for d in baseline};extra=[]
 with closing(ro(source)) as s:
  for h in hits:
   row=s.execute('select u.*,m.session_id,m.role,m.timestamp,m.content as full_content from units u join messages m on m.id=u.message_id and m.user_id=u.user_id where u.id=? and u.user_id=?',(h['id'],user_id)).fetchone()
   if row is None:raise ValueError('hit outside source')
   m={'id':row['message_id'],'content':row['full_content'],'session_id':row['session_id'],'role':row['role'],'timestamp':row['timestamp']}
   actual={u['id']:u for u in source_units(s,user_id,m,0,len(m['content']))}[h['id']]
   if h['content']!=actual['content']:raise ValueError('altered hit')
   if h['id'] in seen:continue
   a,b=actual['span'];extra.append({'id':h['id'],'type':'raw','content':h['content'],'spans':[{'message_id':m['id'],'start':a,'end':b,'text':m['content'][a:b]}]});seen.add(h['id'])
   if len(extra)==limit:break
 return baseline+extra
