"""Apply the existing category compatibility rule before identity comparison."""
from resolved_graph_ingest_v28 import IdentityError, MAX_CANDIDATES, norm, words

def compatible_lookup(db, mention):
    category = mention['category']
    rows = list(db.execute('''select a.*,e.category from aliases a
        join entities e on e.id=a.entity_id
        where a.collection=? and a.user_id=?
          and (?='unknown' or e.category=? or e.category='unknown')
        order by a.entity_id,a.normalized limit 4097''',
        (mention['collection'],mention['user_id'],category,category)))
    if len(rows)>4096:
        raise IdentityError('bounded alias prototype exceeded; explicit indexed expansion needed')
    key=norm(mention['name']);tokens=words(mention['name']);scored={}
    for row in rows:
        exact=row['normalized']==key;overlap=len(tokens & words(row['alias']))
        if not exact and not overlap:continue
        score=(0 if exact else 1,-overlap,row['entity_id']);old=scored.get(row['entity_id'])
        if old is None or score<old[0]:scored[row['entity_id']]=(score,dict(row))
    ordered=[r for _,r in sorted(scored.values(),key=lambda x:x[0])]
    return ordered[:MAX_CANDIDATES],len(ordered)>MAX_CANDIDATES
