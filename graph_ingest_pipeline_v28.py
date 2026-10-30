"""New-message ingestion entry point: raw extraction -> identity resolution -> graph."""
from copy import deepcopy
from entity_identity_v1 import digest
from graph_block_reference_v4 import build_bundle,validate_bundle,normalize,SCHEMA
from graph_validation import schema_check
from resolved_graph_ingest_v28 import IdentityGraph,IdentityError

class GraphIngestPipeline:
 def __init__(self,path,identity_judge=None):self.graph=IdentityGraph(path,identity_judge)
 def add(self,bundle,extraction_output,origin=None):
  b=validate_bundle(bundle);schema_check(extraction_output,SCHEMA)
  normalized=normalize(b,extraction_output)
  metadata={'raw_extraction_sha256':digest(extraction_output),'raw_extraction':deepcopy(extraction_output),'source_rejected_candidates':deepcopy(normalized['rejected']),'partial_overflow':normalized['overflow'],'caller_origin':deepcopy(origin)}
  result=self.graph.ingest(b,normalized['candidates'],metadata)
  if result['cached'] and (result.get('origin') or {}).get('raw_extraction_sha256')!=metadata['raw_extraction_sha256']:raise IdentityError('cached extraction lineage differs; explicit new version required')
  return {**result,'source_rejected_candidates':normalized['rejected'],'partial_overflow':normalized['overflow'],'ingestion_order':['validate_source','normalize_extraction','find_entity_candidates','judge_source_identity','persist_entity_id_and_aliases','insert_original_relations']}
 def add_from_source(self,source_db,user_id,message_id,extraction_output,origin=None):return self.add(build_bundle(source_db,user_id,message_id),extraction_output,origin)
