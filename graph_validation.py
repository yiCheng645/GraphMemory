from copy import deepcopy

from graph_block_reference_v4 import normalize

class ReviewError(ValueError):pass

def raw_record(candidate):
 try:
  return {"subject":{k:candidate["subject"][k] for k in ("name","category")},"relation":candidate["relation"],"object":None if candidate["object"] is None else {k:candidate["object"][k] for k in ("name","category")},"kind":candidate["kind"],"scope":candidate["scope"],"time_expression":candidate["time_expression"],"source_block_ids":list(candidate["source_block_ids"])}
 except (KeyError,TypeError) as exc:raise ReviewError("complete normalized candidate required") from exc

def checked_candidate(bundle,candidate):
 if not isinstance(candidate,dict):raise ReviewError("normalized candidate required")
 result=normalize(bundle,{"source_type":"personal","relations":[raw_record(candidate)],"overflow":False})
 if len(result["candidates"])!=1 or result["rejected"]:raise ReviewError("candidate failed source validation")
 expected=result["candidates"][0]
 if set(candidate)!=set(expected) or any(candidate[k]!=v for k,v in expected.items() if k!="input_index"):raise ReviewError("candidate content or source snapshot changed")
 if type(candidate["input_index"]) is not int or candidate["input_index"]<0:raise ReviewError("candidate origin index required")
 return deepcopy(candidate)

def schema_check(value,schema):
 if "anyOf" in schema:
  for option in schema["anyOf"]:
   try:schema_check(value,option);return
   except ValueError:pass
  raise ValueError("anyOf mismatch")
 typ=schema["type"]
 valid={"object":isinstance(value,dict),"array":isinstance(value,list),"string":isinstance(value,str),"boolean":type(value) is bool,"null":value is None}.get(typ,False)
 if not valid:raise ValueError("JSON schema type mismatch")
 if "enum" in schema and value not in schema["enum"]:raise ValueError("JSON schema enum mismatch")
 if typ=="object":
  if set(value)!=set(schema["required"]):raise ValueError("JSON schema object keys mismatch")
  for k,v in value.items():schema_check(v,schema["properties"][k])
 elif typ=="array":
  for item in value:schema_check(item,schema["items"])
