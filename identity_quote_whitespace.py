"""Restore unique literal source spans for whitespace-only quote differences.

This does not validate or approve identity. The original validator must still run.
The native output is immutable; a copied decision and provenance are returned.
"""
from copy import deepcopy
import hashlib,re

def restore_literal_quotes(packet,output):
    result=deepcopy(output);audit=[]
    if not isinstance(result,dict) or set(result)!={'decision','reason','quotes'} or not isinstance(result['quotes'],list):
        return result,audit
    sources={side['message_id']:side['target']['content'] for side in (packet['current'],packet['reference'])}
    for index,quote in enumerate(result['quotes']):
        if not isinstance(quote,dict) or set(quote)!={'message_id','text'}:continue
        text=quote['text'];source=sources.get(quote['message_id'])
        if not isinstance(text,str) or not text or not isinstance(source,str) or text in source:continue
        words=text.split()
        if not words:continue
        # Escape all non-whitespace characters: case, punctuation and words stay exact.
        pattern=r'\s+'.join(re.escape(word) for word in words)
        matches=list(re.finditer(pattern,source))
        if len(matches)!=1:continue
        match=matches[0];literal=source[match.start():match.end()]
        if re.sub(r'\s','',literal)!=re.sub(r'\s','',text):continue
        quote['text']=literal
        audit.append({'quote_index':index,'message_id':quote['message_id'],'source_start':match.start(),'source_end':match.end(),
            'original_quote_sha256':hashlib.sha256(text.encode()).hexdigest(),
            'literal_quote_sha256':hashlib.sha256(literal.encode()).hexdigest(),'method':'unique_whitespace_only_source_span'})
    return result,audit


class SourceWhitespaceModelJudge:
    """Keep original prompts and validators; restore only unique whitespace spans."""
    def __init__(self,provider,user_id):self.provider,self.user_id=provider,user_id
    def __call__(self,packet):
        from resolved_graph_ingest_v28 import ModelCallbackJudge
        audit=[]
        def callback(body):
            original=self.provider.chat(self.user_id,'identity',body)
            restored,changes=restore_literal_quotes(packet,original)
            audit.extend(changes)
            return restored
        decision=ModelCallbackJudge(callback)(packet)
        if audit:decision['literal_quote_restoration']=audit
        return decision
