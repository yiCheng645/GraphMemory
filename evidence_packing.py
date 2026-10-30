import json

TOKEN_BUDGET = 8192

def count(text, enc):
    return len(enc.encode(text, disallowed_special=()))

def literal(span, messages):
    m = messages[span['message_id']]
    start, end = span['start'], span['end']
    if type(start) is not int or type(end) is not int or not 0 <= start < end <= len(m['content']):
        raise ValueError('Invalid citation bounds')
    if m['content'][start:end] != span['text']:
        raise ValueError('Nonliteral citation')
    return m

def source_text(span, messages):
    m = literal(span, messages)
    return json.dumps({'message_id': m['id'], 'role': m['role'], 'session_id': m['session_id'],
        'recorded_at_ms': m['timestamp_ms'], 'range': [span['start'], span['end']]}, ensure_ascii=False) + '\n' + span['text']

def remaining(span, intervals):
    pieces = [(span['start'], span['end'])]
    for a, b in intervals:
        next_pieces = []
        for x, y in pieces:
            if b <= x or a >= y:
                next_pieces.append((x, y))
            else:
                if x < a: next_pieces.append((x, a))
                if b < y: next_pieces.append((b, y))
        pieces = next_pieces
    return pieces

def pack(ranked, messages, enc, budget=TOKEN_BUDGET):
    covered, kept, skipped, text = {}, [], [], ''
    for doc in ranked:
        novel = []
        for span in doc['spans']:
            literal(span, messages)
            for a, b in remaining(span, covered.get(span['message_id'], [])):
                novel.append({'message_id': span['message_id'], 'start': a, 'end': b,
                              'text': messages[span['message_id']]['content'][a:b]})
        if not novel:
            skipped.append({'id': doc['id'], 'reason': 'sources_already_present'})
            continue
        block = '--- Evidence ' + doc['id'] + ' ---\n'
        if doc['type'] == 'derived':
            block += json.dumps(doc['annotation'], ensure_ascii=False) + '\n'
            block += 'Complete citation ranges (overlap may appear in earlier evidence): ' + json.dumps(
                [{k: s[k] for k in ('message_id', 'start', 'end')} for s in doc['spans']]) + '\n'
        block += '\n[... source gap or previously included text ...]\n'.join(source_text(s, messages) for s in novel)
        candidate = text + ('\n\n' if text else '') + block
        if count(candidate, enc) > budget:
            skipped.append({'id': doc['id'], 'reason': 'whole_remaining_block_exceeds_budget'})
            continue
        text = candidate
        kept.append({'id': doc['id'], 'type': doc['type'], 'spans': novel})
        for span in novel:
            covered.setdefault(span['message_id'], []).append((span['start'], span['end']))
    return {'text': text, 'tokens': count(text, enc), 'kept': kept, 'skipped': skipped}
