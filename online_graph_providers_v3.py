"""Explicit live-provider wiring, durable native receipts, conservative preflight caps.

Construction never calls a model. Failed or interrupted packets are never resent.
Credentials stay in memory; only endpoint, body, response and safe error class persist.
"""
from contextlib import closing
from pathlib import Path
import json, math, sqlite3, threading, time
import urllib.request, urllib.parse
from memory_service import ApiError, AlibabaEmbedding, encode, digest
from reranker import AlibabaReranker, INSTRUCTION, MODEL
from graph_block_reference_v4 import SYSTEM, RESPONSE_FORMAT, model_input
from resolved_graph_ingest_v28 import ModelCallbackJudge


class CallJournal:
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.RLock()
        with closing(self.connect()) as db, db:
            db.executescript('''
CREATE TABLE IF NOT EXISTS budgets(name TEXT PRIMARY KEY,policy TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS calls(id TEXT PRIMARY KEY,provider TEXT,scope TEXT,kind TEXT,
 endpoint TEXT,body TEXT,state TEXT,raw_response TEXT,reserved REAL,error TEXT,created REAL);
''')

    def connect(self):
        return sqlite3.connect(self.path, timeout=30)

    def register(self, name, policy):
        if set(policy) != {'currency', 'max_requests', 'max_estimated_cost',
                           'input_per_million', 'output_per_million'}:
            raise ValueError('explicit cumulative request/cost/rate policy required')
        if policy['currency'] not in ('USD', 'CNY') or type(policy['max_requests']) is not int or policy['max_requests'] <= 0:
            raise ValueError('positive cumulative request cap and currency required')
        for key in ('max_estimated_cost', 'input_per_million', 'output_per_million'):
            value = policy[key]
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError('positive finite cost caps and rates required')
        with self.lock, closing(self.connect()) as db, db:
            row = db.execute('select policy from budgets where name=?', (name,)).fetchone()
            if row and row[0] != encode(policy):
                raise ValueError('persisted cumulative provider budget differs; no implicit reset or increase')
            db.execute('insert or ignore into budgets values(?,?)', (name, encode(policy)))

    def call(self, provider, scope, kind, endpoint, body, output_tokens, invoke):
        key = digest([provider, scope, kind, endpoint, body])
        # Serialize reservation and dispatch. Each successful native packet has one receipt.
        with self.lock:
            with closing(self.connect()) as db:
                prior = db.execute('select state,raw_response from calls where id=?', (key,)).fetchone()
                if prior:
                    if prior[0] == 'received':
                        return json.loads(prior[1])
                    raise ApiError(503, 'a prior provider attempt failed or was interrupted; automatic resend is disabled')
                policy = json.loads(db.execute('select policy from budgets where name=?', (provider,)).fetchone()[0])
                count, held = db.execute('select count(*),coalesce(sum(reserved),0) from calls where provider=?', (provider,)).fetchone()
                # UTF-8 bytes plus framing is deliberately a conservative estimate,
                # not a claimed invoice. Rerank may charge the query per document.
                input_bound = len(encode(body).encode('utf-8')) + 512
                if kind == 'rerank':
                    n = len(body['input']['documents'])
                    input_bound += max(n - 1, 0) * len(body['input']['query'].encode('utf-8')) + n * 128
                reserve = (input_bound * policy['input_per_million'] + output_tokens * policy['output_per_million']) / 1_000_000
                if count >= policy['max_requests'] or held + reserve > policy['max_estimated_cost']:
                    raise ApiError(503, 'cumulative provider request or estimated cost cap reached')
                with db:
                    db.execute('insert into calls values(?,?,?,?,?,?,?,?,?,?,?)',
                               (key, provider, scope, kind, endpoint, encode(body), 'inflight', None, reserve, None, time.time()))
            try:
                response = invoke()
                serialized = encode(response)
            except Exception as exc:
                with closing(self.connect()) as db, db:
                    db.execute("update calls set state='failed',error=? where id=?", (type(exc).__name__, key))
                raise ApiError(503, 'model provider attempt failed; original text and pending work are retained') from None
            with closing(self.connect()) as db, db:
                db.execute("update calls set state='received',raw_response=? where id=?", (serialized, key))
            return response

    def summary(self):
        with closing(self.connect()) as db:
            return [{'provider': r[0], 'attempts': r[1], 'reserved_estimate': r[2], 'failed_or_interrupted': r[3]}
                    for r in db.execute("select provider,count(*),sum(reserved),sum(state!='received') from calls group by provider")]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class JsonProvider:
    def __init__(self, config, journal, name, *, api_key, test_loopback=False):
        self.config, self.journal, self.name = config, journal, name
        self.endpoint, self.key = config['endpoint'], api_key
        parsed = urllib.parse.urlsplit(self.endpoint)
        local_test = test_loopback and parsed.scheme == 'http' and parsed.hostname == '127.0.0.1'
        if (not (parsed.scheme == 'https' or local_test) or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment or not api_key):
            raise ValueError('explicit HTTPS provider endpoint and credential required')
        journal.register(name, config['budget'])
        self.identity = digest([name, self.endpoint, {k: v for k, v in config.items() if k not in ('budget', 'api_key_env')}])

    def post(self, scope, kind, body, output_tokens=0):
        if len(encode(body).encode('utf-8')) > self.config.get('max_input_bytes', 1_048_576):
            raise ApiError(413, 'provider request exceeds configured input byte limit')

        def invoke():
            request = urllib.request.Request(self.endpoint, data=encode(body).encode('utf-8'),
                        headers={'Authorization': 'Bearer ' + self.key, 'Content-Type': 'application/json'})
            with urllib.request.build_opener(_NoRedirect()).open(request, timeout=30) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
                if len(raw) > 4 * 1024 * 1024:
                    raise ValueError('provider response exceeds receipt limit')
                return json.loads(raw)
        return self.journal.call(self.name, scope, kind, self.endpoint, body, output_tokens, invoke)

    def chat(self, scope, kind, body):
        body = {**body, 'model': self.config['model']}
        output = self.post(scope, kind, body, body['max_tokens'])
        try:
            if not isinstance(output, dict) or ('model' in output and output['model'] != self.config['model']):
                raise ValueError('reported model differs from explicit configuration')
            choice = output['choices'][0]
            if choice['finish_reason'] != 'stop':
                raise ValueError('nonterminal chat completion')
            text = choice['message']['content']
            if not isinstance(text, str) or not text.strip():
                raise ValueError('missing chat content')
            return json.loads(text)
        except (KeyError, IndexError, TypeError, ValueError):
            raise ApiError(503, 'invalid or truncated model output; native response retained and not resent') from None


class LiveExtraction:
    def __init__(self, provider, user_id):
        self.provider, self.user_id = provider, user_id
        self.identity = 'live-block-reference:' + provider.identity

    def __call__(self, bundle):
        body = {'model': self.provider.config['model'], 'temperature': 0,
                'max_tokens': self.provider.config.get('max_output_tokens', 1600), 'stream': False,
                'messages': [{'role': 'system', 'content': SYSTEM},
                             {'role': 'user', 'content': encode(model_input(bundle))}],
                'response_format': RESPONSE_FORMAT}
        return self.provider.chat(self.user_id, 'extract', body)


def live_judge(provider, user_id):
    from identity_quote_whitespace import SourceWhitespaceModelJudge
    return SourceWhitespaceModelJudge(provider, user_id)


class BudgetedEmbedding:
    def __init__(self, provider, user_id):
        self.provider, self.user_id = provider, user_id
        self.dimension = provider.config.get('dimension', 1024)
        # Validate the actual native production endpoint/configuration without dispatch.
        AlibabaEmbedding(provider.endpoint, provider.key, self.dimension)
        self.identity = f'text-embedding-v4:{self.dimension}:{provider.endpoint}'

    def embed(self, texts, text_type):
        vectors = []
        for start in range(0, len(texts), 10):
            batch = texts[start:start + 10]
            body = {'model': 'text-embedding-v4', 'input': {'texts': batch},
                    'parameters': {'dimension': self.dimension, 'text_type': text_type, 'output_type': 'dense'}}
            raw = self.provider.post(self.user_id, 'embedding:' + text_type, body)
            vectors.extend(AlibabaEmbedding.parse(self, raw, len(batch)))
        return vectors


class BudgetedReranker:
    def __init__(self, provider, user_id):
        self.provider, self.user_id = provider, user_id
        AlibabaReranker(provider.endpoint, provider.key, model=MODEL)

    def rerank(self, query, documents, top_n):
        body = {'model': MODEL, 'input': {'query': query, 'documents': documents},
                'parameters': {'top_n': min(top_n, len(documents)), 'instruct': INSTRUCTION}}
        raw = self.provider.post(self.user_id, 'rerank', body)
        return AlibabaReranker.parse(raw, len(documents), top_n)
