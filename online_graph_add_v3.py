"""Writable Add -> vectors -> raw-user extraction -> v28 identity/graph -> v2 Search.

This is an independent live data generation. Frozen corpora and services are never
opened for writing. One process owns a root; a user lock covers Add and Search.
"""
from contextlib import closing
from copy import deepcopy
from pathlib import Path
import json, os, sqlite3, threading, time
from memory_service import MemoryStore, ApiError, require_text, digest, encode, chunks
from entity_identity_v1 import ROOT, sha, ro
from graph_block_reference_v4 import build_bundle, validate_bundle
from graph_ingest_pipeline_v28 import GraphIngestPipeline
from resolved_graph_ingest_v28 import IdentityGraph, ResolvedGraph, CatalogJudge
from role_graph_system_v2 import RoleGraphStore
from graph_model_degradation import GraphModelExhausted,commit_degraded,recover_degraded

VERSION = 'direct-graph-v3-online-add-v1'
LIVE_ROOT = ROOT / 'data/live/online_graph_v3'


def safe_root(path):
    path = Path(path).resolve()
    if not path.is_relative_to(LIVE_ROOT) or path == LIVE_ROOT:
        raise ValueError('new isolated instance under data/live/online_graph_v3 required')
    return path


def save_atomic(path, value):
    temporary = path.with_suffix(path.suffix + '.pending')
    with temporary.open('w', encoding='utf-8', newline='\n') as stream:
        stream.write(encode(value) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


class InstanceLock:
    def __init__(self, root):
        self.file = (root / 'instance.lock').open('a+b')
        try:
            self.file.seek(0)
            if self.file.read(1) == b'':
                self.file.write(b'1')
                self.file.flush()
            self.file.seek(0)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise ValueError('another online process owns this data root') from None

    def close(self):
        if not self.file.closed:
            self.file.close()


class OnlineIdentityGraph(IdentityGraph):
    """Only substitute a new safe live path; reuse all frozen v28 policy/SQL checks."""
    def __init__(self, path, judge=None):
        self.path = Path(path).resolve()
        if not self.path.is_relative_to(LIVE_ROOT) or self.path.name != 'graph.sqlite3':
            raise ValueError('isolated online graph path required')
        self.judge = judge if judge is not None else CatalogJudge()


    def lookup(self, db, mention):
        from category_candidate_lookup import compatible_lookup
        return compatible_lookup(db, mention)


class OnlinePipeline(GraphIngestPipeline):
    def __init__(self, path, judge):
        self.graph = OnlineIdentityGraph(path, judge)


def validate_add(body):
    if not isinstance(body, dict):
        raise ApiError(422, 'request must be an object')
    uid, request, session = [require_text(body, key) for key in ('user_id', 'request_id', 'session_id')]
    messages = body.get('messages')
    if not isinstance(messages, list) or not messages:
        raise ApiError(422, 'messages must be a nonempty array')
    count = 0
    for message in messages:
        if not isinstance(message, dict) or message.get('role') not in ('user', 'assistant'):
            raise ApiError(422, 'message role must be user or assistant')
        text = require_text(message, 'content')
        if 'timestamp' in message and (type(message['timestamp']) is not int or not 0 <= message['timestamp'] <= 253402300799999):
            raise ApiError(422, 'timestamp must be Unix milliseconds in years 1970-9999')
        count += sum(1 for _ in chunks(text))
        if count > 256:
            raise ApiError(413, 'at most 256 chunks per Add')
    return uid, request, session


class OnlineUserStore:
    def __init__(self, directory, user_id, embedding, reranker, encoder, extractor,
                 judge=None, *, strict_graph=False, fault=None):
        self.directory, self.user_id = directory, user_id
        self.lock = threading.RLock()
        self.path, self.graph_path = directory / 'source.sqlite3', directory / 'graph.sqlite3'
        self.job_path, self.publication_path = directory / 'jobs.sqlite3', directory / 'publication.json'
        self.extractor, self.fault = extractor, fault
        self.base = MemoryStore(self.path, embedding)
        self.pipeline = OnlinePipeline(self.graph_path, judge)
        self.pipeline.graph.create()
        self.route = RoleGraphStore(self.base, reranker, encoder, strict_graph=strict_graph, readonly=False)
        with closing(self.jobs()) as db, db:
            db.executescript('''
CREATE TABLE IF NOT EXISTS requests(request_id TEXT PRIMARY KEY,payload_hash TEXT,body TEXT,state TEXT,
 text_receipt TEXT,receipt TEXT,error TEXT,updated REAL);
CREATE TABLE IF NOT EXISTS messages(message_id TEXT PRIMARY KEY,request_id TEXT,role TEXT,state TEXT,
 bundle TEXT,extraction TEXT,result TEXT);
CREATE TABLE IF NOT EXISTS publications(generation INTEGER PRIMARY KEY,body TEXT NOT NULL);
''')
        self.recover_local()

    def jobs(self):
        db = sqlite3.connect(self.job_path, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def check_user(self, body):
        if not isinstance(body, dict) or require_text(body, 'user_id') != self.user_id:
            raise ApiError(404, 'no source binding for this user')

    def _fault(self, stage):
        if self.fault:
            self.fault(stage)

    def _text_receipt(self, request):
        with closing(self.base.connect()) as db:
            row = db.execute('select receipt from requests where user_id=? and request_id=?', (self.user_id, request)).fetchone()
        return json.loads(row[0]) if row else None

    def _update_message(self, mid, **values):
        with closing(self.jobs()) as db, db:
            db.execute('update messages set ' + ','.join(k + '=?' for k in values) + ' where message_id=?', (*values.values(), mid))

    def add(self, body):
        self.check_user(body)
        uid, request, _ = validate_add(body)
        with self.lock:
            with closing(self.jobs()) as db, db:
                old = db.execute('select * from requests where request_id=?', (request,)).fetchone()
                if old and old['payload_hash'] != digest(body):
                    raise ApiError(422, 'request_id was already used with different content')
                if old and old['state'] == 'complete':
                    return json.loads(old['receipt'])
                if not old:
                    db.execute('insert into requests values(?,?,?,?,?,?,?,?)',
                               (request, digest(body), encode(body), 'pending', None, None, None, time.time()))
                    for index, message in enumerate(body['messages']):
                        db.execute('insert into messages values(?,?,?,?,?,?,?)',
                                   (digest([uid, request, index]), request, message['role'], 'pending', None, None, None))
            try:
                text_receipt = self.base.add(body)
                with closing(self.jobs()) as db, db:
                    db.execute("update requests set text_receipt=?,state='text_written',error=NULL,updated=? where request_id=?",
                               (encode(text_receipt), time.time(), request))
                self._fault('after_text')
                # Original message order preserves same-session preceding context.
                for index, message in enumerate(body['messages']):
                    mid = digest([uid, request, index])
                    with closing(self.jobs()) as db:
                        job = db.execute('select * from messages where message_id=?', (mid,)).fetchone()
                    if job['state'] in ('graph_committed', 'graph_degraded', 'assistant_skipped'):
                        continue
                    if message['role'] == 'assistant':
                        self._update_message(mid, state='assistant_skipped')
                        continue
                    bundle = build_bundle(self.path, uid, mid)
                    if recover_degraded(self, job):
                        continue
                    try:
                        if job['extraction']:
                            if json.loads(job['bundle']) != bundle:
                                raise ValueError('pending source bundle changed')
                            extraction = json.loads(job['extraction'])
                        else:
                            extraction = self.extractor(deepcopy(bundle))
                            # Persist successful raw output before normalization/identity.
                            self._update_message(mid, state='extracted', bundle=encode(bundle), extraction=encode(extraction))
                        self._fault('after_extraction')
                        result = self.pipeline.add(bundle, extraction, {'route': VERSION, 'request_id': request})
                        self._fault('after_graph_commit')
                        self._update_message(mid, state='graph_committed', result=encode(result))
                    except GraphModelExhausted as exc:
                        with closing(self.jobs()) as db:
                            saved = db.execute('select * from messages where message_id=?', (mid,)).fetchone()
                        commit_degraded(self, bundle, saved, exc)
                self._fault('before_publish')
                publication = self._publish()
                return self._complete_request(request, publication)
            except Exception as exc:
                with closing(self.jobs()) as db, db:
                    db.execute("update requests set state='pending',error=?,updated=? where request_id=?",
                               (type(exc).__name__, time.time(), request))
                if isinstance(exc, ApiError):
                    raise
                raise ApiError(503, 'Add is incomplete; saved text and graph work are retained for the same request_id') from None

    def _publish(self):
        # A full binding requires every stored user message to have a real graph job.
        with closing(ro(self.path)) as source, closing(ro(self.graph_path)) as graph:
            user_messages = list(source.execute("select id from messages where user_id=? and role='user' order by rowid", (self.user_id,)))
            for message in user_messages:
                bundle = build_bundle(self.path, self.user_id, message['id'])
                row = graph.execute('select bundle from sources where collection=? and message_id=?', (bundle['collection_id'], message['id'])).fetchone()
                if row is None or json.loads(row['bundle']) != bundle:
                    raise ValueError('graph coverage incomplete or source binding changed')
        with closing(self.jobs()) as db:
            partial = 0
            degraded = 0
            for row in db.execute("select result from messages where state in ('graph_committed','graph_degraded')"):
                result = json.loads(row[0])
                origin = result.get('origin') or {}
                degraded += bool(origin.get('graph_degradation'))
                partial += bool(origin.get('graph_degradation') or result.get('source_rejected_candidates') or result.get('partial_overflow')
                                or origin.get('source_rejected_candidates') or origin.get('partial_overflow'))
            previous = db.execute('select max(generation) from publications').fetchone()[0] or 0
        publication = {'route': VERSION, 'user_id': self.user_id, 'generation': previous + 1,
                       'source_sha256': sha(self.path), 'graph_sha256': sha(self.graph_path),
                       'user_messages': len(user_messages), 'partial_messages': partial, 'degraded_messages': degraded}
        # Keep a durable generation history before atomically advancing current binding.
        with closing(self.jobs()) as db, db:
            db.execute('insert into publications values(?,?)', (publication['generation'], encode(publication)))
        save_atomic(self.publication_path, publication)
        return publication

    def _complete_request(self, request, publication):
        with closing(self.jobs()) as db, db:
            row = db.execute('select text_receipt from requests where request_id=?', (request,)).fetchone()
            receipt = {**json.loads(row[0]), 'text_status': 'written',
                       'graph_status': 'partial' if publication['partial_messages'] else 'indexed',
                       'graph_version': publication['generation'], 'graph_degraded_messages': publication.get('degraded_messages',0)}
            db.execute("update requests set state='complete',receipt=?,error=NULL,updated=? where request_id=?",
                       (encode(receipt), time.time(), request))
        return receipt

    def recover_local(self):
        """Only reconcile committed local stages. Startup never invokes a provider."""
        with self.lock, closing(self.jobs()) as db:
            requests = list(db.execute("select * from requests where state!='complete' order by rowid"))
        for request in requests:
            text_receipt = self._text_receipt(request['request_id'])
            if text_receipt is None:
                continue
            with closing(self.jobs()) as db, db:
                db.execute('update requests set text_receipt=? where request_id=?', (encode(text_receipt), request['request_id']))
                messages = list(db.execute('select * from messages where request_id=? order by rowid', (request['request_id'],)))
            complete = True
            for job in messages:
                if job['role'] == 'assistant':
                    self._update_message(job['message_id'], state='assistant_skipped')
                elif recover_degraded(self, job):
                    continue
                elif job['extraction']:
                    bundle = build_bundle(self.path, self.user_id, job['message_id'])
                    if json.loads(job['bundle']) != bundle:
                        complete = False
                        continue
                    with closing(ro(self.graph_path)) as graph:
                        found = graph.execute('select result from ingestion_jobs where collection=? and message_id=?', (bundle['collection_id'], job['message_id'])).fetchone()
                    if found:
                        result = json.loads(found['result'])
                        if (result.get('origin') or {}).get('raw_extraction_sha256') == digest(json.loads(job['extraction'])):
                            self._update_message(job['message_id'], state='graph_committed', result=encode(result))
                        else:
                            complete = False
                    else:
                        complete = False
                else:
                    complete = False
            if complete:
                try:
                    self._complete_request(request['request_id'], self._publish())
                except (ValueError, OSError, sqlite3.Error):
                    # Another pending request can make the whole source incomplete.
                    pass

    def search_with_trace(self, body):
        self.check_user(body)
        with self.lock:
            publication = None
            if self.publication_path.is_file():
                try:
                    candidate = json.loads(self.publication_path.read_text(encoding='utf-8'))
                    if (candidate['route'] == VERSION and candidate['user_id'] == self.user_id
                            and candidate['source_sha256'] == sha(self.path)
                            and candidate['graph_sha256'] == sha(self.graph_path)):
                        publication = candidate
                except (ValueError, KeyError, OSError, TypeError):
                    pass
            self.route.graph_provider = ResolvedGraph(self.graph_path, publication['graph_sha256']) if publication else None
            self.route.source_sha256 = publication['source_sha256'] if publication else None
            response, trace = self.route.search_with_trace(body)
            response['meta'].update(route=VERSION, graph_index_status=('partial' if publication['partial_messages'] else 'current') if publication else 'pending',
                                    graph_version=publication['generation'] if publication else None)
            return response, trace

    def search(self, body):
        return self.search_with_trace(body)[0]


class OnlineRouter:
    def __init__(self, root, user_ids, factories, encoder, *, policy_id, strict_graph=False, fault=None):
        self.root = safe_root(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.instance_lock = InstanceLock(self.root)
        self.routes = {}
        try:
            modules = ['online_graph_add_v3.py', 'online_graph_providers_v3.py', 'memory_service.py',
                       'graph_ingest_pipeline_v28.py', 'resolved_graph_ingest_v28.py',
                       'graph_block_reference_v4.py', 'direct_graph_role_entry_v2.py', 'role_graph_system_v2.py']
            policy = {'version': VERSION, 'policy_id': policy_id, 'users': sorted(user_ids),
                      'code': {name: sha(ROOT / name) for name in modules}}
            path = self.root / 'policy.json'
            if path.exists() and json.loads(path.read_text(encoding='utf-8')) != policy:
                raise ValueError('live source policy/configuration differs; explicit migration required')
            if not path.exists():
                save_atomic(path, policy)
            for uid in user_ids:
                directory = self.root / digest(['user', uid])
                directory.mkdir(exist_ok=True)
                providers = factories(uid)
                self.routes[uid] = OnlineUserStore(directory, uid, encoder=encoder, strict_graph=strict_graph,
                                                  fault=fault, **providers)
        except Exception:
            self.close()
            raise

    def close(self):
        self.instance_lock.close()

    def _route(self, body):
        if not isinstance(body, dict):
            raise ApiError(422, 'request must be an object')
        uid = require_text(body, 'user_id')
        if uid not in self.routes:
            raise ApiError(404, 'no source binding for this user')
        return self.routes[uid]

    def add(self, body):
        return self._route(body).add(body)

    def search(self, body):
        return self._route(body).search(body)

    def health(self):
        pending = 0
        for store in self.routes.values():
            with closing(store.jobs()) as db:
                pending += db.execute("select count(*) from requests where state!='complete'").fetchone()[0]
        return {'status': 'ok', 'route': VERSION, 'automatic_graph_extraction': True,
                'user_count': len(self.routes), 'pending_add_requests': pending}
