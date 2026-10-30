"""Atomic durable reservations; bounded network concurrency outside the ledger lock."""
from contextlib import closing
import json
import threading
import time

from memory_service import ApiError, digest, encode
from online_graph_r2_provider import effective_deadline, check_until, dispatch_gate
from online_graph_r2_retry_provider import ReconnectJournal

MAX_NATIVE_CONCURRENCY = 16


class ConcurrentCallJournal(ReconnectJournal):
    def __init__(self, path):
        super().__init__(path)
        self.native_slots = threading.BoundedSemaphore(MAX_NATIVE_CONCURRENCY)

    def call(self, provider, scope, kind, endpoint, body, output_tokens, invoke):
        until = effective_deadline(kind)
        key = digest([provider, scope, kind, endpoint, body])
        with dispatch_gate(self.logical_lock('native:' + key), until):
            with dispatch_gate(self.native_slots, until):
                with dispatch_gate(self.lock, until), closing(self.connect()) as db:
                    # Transaction also protects budgets if another journal object
                    # reaches this file. No network runs inside this transaction.
                    db.execute('BEGIN IMMEDIATE')
                    try:
                        check_until(until)
                        prior = db.execute('select state,raw_response from calls where id=?', (key,)).fetchone()
                        if prior:
                            if prior[0] == 'received':
                                return json.loads(prior[1])
                            raise ApiError(503, 'a prior provider attempt failed or was interrupted; numbered recovery required')
                        policy = json.loads(db.execute('select policy from budgets where name=?', (provider,)).fetchone()[0])
                        count, held = db.execute('select count(*),coalesce(sum(reserved),0) from calls where provider=?', (provider,)).fetchone()
                        input_bound = len(encode(body).encode('utf-8')) + 512
                        if kind == 'rerank':
                            n = len(body['input']['documents'])
                            input_bound += max(n - 1, 0) * len(body['input']['query'].encode('utf-8')) + n * 128
                        reserve = (input_bound * policy['input_per_million'] + output_tokens * policy['output_per_million']) / 1_000_000
                        if count >= policy['max_requests'] or held + reserve > policy['max_estimated_cost']:
                            raise ApiError(503, 'cumulative provider request or estimated cost cap reached')
                        check_until(until)
                        db.execute('insert into calls values(?,?,?,?,?,?,?,?,?,?,?)',
                                   (key, provider, scope, kind, endpoint, encode(body), 'inflight', None, reserve, None, time.time()))
                        db.commit()
                    except BaseException:
                        db.rollback()
                        raise
                try:
                    check_until(until)
                    response = invoke()
                    serialized = encode(response)
                except Exception as exc:
                    with closing(self.connect()) as db, db:
                        db.execute("update calls set state='failed',error=? where id=?", (type(exc).__name__, key))
                    raise ApiError(503, 'model provider attempt failed; original text and pending work are retained') from None
                with closing(self.connect()) as db, db:
                    db.execute("update calls set state='received',raw_response=? where id=?", (serialized, key))
                return response
