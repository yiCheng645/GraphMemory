"""Text-only local baseline. Python 3.10+, standard library only."""
from __future__ import annotations

from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
from pathlib import Path
import re
import sqlite3
import struct
import threading
import urllib.error
import urllib.request


class ApiError(Exception):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason
        super().__init__(reason)


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(encode(value).encode("utf-8")).hexdigest()


def tokens(text):
    parts = re.findall(r"[a-z0-9_]+|[\u3400-\u9fff]|[^\W\d_]", text.lower())
    return parts + [a + b for a, b in zip(parts, parts[1:]) if len(a) == len(b) == 1]


def chunks(text):
    start = 0
    while start < len(text):
        end = min(start + 600, len(text))
        yield start, end, text[start:end]
        if end == len(text):
            break
        start = end - 60


def unit_vector(vector, dimension):
    if (not isinstance(vector, list) or len(vector) != dimension or
            any(type(v) not in (int, float) or not math.isfinite(v) for v in vector)):
        raise ApiError(503, "embedding response has invalid dimensions or values")
    norm = math.hypot(*vector)
    if not math.isfinite(norm) or norm == 0:
        raise ApiError(503, "embedding response contains a zero or invalid vector")
    return [v / norm for v in vector]


class DemoEmbedding:
    """Deterministic lexical hash, NOT a semantic model or contest submission."""
    dimension = 128
    identity = "demo-lexical-hash-v1:128"

    def embed(self, texts, text_type):
        result = []
        for text in texts:
            vector = [0.0] * self.dimension
            for term, count in Counter(tokens(text) or [text]).items():
                number = int.from_bytes(hashlib.sha256(term.encode()).digest()[:8], "big")
                vector[number % self.dimension] += count
            result.append(unit_vector(vector, self.dimension))
        return result


class AlibabaEmbedding:
    def __init__(self, endpoint, api_key, dimension=1024):
        if not endpoint.startswith("https://") or "{" in endpoint or not api_key:
            raise ValueError("Set a real HTTPS EMBEDDING_ENDPOINT and DASHSCOPE_API_KEY in .env")
        if dimension not in (64, 128, 256, 512, 768, 1024, 1536, 2048):
            raise ValueError("Unsupported text-embedding-v4 dimension")
        self.endpoint, self.api_key, self.dimension = endpoint, api_key, dimension
        self.identity = f"text-embedding-v4:{dimension}:{endpoint}"
        self.total_tokens = 0
        self.usage_lock = threading.Lock()

    def parse(self, payload, count):
        try:
            rows = payload["output"]["embeddings"]
            if not isinstance(rows, list) or len(rows) != count:
                raise ValueError()
            ordered = [None] * count
            for row in rows:
                index = row["text_index"]
                if type(index) is not int or not 0 <= index < count or ordered[index] is not None:
                    raise ValueError()
                ordered[index] = unit_vector(row["embedding"], self.dimension)
            return ordered
        except (KeyError, TypeError, ValueError, IndexError):
            raise ApiError(503, "embedding response format is invalid") from None

    def embed(self, texts, text_type):
        vectors = []
        for start in range(0, len(texts), 10):
            batch = texts[start:start + 10]
            body = {"model": "text-embedding-v4", "input": {"texts": batch},
                    "parameters": {"dimension": self.dimension, "text_type": text_type, "output_type": "dense"}}
            request = urllib.request.Request(self.endpoint, data=encode(body).encode(), headers={
                "Authorization": "Bearer " + self.api_key, "Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    payload = json.loads(response.read())
            except urllib.error.HTTPError as exc:
                raise ApiError(503, f"embedding provider returned HTTP {exc.code}; check local configuration/quota") from None
            except (urllib.error.URLError, TimeoutError, OSError, ValueError):
                raise ApiError(503, "embedding provider unavailable or invalid JSON") from None
            vectors.extend(self.parse(payload, len(batch)))
            used = payload.get("usage", {}).get("total_tokens", 0)
            if type(used) is int and used >= 0:
                with self.usage_lock:
                    self.total_tokens += used
        return vectors


def require_text(body, name):
    value = body.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ApiError(422, f"{name} must be a nonempty string")
    # Reject lone UTF-16 surrogate code points, which cannot be stored as UTF-8.
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ApiError(422, f"{name} contains invalid Unicode") from None
    return value


class MemoryStore:
    def __init__(self, path, embedding):
        self.path, self.embedding = str(path), embedding
        self.write_lock = threading.Lock()
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as db, db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS requests (
                    user_id TEXT, request_id TEXT, payload_hash TEXT NOT NULL, receipt TEXT NOT NULL,
                    PRIMARY KEY(user_id, request_id));
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY, user_id TEXT NOT NULL, session_id TEXT NOT NULL,
                    role TEXT NOT NULL, content TEXT NOT NULL, timestamp INTEGER);
                CREATE TABLE IF NOT EXISTS units (
                    id TEXT PRIMARY KEY, user_id TEXT NOT NULL, message_id TEXT NOT NULL,
                    content TEXT NOT NULL, terms TEXT NOT NULL, vector BLOB NOT NULL);
                CREATE INDEX IF NOT EXISTS units_by_user ON units(user_id);
            """)
            fingerprint = "schema1:chunk600-overlap60:tokens1:" + embedding.identity
            saved = db.execute("SELECT value FROM meta WHERE key='fingerprint'").fetchone()
            if saved and saved[0] != fingerprint:
                raise ValueError("Database uses another embedding/configuration; choose a new database path")
            db.execute("INSERT OR IGNORE INTO meta VALUES ('fingerprint',?)", (fingerprint,))

    def connect(self):
        return sqlite3.connect(self.path, timeout=30)

    def add(self, body):
        if not isinstance(body, dict):
            raise ApiError(422, "request must be an object")
        user, request, session = [require_text(body, key) for key in ("user_id", "request_id", "session_id")]
        messages = body.get("messages")
        if not isinstance(messages, list) or not messages:
            raise ApiError(422, "messages must be a nonempty array")
        prepared = []
        for index, message in enumerate(messages):
            if not isinstance(message, dict) or message.get("role") not in ("user", "assistant"):
                raise ApiError(422, "message role must be user or assistant")
            content = require_text(message, "content")
            timestamp = message.get("timestamp")
            if "timestamp" in message and (type(timestamp) is not int or not 0 <= timestamp <= 253402300799999):
                raise ApiError(422, "timestamp must be a nonnegative Unix millisecond integer in years 1970-9999")
            message_id = digest([user, request, index])
            prepared.append((message_id, user, session, message["role"], content, timestamp))
        payload_hash = digest(body)
        # One process per database. Serialize Add so concurrent retries do not embed twice.
        if not self.write_lock.acquire(timeout=1):
            raise ApiError(429, "another write is in progress; retry this request")
        try:
            with closing(self.connect()) as db:
                old = db.execute("SELECT payload_hash,receipt FROM requests WHERE user_id=? AND request_id=?", (user, request)).fetchone()
            if old:
                if old[0] != payload_hash:
                    raise ApiError(422, "request_id was already used with different content")
                return json.loads(old[1])
            units = []
            for message_id, _, _, _, content, _ in prepared:
                for start, end, excerpt in chunks(content):
                    units.append((digest([message_id, start, end]), message_id, excerpt))
                    if len(units) > 256:
                        raise ApiError(413, "local baseline allows 256 chunks per Add; split into smaller requests")
            vectors = self.embedding.embed([u[2] for u in units], "document")
            if len(vectors) != len(units):
                raise ApiError(503, "embedding count mismatch")
            vectors = [unit_vector(v, self.embedding.dimension) for v in vectors]
            receipt = dict(success=True, request_id=request, user_id=user, session_id=session)
            with closing(self.connect()) as db, db:
                db.executemany("INSERT INTO messages VALUES (?,?,?,?,?,?)", prepared)
                for (unit_id, message_id, excerpt), vector in zip(units, vectors):
                    db.execute("INSERT INTO units VALUES (?,?,?,?,?,?)", (unit_id, user, message_id,
                               excerpt, encode(Counter(tokens(excerpt))), struct.pack(f'<{len(vector)}f', *vector)))
                db.execute("INSERT INTO requests VALUES (?,?,?,?)", (user, request, payload_hash, encode(receipt)))
            return receipt
        finally:
            self.write_lock.release()

    def search(self, body):
        if not isinstance(body, dict):
            raise ApiError(422, "request must be an object")
        user, query = require_text(body, "user_id"), require_text(body, "query")
        count = body.get("top_k", 10)
        if type(count) is not int or not 1 <= count <= 100:
            raise ApiError(422, "top_k must be an integer between 1 and 100")
        options = body.get("options", [])
        if not isinstance(options, list) or any(not isinstance(x, str) for x in options):
            raise ApiError(422, "options must be an array of strings")
        for option in options:
            try:
                option.encode("utf-8")
            except UnicodeEncodeError:
                raise ApiError(422, "options contains invalid Unicode") from None
        search_text = query + ("\n" + "\n".join(options) if options else "")
        if len(search_text) > 8192:
            raise ApiError(413, "local baseline allows 8192 query/options characters")
        with closing(self.connect()) as db:
            # One SELECT sees a consistent snapshot, including all committed sessions.
            rows = db.execute("""SELECT u.id,u.content,u.terms,u.vector,m.session_id,m.role,m.timestamp
                               FROM units u JOIN messages m ON u.message_id=m.id
                               WHERE u.user_id=? ORDER BY u.id""", (user,)).fetchall()
        if not rows:
            return {"data": []}
        query_vectors = self.embedding.embed([c[2] for c in chunks(search_text)], "query")
        vector = unit_vector([sum(values) / len(query_vectors) for values in zip(*query_vectors)], self.embedding.dimension)
        terms = [Counter(json.loads(row[2])) for row in rows]
        lengths = [sum(t.values()) for t in terms]
        average = max(sum(lengths) / len(rows), 1)
        qterms = set(tokens(search_text))
        df = {term: sum(term in t for t in terms) for term in qterms}
        lexical, dense = [], []
        for i, row in enumerate(rows):
            score = 0.0
            for term in qterms:
                tf = terms[i][term]
                if tf:
                    idf = math.log(1 + (len(rows) - df[term] + 0.5) / (df[term] + 0.5))
                    score += idf * tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * lengths[i] / average))
            lexical.append(score)
            stored = struct.unpack(f'<{self.embedding.dimension}f', row[3])
            dense.append(sum(a * b for a, b in zip(vector, stored)))
        fused = Counter()
        for scores in (lexical, dense):
            ranked = sorted((i for i, score in enumerate(scores) if score > 0), key=lambda i: (-scores[i], rows[i][0]))[:200]
            for rank, i in enumerate(ranked, 1):
                fused[i] += 1 / (60 + rank)
        data = []
        for i in sorted(fused, key=lambda i: (-fused[i], rows[i][0]))[:count]:
            unit_id, content, _, _, session, role, timestamp = rows[i]
            source = {"session_id": session, "role": role}
            if timestamp is not None:
                source["timestamp_ms"] = timestamp
            data.append({"id": unit_id, "content": "[source " + encode(source) + "]\n" + content, "score": fused[i]})
        return {"data": data}


def make_server(store, host="127.0.0.1", port=8000, api_key="local-demo-only"):
    if not api_key:
        raise ValueError("A service API key is required")

    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(30)

        def log_message(self, format, *args):
            pass  # Do not log chat contents, query strings or credentials.

        def respond(self, status, body):
            payload = encode(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            if status in (429, 503):
                self.send_header("Retry-After", "2")
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path == "/healthz":
                self.respond(200, {"status": "ok"})
            else:
                self.respond(404, {"detail": {"reason": "route not found"}})

        def do_POST(self):
            try:
                supplied = self.headers.get("Authorization", "")
                expected = "Bearer " + api_key
                if not hmac.compare_digest(supplied.encode(), expected.encode()):
                    raise ApiError(401, "invalid service Bearer token")
                method = {"/v1/add": store.add, "/v1/search": store.search}.get(self.path)
                if method is None:
                    raise ApiError(404, "route not found")
                if self.headers.get_content_type() != "application/json":
                    raise ApiError(415, "Content-Type must be application/json")
                if self.headers.get("Transfer-Encoding"):
                    raise ApiError(400, "chunked transfer is not supported by this local baseline")
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    raise ApiError(400, "invalid Content-Length") from None
                if not 0 < length <= 1024 * 1024:
                    raise ApiError(413, "body must be between 1 byte and 1 MiB")
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise ApiError(400, "incomplete request body")
                def reject_constant(value):
                    raise ValueError("non-finite JSON number")
                try:
                    body = json.loads(raw.decode("utf-8"), parse_constant=reject_constant)
                except (ValueError, UnicodeError, RecursionError):
                    raise ApiError(400, "invalid UTF-8 JSON") from None
                self.respond(200, method(body))
            except ApiError as exc:
                self.respond(exc.status, {"detail": {"reason": exc.reason}})
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                return
            except Exception:
                self.respond(500, {"detail": {"reason": "internal service error"}})

    return ThreadingHTTPServer((host, port), Handler)
