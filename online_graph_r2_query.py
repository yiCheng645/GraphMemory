"""Bounded, deterministic long-query retrieval; no generated queries or answers."""
from collections import Counter
import hashlib

from memory_service import ApiError, require_text

MAX_QUERY_CHARS = 131072
WINDOW_CHARS = 4096
WINDOW_OVERLAP = 256
SHORT_QUERY_CHARS = 8192
RERANK_QUERY_BYTES = 3900


def query_text(body):
    query = require_text(body, "query")
    options = body.get("options", [])
    if not isinstance(options, list) or any(not isinstance(o, str) for o in options):
        raise ApiError(422, "options must be an array of strings")
    text = query + ("\n" + "\n".join(options) if options else "")
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        raise ApiError(422, "query/options contain invalid Unicode") from None
    if len(text) > MAX_QUERY_CHARS:
        raise ApiError(413, "query and options exceed 131072 characters")
    return text


def windows(text):
    if len(text) <= SHORT_QUERY_CHARS:
        return [text]
    result = []
    start = 0
    while start < len(text):
        end = min(start + WINDOW_CHARS, len(text))
        result.append(text[start:end])
        if end == len(text):
            break
        start = end - WINDOW_OVERLAP
    return result


def rerank_query(text):
    """Only long inputs use disclosed extractive head/stratified/tail excerpts.

    Every input character still participates in first-stage window retrieval.
    The platform receives the original question independently of this service.
    """
    if len(text) <= SHORT_QUERY_CHARS:
        return text
    raw = text.encode("utf-8")
    excerpts = [raw[:512].decode("utf-8", errors="ignore")]
    for i in range(1, 9):
        center = len(raw) * i // 9
        excerpts.append(raw[max(0, center - 120):center + 120].decode("utf-8", errors="ignore"))
    excerpts.append(raw[-1200:].decode("utf-8", errors="ignore"))
    result = "\n[query excerpt]\n".join(excerpts)
    if len(result.encode("utf-8")) > RERANK_QUERY_BYTES:
        raise ApiError(503, "extractive query exceeded fixed rerank byte budget")
    return result


class WindowedMemory:
    def __init__(self, inner, check_deadline):
        self.inner, self.check_deadline = inner, check_deadline

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def search(self, body):
        text = query_text(body)
        self.check_deadline()
        if len(text) <= SHORT_QUERY_CHARS:
            return self.inner.search(body)
        scores, records = Counter(), {}
        # Identical windows need not repeat the same scan or native request.
        for part in dict.fromkeys(windows(text)):
            self.check_deadline()
            request = {**body, "query": part, "top_k": 100}
            request.pop("options", None)  # Options were included in text before splitting.
            rows = self.inner.search(request)["data"]
            for rank, row in enumerate(rows, 1):
                previous = records.setdefault(row["id"], row)
                if previous["content"] != row["content"]:
                    raise ApiError(503, "candidate source changed across query windows")
                scores[row["id"]] += 1 / (60 + rank)
        self.check_deadline()
        return {"data": [{**records[key], "score": scores[key]} for key in
                         sorted(scores, key=lambda key: (-scores[key], key))[:body.get("top_k", 100)]]}


class BoundedQueryReranker:
    def __init__(self, inner, check_deadline):
        self.inner, self.check_deadline = inner, check_deadline

    def rerank(self, query, documents, top_n):
        self.check_deadline()
        result = self.inner.rerank(rerank_query(query), documents, top_n)
        self.check_deadline()
        return result


def query_trace(text):
    return {"query_characters": len(text),
            "query_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "query_window_count": len(dict.fromkeys(windows(text))),
            "long_query_policy": "full_window_rrf100_stratified_rerank_v1" if
                                 len(text) > SHORT_QUERY_CHARS else "original_short_query"}
