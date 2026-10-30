"""Alibaba qwen3.7-text-rerank adapter and optional Search wrapper."""
import http.client
import json
import math
import re
import threading
import urllib.error
import urllib.parse
import urllib.request

from memory_service import ApiError, encode

MODEL = "qwen3.7-text-rerank"
NATIVE_PATH = "/api/v1/services/rerank/text-rerank/text-rerank"
INSTRUCTION = ("Retrieve passages from the conversation history that contain evidence needed "
               "to answer the query. Relevant passages may provide only part of the evidence "
               "for questions requiring multiple facts. Prefer actual recorded facts over general advice.")


def default_endpoint(embedding_endpoint):
    parsed = urllib.parse.urlsplit(embedding_endpoint)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or not parsed.hostname.endswith(".maas.aliyuncs.com")):
        raise ValueError("Cannot derive reranker endpoint; configure the official HTTPS endpoint")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, NATIVE_PATH, "", ""))


class AlibabaReranker:
    def __init__(self, endpoint, api_key, model=MODEL, timeout=30):
        url = urllib.parse.urlsplit(endpoint)
        if (url.scheme != "https" or not url.hostname or url.username or url.password
                or url.query or url.fragment or url.path != NATIVE_PATH
                or not url.hostname.endswith(".maas.aliyuncs.com") or not api_key or "{" in endpoint):
            raise ValueError("Set a real official workspace HTTPS reranker endpoint and API key")
        if model != MODEL:
            raise ValueError("This native adapter currently supports only qwen3.7-text-rerank")
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Reranker timeout must be positive")
        self.endpoint, self.api_key, self.model, self.timeout = endpoint, api_key, model, timeout
        self.total_tokens = 0
        self.successful_calls = 0
        self.calls_without_usage = 0
        self.lock = threading.Lock()

    @staticmethod
    def parse(payload, document_count, top_n):
        try:
            result = payload["output"]["results"]
            if not isinstance(result, list) or len(result) != min(document_count, top_n):
                raise ValueError()
            seen, parsed = set(), []
            for row in result:
                index, score = row["index"], row["relevance_score"]
                if (type(index) is not int or not 0 <= index < document_count or index in seen
                        or type(score) not in (int, float) or not math.isfinite(score) or not 0 <= score <= 1):
                    raise ValueError()
                seen.add(index)
                parsed.append({"index": index, "relevance_score": score})
            return sorted(parsed, key=lambda r: (-r["relevance_score"], r["index"]))
        except (KeyError, TypeError, ValueError):
            raise ApiError(503, "reranker response has invalid result indices or scores") from None

    def rerank(self, query, documents, top_n):
        if not isinstance(query, str) or not query.strip():
            raise ApiError(422, "reranker query must be nonempty text")
        if (not isinstance(documents, list) or len(documents) > 500
                or any(not isinstance(d, str) or not d.strip() for d in documents)):
            raise ApiError(422, "reranker documents must be a list of at most 500 nonempty texts")
        if type(top_n) is not int or not 1 <= top_n <= 500:
            raise ApiError(422, "reranker top_n must be between 1 and 500")
        if not documents:
            return []
        requested = min(top_n, len(documents))
        body = {"model": self.model, "input": {"query": query, "documents": documents},
                "parameters": {"top_n": requested, "instruct": INSTRUCTION}}
        try:
            request = urllib.request.Request(self.endpoint, data=encode(body).encode("utf-8"),
                headers={"Authorization": "Bearer " + self.api_key, "Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
                if len(raw) > 4 * 1024 * 1024:
                    raise ApiError(503, "reranker response is too large")
                payload = json.loads(raw)
        except urllib.error.HTTPError as exc:
            # Do not expose upstream response bodies or credentials to clients/logs.
            code = ""
            try:
                failure = json.loads(exc.read(8192))
                supplied_code = failure.get("code", "") if isinstance(failure, dict) else ""
                if isinstance(supplied_code, str) and re.fullmatch(
                        r"(?:AccessDenied|Forbidden|Model|InvalidApiKey|Unauthorized|OperationDenied)(?:[.:-][A-Za-z0-9_]+)*", supplied_code):
                    code = " (" + supplied_code + ")"
            except (ValueError, OSError, http.client.HTTPException):
                pass
            finally:
                exc.close()
            if exc.code == 400:
                raise ApiError(422, "reranker rejected input; check token limits and model parameters") from None
            raise ApiError(503, f"reranker provider returned HTTP {exc.code}{code}; check model access/configuration/quota") from None
        except (http.client.HTTPException, urllib.error.URLError, TimeoutError, OSError, ValueError, UnicodeError):
            raise ApiError(503, "reranker provider unavailable or invalid JSON") from None
        # Record usage even when the response cannot be safely mapped to sources.
        usage = payload.get("usage", {}) if isinstance(payload, dict) else {}
        used = usage.get("total_tokens") if isinstance(usage, dict) else None
        with self.lock:
            if type(used) is int and used >= 0:
                self.total_tokens += used
            else:
                self.calls_without_usage += 1
        result = self.parse(payload, len(documents), requested)
        with self.lock:
            self.successful_calls += 1
        return result


class RerankedStore:
    """Wrap an existing store: retrieve up to 100 candidates, rerank, keep top_k.

    Errors propagate as explicit HTTP errors; never silently claim reranking
    worked when the provider failed. The base store supplies tenant/user scope.
    """
    def __init__(self, store, reranker, candidates=100):
        if type(candidates) is not int or not 1 <= candidates <= 100:
            raise ValueError("Reranker candidate count must be 1..100")
        self.store, self.reranker, self.candidates = store, reranker, candidates

    def add(self, body):
        return self.store.add(body)

    def search(self, body):
        if not isinstance(body, dict):
            return self.store.search(body)
        top_k = body.get("top_k", 10)
        if type(top_k) is not int or not 1 <= top_k <= 100:
            return self.store.search(body)
        response = self.store.search({**body, "top_k": max(top_k, self.candidates)})
        candidates = response["data"]
        if not candidates:
            return {"data": []}
        query = body["query"]
        if body.get("options"):
            query += "\n" + "\n".join(body["options"])
        ranked = self.reranker.rerank(query, [row["content"] for row in candidates], top_k)
        return {"data": [{"id": candidates[r["index"]]["id"],
                          "content": candidates[r["index"]]["content"],
                          "score": r["relevance_score"]} for r in ranked]}
