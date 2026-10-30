"""Experimental presentation layer; source content and database are unchanged."""
from datetime import datetime, timedelta, timezone
import json
import unicodedata

from memory_service import ApiError


def format_document(content):
    header, separator, body = content.partition('\n')
    if not separator or not header.startswith('[source ') or not header.endswith(']'):
        raise ApiError(503, 'Missing internal source header for reranker formatting')
    try:
        source = json.loads(header[len('[source '):-1])
        role = source['role']
        if role not in ('user', 'assistant', 'system'):
            raise ValueError('Invalid role')
        # Test visibility without stripping joiners or changing any useful text.
        if not any(not c.isspace() and unicodedata.category(c)[0] not in ('C', 'Z') for c in body):
            return None
        lines = ['Role: ' + role]
        if 'timestamp_ms' in source:
            ms = source['timestamp_ms']
            if type(ms) is not int or not 0 <= ms <= 253402300799999:
                raise ValueError('Invalid timestamp')
            date = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ms)
            lines.append('Recorded at: ' + date.isoformat(timespec='milliseconds').replace('+00:00', ' UTC'))
        return '\n'.join(lines) + '\n\n' + body
    except (KeyError, TypeError, ValueError, OverflowError):
        raise ApiError(503, 'Invalid internal source metadata for reranker formatting') from None


class FormattedReranker:
    """Map filtered provider indices back to original candidates for RerankedStore."""
    def __init__(self, provider):
        self.provider = provider

    def rerank(self, query, documents, top_n):
        kept, texts = [], []
        for i, document in enumerate(documents):
            formatted = format_document(document)
            if formatted is not None:
                kept.append(i)
                texts.append(formatted)
        if not texts:
            return []
        ranked = self.provider.rerank(query, texts, min(top_n, len(texts)))
        return [{'index': kept[r['index']], 'relevance_score': r['relevance_score']} for r in ranked]
