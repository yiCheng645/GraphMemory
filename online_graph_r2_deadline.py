"""Deadline state for the original online R2 route; no competition router dependency."""
from contextvars import ContextVar
import time
from memory_service import ApiError

REQUEST_DEADLINE = ContextVar("online_r2_request_deadline", default=None)
GRAPH_WINDOW = ContextVar("online_r2_graph_window", default=None)


def check_deadline():
    until = REQUEST_DEADLINE.get()
    if until is not None and time.monotonic() >= until:
        raise ApiError(503, "online R2 request time budget exhausted")

