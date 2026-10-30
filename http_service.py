"""Versioned HTTP transport for the existing, frozen memory implementation."""
import time

from memory_service import make_server as baseline_server


class _BodyReader:
    def __init__(self, stream):
        self.stream = stream
        self.consumed = 0

    def read(self, size=-1):
        data = self.stream.read(size)
        self.consumed += len(data)
        return data

    def __getattr__(self, name):
        return getattr(self.stream, name)


def make_server(store, host='127.0.0.1', port=8000, api_key='local-demo-only'):
    server = baseline_server(store, host=host, port=port, api_key=api_key)
    original_handler = server.RequestHandlerClass

    class Handler(original_handler):
        # Unread incoming bytes on close can hide HTTP 413/401 behind TCP reset.
        drain_limit = 2 * 1024 * 1024
        drain_seconds = .25

        def do_POST(self):
            self._body_reader = _BodyReader(self.rfile)
            self.rfile = self._body_reader
            super().do_POST()

        def respond(self, status, body):
            self._response_status = status
            super().respond(status, body)

        def finish(self):
            try:
                reader = getattr(self, '_body_reader', None)
                if reader is not None and getattr(self, '_response_status', 0) >= 400:
                    try:
                        declared = int(self.headers.get('Content-Length', '0'))
                    except ValueError:
                        declared = 0
                    remaining = min(max(declared - reader.consumed, 0), self.drain_limit)
                    deadline = time.monotonic() + self.drain_seconds
                    while remaining > 0:
                        budget = deadline - time.monotonic()
                        if budget <= 0:
                            break
                        self.connection.settimeout(budget)
                        data = reader.stream.read1(min(65536, remaining))
                        if not data:
                            break
                        remaining -= len(data)
            except (OSError, ValueError):
                # Error already sent. Bound stalled/disconnected client work.
                pass
            finally:
                super().finish()

    server.RequestHandlerClass = Handler
    return server
