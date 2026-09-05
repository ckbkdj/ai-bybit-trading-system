from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable


class HealthServer:
    def __init__(self, host: str, port: int, snapshot: Callable[[], dict]):
        self.host = host
        self.port = int(port)
        self.snapshot = snapshot
        self._server = None
        self._thread = None

    def start(self) -> None:
        snapshot = self.snapshot

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path not in {
                    "/health",
                    "/v1/health/live",
                    "/v1/health/ready",
                    "/v1/health/dependencies",
                }:
                    self.send_response(404)
                    self.end_headers()
                    return
                state = snapshot()
                if self.path == "/v1/health/live":
                    response = {"status": "alive"}
                    status_code = 200
                elif self.path == "/v1/health/ready":
                    response = {"status": "ready" if state.get("ready") else "not_ready"}
                    status_code = 200 if state.get("ready") else 503
                elif self.path == "/v1/health/dependencies":
                    response = state
                    status_code = 200 if state.get("ready") else 503
                else:
                    response = state
                    status_code = 200
                payload = json.dumps(response, ensure_ascii=False).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format, *args):
                return

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, name="health-server", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
