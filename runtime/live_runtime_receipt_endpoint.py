#!/usr/bin/env python3
"""LAN-capable authenticated receiver for live runtime receipts."""
from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from live_runtime_receipts import receive_receipt

HOST = os.getenv("RUNTIME_RECEIPT_HOST", "127.0.0.1")
PORT = int(os.getenv("RUNTIME_RECEIPT_PORT", "6982"))


class Handler(BaseHTTPRequestHandler):
    server_version = "MXC-Runtime-Receipt/1"

    def log_message(self, _format: str, *args: object) -> None:
        return

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/runtime-receipt":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 4 * 1024 * 1024:
            self.send_error(400, "invalid receipt size")
            return
        body = self.rfile.read(length)
        try:
            receive_receipt(dict(self.headers.items()), body)
        except PermissionError:
            self.send_error(401, "unauthorized")
            return
        except (ValueError, KeyError, TypeError):
            self.send_error(422, "invalid receipt")
            return
        self.send_response(202)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(b"accepted\n")


if __name__ == "__main__":
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
