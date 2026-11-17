"""极简 JSON/静态文件 HTTP 框架（仅标准库）。"""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


class HttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def make_handler(route_fn, static_dir: str | None = None):
    """route_fn(method, path, query, body) -> (dict|list|tuple)。

    返回 dict/list 自动 JSON 序列化；返回 ('static', relpath) 走静态文件；
    抛 HttpError 返回对应错误 JSON。
    """

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # 安静一点，进度由调用方打印
            pass

        def _send_json(self, status, payload):
            data = json.dumps(payload, ensure_ascii=False, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_file(self, relpath):
            import mimetypes
            import os
            safe = os.path.normpath(relpath).lstrip("/")
            full = os.path.join(static_dir, safe)
            if not os.path.abspath(full).startswith(os.path.abspath(static_dir)) or not os.path.isfile(full):
                full = os.path.join(static_dir, "index.html")
            with open(full, "rb") as f:
                data = f.read()
            ctype = mimetypes.guess_type(full)[0] or "application/octet-stream"
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _handle(self):
            parsed = urlparse(self.path)
            body = None
            if self.command in ("POST", "PUT", "PATCH"):
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    raw = self.rfile.read(length)
                    try:
                        body = json.loads(raw)
                    except json.JSONDecodeError:
                        return self._send_json(400, {"error": "请求体不是合法 JSON"})
            try:
                result = route_fn(self.command, parsed.path, _query(parsed.query), body)
            except HttpError as e:
                return self._send_json(e.status, {"error": e.message})
            except Exception as e:  # 未预期异常返回 500，不断连
                import traceback
                traceback.print_exc()
                return self._send_json(500, {"error": f"服务内部错误: {type(e).__name__}: {e}"})
            if isinstance(result, tuple) and result and result[0] == "static":
                return self._send_file(result[1])
            self._send_json(200, result)

        do_GET = _handle
        do_POST = _handle
        do_PUT = _handle

    return Handler


def _query(qs: str) -> dict:
    from urllib.parse import parse_qs
    return {k: v[0] for k, v in parse_qs(qs).items()}


def serve(route_fn, port: int, static_dir: str | None = None) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(route_fn, static_dir))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
