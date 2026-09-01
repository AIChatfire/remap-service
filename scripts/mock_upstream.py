#!/usr/bin/env python3
"""模拟上游厂商，用于本地联调网关的改写与脱敏行为。

启动：python3 scripts/mock_upstream.py [port]

行为：
  - 请求体中的 model 会原样回显到响应，便于验证「请求改写」是否生效；
  - 响应刻意在 id / system_fingerprint / 错误 message 中塞入模型名，
    用于验证「响应脱敏」是否覆盖全面；
  - model 含 "boom" 时返回 429 错误，验证错误体脱敏与状态码透传。
"""
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("[upstream] %s %s\n" % (self.path, fmt % args))

    def _read(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def do_POST(self):
        req = self._read()
        model = req.get("model", "unknown")
        # 打印全部与鉴权/路由相关的头，供 e2e 断言凭据透传与协议转换是否正确
        print(
            f"[upstream] 收到 model={model}"
            f" auth={self.headers.get('Authorization')}"
            f" apikey={self.headers.get('x-api-key')}"
            f" ver={self.headers.get('anthropic-version')}"
            f" basehdr={self.headers.get('X-Upstream-Base')}"
            f" maphdr={self.headers.get('X-Model-Map')}",
            flush=True,
        )

        if "boom" in model:
            return self._error(model)
        if req.get("stream"):
            return self._stream(model)
        return self._json(model)

    def _json(self, model):
        body = json.dumps({
            "id": f"chatcmpl-{model}-abc123",
            "object": "chat.completion",
            "model": model,
            "system_fingerprint": f"fp_{model}_01",
            "choices": [{"index": 0, "message": {"role": "assistant",
                                                 "content": f"回答来自 {model}"},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 9, "completion_tokens": 12, "total_tokens": 21},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Upstream-Instance", "volc-node-7")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, model):
        body = json.dumps({
            "error": {
                "message": f"The model `{model}` is currently overloaded",
                "type": "rate_limit_error",
                "code": f"{model}_quota_exceeded",
                "param": "model",
            }
        }).encode()
        self.send_response(429)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _stream(self, model):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        def chunk(payload):
            data = ("data: " + json.dumps(payload) + "\n\n").encode()
            self.wfile.write(b"%x\r\n" % len(data) + data + b"\r\n")
            self.wfile.flush()

        for i, tok in enumerate(["你好", "，", "我是", model, "。"]):
            chunk({
                "id": f"chatcmpl-{model}-stream1",
                "object": "chat.completion.chunk",
                "model": model,
                "system_fingerprint": f"fp_{model}_01",
                "choices": [{"index": 0, "delta": {"content": tok}, "finish_reason": None}],
            })
            time.sleep(0.05)
        done = b"data: [DONE]\n\n"
        self.wfile.write(b"%x\r\n" % len(done) + done + b"\r\n")
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9911
    # 必须先构造（构造即 bind+listen）再打印。
    #
    # 反过来写会让 e2e 的就绪检查提前放行：那边靠 grep 这行日志判断上游可用，
    # 而此时 socket 还没绑定。本机 Python 启动快、间隙可忽略，CI 上冷启动慢，
    # 最先几个断言就会打在未监听的端口上，报成 upstream connection failed ——
    # 看着像脱敏失效，实际是启动竞态。
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"mock upstream listening on :{port}", flush=True)
    srv.serve_forever()
