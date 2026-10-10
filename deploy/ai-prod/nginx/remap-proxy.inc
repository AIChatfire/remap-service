# remap-service 反代（回环 39030；服务无准入鉴权、凭据纯透传 ⇒ 见 README「暴露面」）
proxy_pass http://127.0.0.1:39030;
proxy_http_version 1.1;
proxy_set_header Host              $host;
proxy_set_header X-Real-IP         $remote_addr;
proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
proxy_set_header X-Forwarded-Proto $scheme;
# SSE 长连接：禁缓冲 + 长空闲。remap 流式总超时 30m、首字节（流式）2m，
# 这里取 1200s 覆盖长 prefill；真到 30m 空闲属 stall 异常，由 remap 自己掐。
proxy_buffering off;
proxy_read_timeout 1200s;
proxy_send_timeout 1200s;
# 与 remap MAX_BODY_BYTES=64MB 对齐（多模态图片 base64 才会接近这个量级）
client_max_body_size 64m;
