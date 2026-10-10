# remap-service · ai-prod

LLM 模型映射网关的 ai-prod(210.121.44.245)实例。与 apps 主力实例
（remap.oneapis.cn）同源配置，镜像 public 直接 pull。

## 布局

- 容器：`/opt/remap/`（compose + .env），端口 `127.0.0.1:39030 → 8080`（仅回环）
- 入口：nginx `remap.1task.cn`（:80 301 → :443，反代片段 `remap-proxy.inc`）
- 证书：acme.sh HTTP-01，`nginx/apply-https.sh` 一键落地（幂等）

## 首次部署

```bash
scp -i ~/.ssh/id_ed25519_ops -r docker-compose.yml .env nginx root@210.121.44.245:/opt/remap/
ssh -i ~/.ssh/id_ed25519_ops root@210.121.44.245 'cd /opt/remap && docker compose up -d'
# nginx :80 vhost
scp -i ~/.ssh/id_ed25519_ops nginx/remap.1task.cn.conf root@210.121.44.245:/www/server/panel/vhost/nginx/
ssh -i ~/.ssh/id_ed25519_ops root@210.121.44.245 '/www/server/nginx/sbin/nginx -t && /www/server/nginx/sbin/nginx -s reload'
# DNS：remap.1task.cn A → 210.121.44.245（阿里云 API，TTL 600）
# DNS 生效后（权威 NS 即刻，公共解析器几分钟）：
ssh -i ~/.ssh/id_ed25519_ops root@210.121.44.245 'bash /opt/remap/nginx/apply-https.sh'
```

## 与 apps 的差异项（.env 已改，勿回搬）

| 项 | apps | ai-prod | 原因 |
|---|---|---|---|
| 绑定 | BIND_ADDR/PORT(8081) | compose 固定 39030 | 端口段惯例 39000-40000 |
| CONTAINER_MEMORY | 16g | 4g | 新实例流量未知，GOMEMLIMIT 随之推导 |
| OBS_SERVICE_NAME | remap-gateway | remap-gateway-aiprod | Logfire 看板区分实例 |

其余（UPSTREAM_BASE、SANITIZE_*、TRUSTED_PROXY_HOPS=1、LOGFIRE_TOKEN、
MAX_* 全系）与 apps 完全同源 —— 改任何一边前先想清楚是否要同步另一边。

## 升级

```bash
# 改 deploy/ai-prod/docker-compose.yml 里 REMAP_IMAGE_TAG（或 .env），然后：
ssh -i ~/.ssh/id_ed25519_ops root@210.121.44.245 \
  'cd /opt/remap && docker compose pull && docker compose up -d'
ssh -i ~/.ssh/id_ed25519_ops root@210.121.44.245 \
  'docker ps --format "{{.Names}}\t{{.Status}}" | grep remap; docker inspect remap --format "{{.Image}}"'
```

发版流程见仓库根 README（push main 自增 patch → GHCR，tag 无 v 前缀）。

## 暴露面（部署前必读）

- 网关**无准入鉴权**：凭据纯透传（客户端的 Key 原样注入上游），匿名请求
  会被上游 401 拒绝，不会消耗任何上游配额 —— 但它仍是一个「打向
  UPSTREAM_BASE 的开放转发器」，UPSTREAM_BASE_FROM_HEADER=false 已把
  目标钉死在火山，禁止放开。
- `X-Model-Map` / `X-Proxy` 是请求头，任何可达入口的客户端都能自带；
  与 apps（remap.oneapis.cn）暴露面一致。
- 存活断言用 `/healthz`（恒 200）；`/readyz` 会反映在途水位。

## 遥测

Logfire（token 与 apps 共用），service name `remap-gateway-aiprod` 区分实例。
看板按 service 筛；span 属性 `client.address` 是经 TRUSTED_PROXY_HOPS=1
解析后的真实客户端 IP（nginx 是唯一前置）。
