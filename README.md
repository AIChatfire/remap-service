# remap-gateway

LLM 模型映射网关。解决 NewAPI 原生模型重定向「只改请求、不改响应」导致的上游模型名暴露问题。

```
客户端 ──▶ remap-gateway ──▶ 上游（火山 / OpenAI / Anthropic …）
            映射 + 脱敏         协议自动识别、凭据透传
```

**做什么**：请求改写（对外模型 → 上游真实模型）+ 响应字段脱敏（还原为对外模型）。
**不做什么**：负载均衡、故障回退、重试、准入鉴权。上游错误经脱敏后原样返回。

---

## 三条设计主线

**协议自动识别** —— 由请求路径判定，配置里没有任何协议开关。

| 入站路径 | 协议 | 凭据头 | 自动补齐 |
|---|---|---|---|
| `/v1/chat/completions`、`/v1/completions`、`/v1/embeddings`、`/v1/models` … | `openai` | `Authorization: Bearer <key>` | — |
| `/v1/responses`、`/v1/responses/{id}` | `responses` | `Authorization: Bearer <key>` | — |
| `/v1/messages`、`/v1/messages/count_tokens` | `anthropic` | `x-api-key: <key>`（无前缀） | `anthropic-version: 2023-06-01` |
| 其他 `/v1/*` | 按 OpenAI 惯例兜底 | `Authorization: Bearer <key>` | — |

**凭据纯透传** —— 网关不持有任何密钥。客户端的 Key 被提取后按目标协议重新注入，
所以 OpenAI 风格的客户端可以直接调 Anthropic 端点，反之亦然：

```
客户端 Authorization: Bearer sk-x  →  /v1/messages  →  上游 x-api-key: sk-x
客户端 x-api-key: sk-x             →  /v1/chat/...  →  上游 Authorization: Bearer sk-x
```

Key 的合法性由上游判定 —— 错误的 Key 会拿到上游原汁原味的 401。

**脱敏保守** —— 只改字段与短值，绝不碰模型生成的内容。详见下文。

---

## 快速开始

```bash
make build
cp .env.example .env      # 至少填 UPSTREAM_BASE
./bin/gateway
```

最小可用配置只需一行：

```bash
UPSTREAM_BASE=https://ark.cn-beijing.volces.com/api/v3 ./bin/gateway
```

验证（客户端 Key 直接用上游的）：

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer <你的上游 Key>' \
  -H 'X-Model-Map: deepseek-pro:deepseek-v3' \
  -d '{"model":"deepseek-pro","messages":[{"role":"user","content":"hi"}]}'
```

上游实际收到 `deepseek-v3`，客户端全程只看到 `deepseek-pro`。

Docker：

```bash
cp .env.example .env && docker compose up -d
```

`.env` 是硬依赖（compose 的 `env_file` 指向它），缺文件会直接启动失败。
容器默认只绑 `127.0.0.1`，端口冲突时用变量改，无需动 compose 文件：

```bash
PORT=18080 METRICS_PORT=19090 docker compose up -d
BIND_ADDR=0.0.0.0 docker compose up -d      # 确认前面有反代/安全组再这么做
```

镜像也可以直接从 GHCR 拉（tag 推送时由 CI 构建 amd64/arm64）：

```bash
docker pull ghcr.io/<owner>/remap-service:latest
```

---

## 上游地址

优先级：**`X-Upstream-Base` 请求头 > `UPSTREAM_BASE_<协议>` > `UPSTREAM_BASE`**

```bash
# 环境变量
UPSTREAM_BASE=https://ark.cn-beijing.volces.com/api/v3
UPSTREAM_BASE_ANTHROPIC=https://api.anthropic.com     # 可选，按协议覆盖

# 或由客户端逐请求指定
curl -H 'X-Upstream-Base: https://api.openai.com' ...
```

请求头只接受 http/https；非法 scheme 直接 502，不静默回落到默认地址。
公网暴露时可用 `UPSTREAM_BASE_FROM_HEADER=false` 关掉这个能力。

**URL 拼接规则**：

| base | 入站路径 | 实际请求 |
|---|---|---|
| `https://api.openai.com` | `/v1/chat/completions` | `…/v1/chat/completions` |
| `https://ark…/api/v3` | `/v1/chat/completions` | `…/api/v3/chat/completions` |
| `https://x.com/v1` | `/v1/messages` | `…/v1/messages` |

base 自带路径段时，入站的 `/v1` 被视为网关对外协议版本而剥离，不会产生 `/api/v3/v1/...`。

---

## 模型映射

三级匹配，先命中即用：

```
① 精确    deepseek-pro=deepseek-v3
② 通配    claude-*=claude-3-5-sonnet
③ 兜底    MODEL_MAP_FALLBACK=deepseek-v3
```

请求头 `X-Model-Map` 优先，未命中会继续查静态规则 —— 两者叠加而非互相屏蔽
（Header 通常只声明本渠道的少量映射，全局通配与兜底配在环境变量里）。

```
X-Model-Map: <对外模型>:<上游模型>;<对外模型>:<上游模型>;...
```

| 模式 | 写法 | 效果 |
|---|---|---|
| 一对一 | `pro:v3;flash:lite` | 标准映射 |
| 多对一 | `pro:v3;flash:v3` | 共用上游，各自按请求维度脱敏，互不串味 |
| 一对多 | `pro:v3;pro:r1` | 等权随机分流 |
| 加权 | `pro:v3;pro:v3;pro:r1` | v3 : r1 = 2 : 1（真机实测 262:138） |

### 通配符

键里带 `*` 即为通配规则，支持四种形态：

| 形态 | 例 | 匹配 |
|---|---|---|
| 前缀 | `claude-*` | `claude-3-5-sonnet` |
| 后缀 | `*-flash` | `gemini-2-flash` |
| 中缀 | `gpt-*-turbo` | `gpt-4-turbo` |
| 包含 | `*vision*` | `gpt-4-vision-preview` |
| 全匹配 | `*` | 任何模型（catch-all，永远最后才匹配） |

多条规则同时命中时选**最具体**的一条，与书写顺序无关。具体度依次比较：
精确 > 字面量更长 > 锚定边界更多。

```bash
MODEL_MAP=claude-3-5-*=v3-specific;claude-*=v3-generic;*-flash=lite;*=v3
```

真机实测的匹配结果：

| 请求模型 | 命中规则 | match |
|---|---|---|
| `exact-one` | 精确 | `exact` |
| `claude-3-5-sonnet` | `claude-3-5-*`（比 `claude-*` 具体） | `wildcard` |
| `claude-2` | `claude-*` | `wildcard` |
| `gemini-flash` | `*-flash` | `wildcard` |
| `totally-unknown-xyz` | 兜底 | `fallback` |

命中级别写进日志的 `match` 字段，便于确认规则是否按预期生效。

### 兜底与故障切换

```bash
MODEL_MAP_FALLBACK=deepseek-v3    # 精确与通配都未命中时使用，多值等权随机
MODEL_MAP_FAILOVER=true           # 首选上游失败时自动切到兜底模型
```

`MODEL_MAP_FAILOVER` 只在这些情况重试：上游返回 429 / 500 / 502 / 503 / 504，
或连接层失败。400 / 401 / 403 / 404 属于请求本身的问题，换模型也是同样的失败，
不重试以免让客户端白等一个 RTT。

三条安全边界：

- **响应正文一旦开始下发就绝不重试** —— 流式尤其如此，否则客户端会收到拼接的两段流。
- **已经命中兜底的请求不再切换** —— 避免「兜底失败后又切到兜底」。
- **兜底与首选相同时不切换** —— 重试等于原地再试一次。

切换次数记在 `gateway_model_failover_total`。这个指标上升说明首选上游在报错，
而客户端侧可能完全感知不到。

未命中任何规则（含兜底）时默认原样透传，保证新模型零配置可用；
`MODEL_MAP_STRICT=true` 可改为直接 400。

NewAPI 渠道配置：地址填 `http://<网关地址>`、密钥填**上游真实 Key**、模型重定向留空、
自定义请求头填 `X-Model-Map: ...`。

---

## 扩展模型能力（识图 / 视频 / 音频 / 文档 / 联网）

**不需要改代码。** 这些能力全部已经可用，因为网关对「模型能力」是无感的。

能力差异只体现在请求体的结构里 —— 识图是 `content` 数组里多一个 `image_url` 块，
联网是 `tools` 里多一项 `web_search`。而网关只做两件事：改 `model` 字段、
脱敏响应里的模型标识。请求体的其余部分逐字节透传。

已验证原样透传的形态（见 `TestMultimodalRequestBodyUntouched`）：

| 能力 | 请求体形态 |
|---|---|
| 识图（URL / base64） | `{"type":"image_url","image_url":{"url":"…"}}` |
| 识图（Anthropic 风格） | `{"type":"image","source":{"type":"base64",…}}` |
| 音频理解 | `{"type":"input_audio","input_audio":{"data":"…","format":"wav"}}` |
| 视频理解 | `{"type":"video_url","video_url":{"url":"…","fps":2}}` |
| 文档理解 | `{"type":"file","file":{"file_id":"…"}}` |
| 自动联网 | `{"tools":[{"type":"web_search","max_keyword":3}]}` |
| 函数调用 | `{"tools":[{"type":"function","function":{…}}]}` |

上游新增任何能力，网关都不需要感知。真机验证里 `/v1/responses` + `web_search`
的 447 事件流（含 7 条搜索引用注记）完整透传，就是这个设计的直接结果。

### 唯一需要配置的情况

新能力落在一个**新的路径前缀**上，且该协议的响应把模型名放在了新的字段路径。
此时在 `internal/protocol/protocol.go` 的 `Spec` 里补 `ModelPaths` / `IDPaths`。

判断方法：抓一份真实响应，统计模型名出现的 JSON 路径。

```bash
curl -sN <上游>/v1/新端点 -d '…' > /tmp/raw.sse
python3 - <<'PY'
import json, collections
REAL = "上游真实模型名"
paths = collections.Counter()
def walk(o, p=""):
    if isinstance(o, dict):
        for k, v in o.items(): walk(v, f"{p}.{k}" if p else k)
    elif isinstance(o, list):
        for v in o: walk(v, p + "[]")
    elif isinstance(o, str) and REAL in o: paths[p] += 1
for ln in open('/tmp/raw.sse'):
    if ln.startswith('data: ') and ln[6:].strip().startswith('{'):
        walk(json.loads(ln[6:]))
for p, c in paths.most_common(): print(f"{c:5d}  {p}")
PY
```

输出里出现 `ModelPaths` 未覆盖的路径才需要补。这正是当初定位「模型名只出现在
`response.model` 一处」的方法 —— 先测量，再改代码。

### 不该做的事

**不要在网关里做能力探测、格式转换或参数校验。** 一旦网关开始理解请求体结构，
每次上游更新协议都要跟着改代码，而这恰好是本项目要避免的耦合。
上游能力不匹配时，让上游自己报错并把错误原样透传给客户端 —— 客户端能拿到
准确的错误信息，网关也不必维护一份永远滞后的能力矩阵。

---

## 脱敏边界

这是本项目最需要谨慎的部分。早期版本对整个响应体做字符串替换，风险在于
模型生成的内容里若恰好出现上游模型名（用户问「你是什么模型」、代码片段、
搜索结果摘要……）会被静默篡改。

实测火山 `/v1/responses` 的 438 个 SSE chunk 中，上游模型名**只出现在
`response.model` 一处** —— 全文替换属于过度打击。

现在的策略是路径白名单 + 短值限制：

| 位置 | 处理 |
|---|---|
| `model` / `response.model` / `message.model` | 整体覆盖为对外模型名 |
| `id` / `message.id` / `response.id` / `system_fingerprint` | 短值子串替换，保留随机后缀 |
| 错误体 `message` / `code` / `param` / `type` | 短值子串替换 |
| 工具结果、引用注记等其他短字段 | 短值子串替换 |
| `content` / `text` / `delta` / `reasoning_content` / `arguments` … | **无论长短一律不动** |
| 任何超过 `SANITIZE_MAX_VALUE_LEN`（默认 256）的字符串 | **视为生成内容，不动** |
| 响应头 | 按 `SANITIZE_DROP_HEADERS` 白名单删除 |

真机验证：让模型原样复述含上游模型名的文本，`model` 字段脱敏为 `deepseek-pro`，
而回答正文里的 `deepseek-v4-flash-260425` 完整保留。

> **取舍说明**：这意味着如果模型自己在回答里说出真实型号，网关不会拦。
> 这是刻意的——篡改用户可见的回答比暴露型号更糟。需要更严格可调小
> `SANITIZE_MAX_VALUE_LEN`，但请注意误伤风险随之上升。

**别名库**：上游模型迭代出版本化名称时登记，避免脱敏残留。长串优先匹配，
`deepseek-v3-250101` 不会被 `deepseek-v3` 截断成残留的 `-250101`。

```bash
SANITIZE_ALIASES=deepseek-v3=deepseek-v3-250101|DeepSeek-V3
SANITIZE_REPLACE=ark.cn-beijing.volces.com=api.internal
SANITIZE_DROP_HEADERS=X-Tt-Logid,X-Client-Request-Id
```

---

## 配置

全部来自环境变量，默认读取 `./.env`（`-env` 可指定路径）。
真实环境变量优先于文件，方便 `docker -e` 临时覆盖。

完整注释见 [`.env.example`](.env.example)。启动前校验：`./bin/gateway -check`

几条刻意的简化：

- **无准入鉴权** —— 没有 `GATEWAY_KEYS`，凭据直接透传。
- **`OBS_BACKEND` 可省略** —— 给了 `LOGFIRE_TOKEN` 自动用 Logfire，给了 `OTEL_EXPORTER_OTLP_ENDPOINT` 自动用 OTLP。
- **连接池只有 `MAX_CONNS`** —— 其余参数按比例推导。
- **认证方式不配** —— 由协议决定。

字节数支持 `64MB` / `32mb` / `1048576`，时长支持 `120s` / `2m` / `120`（纯数字按秒）。

### 过载保护

```bash
MAX_INFLIGHT=4096    # 同时在途的请求数上限，超出返回 503 + Retry-After
```

这是网关最重要的自保开关。LLM 单请求耗时数十秒，上游一旦变慢，入站请求会持续
堆积 —— 每个在途请求至少占一个 goroutine、一份请求体缓冲（最大 `MAX_BODY_BYTES`）
与一条上游连接。没有上限时，网关会在上游恢复之前先把自己 OOM 掉，
并把压力全部传导给已经过载的上游。

闸门在**读取请求体之前**获取：body 一旦读入就已占用内存，此时再拒绝就失去了
保护意义。超限直接快速失败而不排队 —— 排队只会把延迟累积到客户端超时，
对已经过载的系统没有帮助。

```bash
curl -s localhost:8080/readyz
# {"status":"ready","inflight":37,"limit":4096}
```

`gateway_requests_rejected_total` 持续非零即说明该扩容或调高上限。
设为 `0` 表示不限制，仅在网关前已有可靠并发控制时才建议这样配。

---

## 可观测性

一个开关控制全部。关闭时 trace 走 OTel 空实现、指标写入丢弃型 provider，热路径无额外分配。

```bash
OBS_ENABLED=true
LOGFIRE_TOKEN=pylf_v1_xxx      # 填了它 backend 自动变 logfire
LOGFIRE_REGION=us              # us | eu
METRICS_ADDR=:9090             # Prometheus，独立端口；留空则不启用
```

- **logfire** — Pydantic Logfire，OTLP/HTTP + 裸 token（不带 `Bearer` 前缀）。
- **otlp** — 任意 OTLP/HTTP 后端：Jaeger、Tempo、SigNoz、阿里云 ARMS、腾讯云 APM。
- **none** — 不外发，仅保留本地 Prometheus 与结构化日志。

高 QPS 下建议 `OBS_SAMPLE_RATIO=0.05`，指标不受采样影响。

**指标**（低基数，标签仅 `route` / `model` / `outcome` / `status`）：

```
gateway_requests_total              gateway_request_duration_milliseconds
gateway_upstream_ttfb_milliseconds  gateway_requests_inflight
gateway_upstream_errors_total       gateway_sse_events_total
gateway_bytes_in_total              gateway_bytes_out_total
gateway_sanitize_rewrites_total     gateway_requests_rejected_total
gateway_model_failover_total
```

> 指标与 trace 中的 `model` 恒为**对外模型名**。真实上游模型只出现在内部结构化日志的
> `upstream_model` 字段（`LOG_UPSTREAM_MODEL=false` 可关闭）。

**`model` 标签的基数是封闭的**：只有在 `MODEL_MAP` 或 `X-Model-Map` 里**精确**声明过
的模型名才会成为独立的时间序列，其余一律归到 `model="other"`。

这一条是必需的而非优化 —— `model` 取自客户端请求体，若直接做标签，客户端只要每次
填一个新名字就能让 Prometheus 的时间序列无限增长。实测修复前 60 个随机模型名产生
60 条序列，修复后收敛为 2 条（1 条已声明 + 1 条 `other`）。

通配命中**不算**已声明：`*` 能匹配无穷多个名字，若把它也算进去，配一条 catch-all
就等于放开了基数限制。排障需要原始模型名时看日志的 `model` 字段，那里是完整值。

---

## 性能

Apple M1 Max，`go test -bench`：

| 场景 | 耗时 | 分配 |
|---|---|---|
| 端到端非流式（含完整脱敏） | 34 µs/op | 275 allocs |
| 端到端流式（50 chunk） | 147 µs/op，53 MB/s | 300 allocs |
| 单 chunk 脱敏（无命中，走剪枝） | 151 ns，1.0 GB/s | 1 alloc |
| 单 chunk 脱敏（命中并改写） | 1.3 µs | 14 allocs |
| `MayMatch` 预检 | 91 ns，1.1 GB/s | **0 allocs** |
| 精确映射查表 | 7.8 ns | **0 allocs** |
| 通配匹配（命中） | 16 ns | **0 allocs** |
| 通配匹配（未命中，扫完 3 条规则） | 20 ns | **0 allocs** |
| Replacer 获取（缓存命中） | 2.2 ns | **0 allocs** |

改用字段级脱敏后，流式路径的分配量从 956 降到 300（-69%）—— 绝大多数
chunk 被 `MayMatch` 剪枝，连 JSON 都不用解析。

关键设计：

- **`MayMatch` 预检** —— SSE 增量 chunk 绝大多数只含生成文本，bytes 级预检不命中就直接返回，跳过全部 JSON 解析
- `strings.Replacer` 单趟 trie 扫描完成多模式替换
- **通配规则在构建期编译并按具体度预排序**，查询时只做匹配；精确规则仍走 map，不受通配规则数量影响
- `X-Model-Map` 解析结果分片缓存，Header 取值基数等于渠道数
- `GOGC=300`（可用环境变量覆盖）—— 网关是短生命周期对象密集型负载

连接复用：640 个请求（并发 32）只用 28 条上游连接，有测试断言。

### Replacer 缓存为何是两层

缓存键含对外模型名，而它取自客户端请求体 —— 基数不可信。原先的
copy-on-write 单层缓存假设「写入极其罕见」，客户端每次换一个 model 名就能
击穿这个假设，每次 miss 都要复制整张 map：

| 场景 | 单层 COW | 现在（两层） |
|---|---|---|
| 稳态命中 | 12 ns / 0 B | **18 ns / 0 B** |
| 每请求都是新 model 名 | **165 µs / 168 KB** | **716 ns / 769 B** |

现在第一层是无锁快照（只收录出现过两次以上的稳定组合，稳态全命中），
第二层是分片 map + 满则清空（承接一次性键，单片成本恒定）。
最坏情况改善 230 倍，稳态只多 6 ns。

---

## 开发

```bash
make test                        # 单元测试
make race                        # 竞态检测
make bench                       # 基准
make cover                       # 覆盖率（当前 82.3%）
make lint                        # gofmt + vet
make e2e                         # 本地 mock 上游，27 项断言，无需密钥
make e2e-ark ARK_KEY=ark-xxx     # 真实火山方舟上游，30 项断言
```

端口被占用时 `e2e.sh` 会直接报错退出而不是跑出一堆假失败
（请求会被占端口的服务截获）。换端口重跑：

```bash
GW_PORT=18981 UP_PORT=19981 ./scripts/e2e.sh
```

`scripts/mock_upstream.py` 是本地 mock 上游：回显请求模型与全部鉴权头、
在 `id`/`fingerprint`/错误体/生成内容中植入模型名、模型含 `boom` 时返回 429，
专门用来同时暴露「脱敏漏网」和「误伤生成内容」两类问题。

```
cmd/gateway         入口、生命周期
internal/protocol   协议注册表（路径识别 + 字段路径）、JSON 脱敏、SSE 管道
internal/config     .env 解析、环境变量取值、校验
internal/mapping    X-Model-Map 解析、通配规则、分片缓存、随机分流、兜底
internal/sanitize   替换器构建与缓存、长度阈值、生成内容字段白名单
internal/upstream   连接池、首字节超时看门狗
internal/gateway    代理核心：闸门 → 协议识别 → 凭据透传 → 改写 → 转发 → 脱敏
                    gate.go 并发闸门，failover.go 故障切换
internal/server     路由、panic 恢复、优雅退出
internal/obs        日志、trace、指标（可一键关闭）
```

### 真实上游验证结果

针对火山方舟 `deepseek-v4-flash-260425`（对外暴露为 `deepseek-pro`）实测 30/30 通过：

| 场景 | 结果 |
|---|---|
| `/v1/chat/completions` 非流式 | model / id / usage 脱敏，`reasoning_content` 等非标准字段原样保留 |
| `/v1/chat/completions` 流式 | 40 chunk 全部脱敏，`[DONE]` 保留 |
| `/v1/responses` 流式 + `web_search` | 430 事件 / 17 种类型零泄漏，正文 1137 字完整重组，6 条引用注记与 `tool_usage` 保留 |
| `/v1/responses` 非流式 | `output` 树（reasoning / web_search_call / message）完整，`X-Tt-Logid` 已清洗 |
| 凭据透传 | 非法 Key 透传上游 401；无凭据同样由上游拒绝 |
| 加权分流 | `wmix:A;wmix:A;wmix:B` 实测 262 : 138 |
| 结构完整性 | 直连 21 字段 vs 网关 21 字段，无丢失、无注入 |
| 生成内容保护 | 模型复述含上游型号的文本，正文完整保留、`model` 字段仍被脱敏 |
| 首块延迟 | 直连中位 979ms vs 网关 996ms，净开销 +17ms（在上游抖动范围内） |

---

## 为什么没有管理页面

管理页面会引入配置的**第二份真相**。一旦页面能改配置，就需要存储、并发写控制、
权限、审计、以及「页面上的值和 `.env` 里的值哪个生效」的规则 —— 网关会从一个
无状态进程变成一个有状态服务，横向扩容、灰度、回滚全都要重新设计。

当前形态下配置是不可变的：`.env` 进 Git，改配置走发布流程，天然有 review 和
回滚能力。这个性质值得保留。

**看板不需要网关提供。** 指标已经以 Prometheus 格式暴露在 `METRICS_ADDR`，
trace 走 OTLP。Grafana 导入即可，而且能和其他服务的指标放在同一个视图里：

```promql
# 各对外模型的 QPS
sum by (model) (rate(gateway_requests_total[1m]))

# 上游首字节 P95
histogram_quantile(0.95, sum by (le, model) (rate(gateway_upstream_ttfb_milliseconds_bucket[5m])))

# 过载：被闸门拒绝的比例
sum(rate(gateway_requests_rejected_total[1m])) / sum(rate(gateway_requests_total[1m]))

# 故障切换频率（首选上游在报错，客户端可能无感）
sum by (model) (rate(gateway_model_failover_total[5m]))

# 在途请求接近上限
gateway_requests_inflight
```

本地想直接看效果，compose 带了一个可选栈（Prometheus + Grafana，数据源已预置）：

```bash
docker compose --profile observability up -d
# Grafana http://127.0.0.1:3000 （admin/admin）
# Prometheus http://127.0.0.1:9091
```

**前提是 `.env` 里 `OBS_ENABLED=true`。** 关闭时 registry 为 nil，`/metrics`
端点根本不会监听，抓取会一直 target down。另外指标是懒注册的 ——
网关刚启动、还没收到过请求时 `/metrics` 返回 200 但内容为空，这是正常的。

如果确实需要一个页面，建议的边界是**只读**：读 `/readyz` 与 `/metrics` 渲染状态，
不提供写入。这样网关仍然无状态，页面挂了也不影响转发。

真正需要「界面上改模型映射」的场景，通常意味着上游应该是 NewAPI 这类带管理后台的
服务，由它下发 `X-Model-Map` —— 这也正是本网关设计时假定的上游形态。
职责分开：管理后台管配置，网关只管转发与脱敏。

---

## 运维要点

1. **过载保护** —— `MAX_INFLIGHT` 是最重要的一个值。盯住
   `gateway_requests_rejected_total`，持续非零就该扩容或调高上限。
2. **超时** —— 非流式 `UPSTREAM_TIMEOUT` 120s；`UPSTREAM_FIRST_BYTE_TIMEOUT` 30s 只约束响应头到达前，
   流式连接建立后不再受其影响（有测试覆盖这条竞态）。
3. **不设 WriteTimeout** —— 流式响应可持续数分钟，写超时会误杀长连接。
4. **优雅退出** —— 收到 SIGTERM 后 `/readyz` 先转 503 让 LB 摘流量，再关闭连接。
5. **别名库维护** —— 上游发布新版本号时及时登记，否则脱敏会残留。
6. **通配规则慎用 catch-all** —— 配了 `*=某模型` 之后所有未知模型都会被静默接受，
   排障时先看日志的 `match` 字段确认命中的是哪一级。
7. **网络隔离** —— 网关无准入鉴权，公网部署时请用安全组 / 内网 / 反代限制来源，
   并考虑关闭 `UPSTREAM_BASE_FROM_HEADER`。
8. **本机联调** —— 若系统设了 `HTTP_PROXY`，访问本机网关的 curl 需加 `--noproxy '*'`，
   否则请求会被代理拦截，表现为莫名 502 且网关日志无记录。

---

## CI 与发布

`.github/workflows/ci.yml` —— push / PR 触发，四个并行 job：

| job | 内容 |
|---|---|
| 静态检查 | gofmt、`go vet`、`go mod tidy` 无残留、staticcheck、actionlint |
| 测试 | `go test -race` on Linux + macOS，Linux 侧另出覆盖率 |
| 端到端 | `make e2e`，本地 mock 上游，不需要任何密钥 |
| 镜像 | 构建镜像 + `-version`/`-check` 冒烟 + compose 语法校验 |

Go 版本统一由 `go-version-file: go.mod` 决定，升级 `go.mod` 即全局生效 ——
Dockerfile 里的 `golang:1.25-alpine` 需要同步手改，两者不一致会直接构建失败。

`.github/workflows/release.yml` —— 推 `v*` tag 触发：先重跑测试闸门（tag 不走 CI，
必须自带验证），然后交叉编译 linux/darwin × amd64/arm64 二进制（附 `SHA256SUMS`）、
推送 GHCR 多架构镜像、创建 GitHub Release。

本地想在推之前自查工作流语法：

```bash
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:latest .github/workflows/*.yml
```

---

## 许可

MIT
