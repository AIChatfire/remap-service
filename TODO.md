# TODO

## 已完成（2026-08-23）架构 review 与并发加固

### 修复的两个真实缺陷（均可由客户端触发）

- [x] **Replacer 缓存高基数退化** —— 缓存键含对外模型名，而它取自客户端请求体。
      原单层 copy-on-write 假设「写入极其罕见」不成立：每个新 model 名都 miss，
      每次 miss 复制整张 map，实测 **165 µs/op + 168 KB/op**（稳态 12 ns/0 B）。
      改为「无锁快照 + 分片回填」两层，最坏情况降到 716 ns/769 B（改善 230 倍），
      稳态 18 ns。测试：`TestForHighCardinalityBounded`、`TestForPromotesToHotSnapshot`。
- [x] **指标标签基数爆炸** —— `model` 标签直接取客户端请求值，实测 60 个随机
      model 名产生 60 条独立时间序列。改为只有**精确声明**过的模型名
      进标签，其余归一为 `other`（通配命中不算已声明，否则 catch-all 等于放开限制）。
      实测 60 条收敛为 2 条。测试：`TestMetricModelCardinalityBounded`。

### 新增能力

- [x] **并发闸门 `MAX_INFLIGHT`**（默认 4096，0 为不限制）—— 超限返回
      503 + `Retry-After`，不排队。闸门在**读请求体之前**获取，否则 body 已占内存。
      `/readyz` 暴露 `inflight`/`limit`，新增 `gateway_requests_rejected_total`。
- [x] **模型映射通配符** —— 支持前缀 `claude-*`、后缀 `*-flash`、中缀
      `gpt-*-turbo`、包含 `*vision*`、全匹配 `*`。多条命中时按**具体度**选择
      （精确 > 字面量更长 > 锚定边界更多），与书写顺序无关。零分配（命中 16 ns）。
- [x] **兜底模型 `MODEL_MAP_FALLBACK`** —— 精确与通配都未命中时使用，多值等权随机。
- [x] **故障切换 `MODEL_MAP_FAILOVER`** —— 上游 429/500/502/503/504 或连接失败时
      切到兜底模型重试一次。三条安全边界：正文已下发不重试（流式尤其）、
      已命中兜底不再切、兜底与首选相同不切。新增 `gateway_model_failover_total`。
- [x] **e2e 端口占用前置检查** —— 端口被占时请求会被别的服务截获，产生一堆
      看似脱敏失效的假失败。现在直接报错并提示换端口。

### 结论性说明（无需改代码）

- **多模态与工具能力已全部可用** —— 识图 / 视频 / 音频 / 文档 / 联网 / 函数调用
      只体现在请求体结构里，网关只改 `model` 字段，其余逐字节透传。
      已固化护栏测试 `TestMultimodalRequestBodyUntouched`（8 种形态）。
      唯一需要改代码的情况：新能力落在新路径前缀**且**响应把模型名放在了新字段
      路径，此时补 `Spec.ModelPaths` / `IDPaths`（README 附了定位脚本）。
- **不做管理页面** —— 会引入配置的第二份真相，把无状态进程变成有状态服务。
      看板直接在 Logfire 控制台建（README 附了常用查询）。
      需要界面改映射的场景应由上游 NewAPI 这类带后台的服务下发 `X-Model-Map`。

### 验收

`go test -race` 全绿，覆盖率 82.3%；mock e2e 27/27、真机 ark e2e 30/30。
基准无退化：非流式 34 µs/op，流式 147 µs/op，精确映射 7.8 ns/0 alloc。

---

## 待观察

- [ ] `MAX_INFLIGHT` 默认 4096 是按「LLM 请求大部分时间在等上游」估的。
      实际值应按 `gateway_requests_rejected_total` 与内存占用调整，
      单请求峰值内存约等于 body 大小 + 响应缓冲。
- [ ] 通配规则数量目前用线性扫描（个位数规则下 20 ns）。若某天规则数上百，
      再考虑按首段字面量建索引；在此之前线性扫描更简单也更快。
- [ ] 上游若在新端点使用不同的模型字段路径，需要在
      `internal/protocol/protocol.go` 的 `Spec.ModelPaths` / `IDPaths` 中补充。
- [ ] `SANITIZE_MAX_VALUE_LEN` 默认 256 是经验值。若发现某类短生成内容被误伤，
      优先考虑往 `sanitize.contentFields` 加字段名，而不是调低阈值。
- [ ] 故障切换目前只重试一次且只切兜底模型。若需要多级降级链
      （首选 → 次选 → 兜底），要先想清楚重试放大对上游的压力。
