#!/usr/bin/env bash
# 真实上游联调（火山方舟 Ark）。
#
#   ARK_KEY=ark-xxx ./scripts/e2e_ark.sh
#
# 覆盖两条真实用例：
#   1. POST /v1/chat/completions          非流式 + 流式
#   2. POST /v1/responses  (web_search)   流式，17 种事件类型、1300+ 行
#
# 断言重点：请求改写生效、响应体/SSE/annotation/usage 全链路无上游模型泄漏、
#          SSE 结构与事件序列完整、增量下发未被缓冲。
set -uo pipefail
cd "$(dirname "$0")/.."

ARK_KEY=${ARK_KEY:?请设置 ARK_KEY 环境变量}
ARK_MODEL=${ARK_MODEL:-deepseek-v4-flash-260425}
PUB_MODEL=${PUB_MODEL:-deepseek-pro}
GW_PORT=${GW_PORT:-18088}
PASS=0; FAIL=0
# 网关在本机，必须绕开系统代理；网关自身访问上游仍走 HTTP_PROXY。
CURL=(curl -s --noproxy '*')

GW_PID=""
cleanup() { [[ -n "$GW_PID" ]] && kill "$GW_PID" 2>/dev/null; wait 2>/dev/null; rm -f "${ENVFILE:-}"; }
trap cleanup EXIT

ok()  { PASS=$((PASS+1)); printf '  \033[32m✓\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  \033[31m✗\033[0m %s\n' "$1"; }
eq()  { if [[ "$1" == "$2" ]]; then ok "$3"; else bad "$3（实际 $1，期望 $2）"; fi; }
# noleak <文件> <描述>：检查文件中不含任何上游标识
noleak() {
  local hits
  hits=$(grep -o -E "$ARK_MODEL|deepseek-v4-flash|DeepSeek-V4|ark\.cn-beijing|volces" "$1" 2>/dev/null | sort -u | tr '\n' ' ')
  if [[ -z "$hits" ]]; then ok "$2"; else bad "$2 —— 泄漏: $hits"; fi
}

# ---------- 启动网关 ----------
[[ -x bin/gateway ]] || { echo "请先 make build"; exit 1; }

ENVFILE=$(mktemp -t arkenv.XXXXXX)
cat >"$ENVFILE" <<EOF
ADDR=127.0.0.1:${GW_PORT}
UPSTREAM_BASE=https://ark.cn-beijing.volces.com/api/v3
UPSTREAM_TIMEOUT=180s
UPSTREAM_FIRST_BYTE_TIMEOUT=60s
SANITIZE_ALIASES=${ARK_MODEL}=deepseek-v4-flash|DeepSeek-V4-Flash
SANITIZE_REPLACE=ark.cn-beijing.volces.com=api.internal.local;volces=internal
SANITIZE_DROP_HEADERS=X-Tt-Logid,X-Client-Request-Id
EOF

./bin/gateway -env "$ENVFILE" >/tmp/ark_gw.log 2>&1 &
GW_PID=$!
for _ in $(seq 1 50); do
  "${CURL[@]}" -f "http://127.0.0.1:${GW_PORT}/healthz" >/dev/null 2>&1 && break
  sleep 0.1
done

GW="http://127.0.0.1:${GW_PORT}"
H=(-H "Content-Type: application/json"
   -H "Authorization: Bearer ${ARK_KEY}"
   -H "X-Model-Map: ${PUB_MODEL}:${ARK_MODEL}")

# ============================================================
echo "【用例 1】/v1/chat/completions 非流式"
# ============================================================
C=$("${CURL[@]}" -o /tmp/ark_chat.json -w '%{http_code}' "$GW/v1/chat/completions" "${H[@]}" -d "{
  \"model\": \"${PUB_MODEL}\",
  \"messages\": [
    {\"role\":\"system\",\"content\":\"你是人工智能助手.\"},
    {\"role\":\"user\",\"content\":\"你好\"}
  ]}")
eq "$C" "200" "上游返回 200"
noleak /tmp/ark_chat.json "响应体无上游模型 / 域名泄漏"
python3 - "$PUB_MODEL" <<'PY'
import json,sys
pub=sys.argv[1]
d=json.load(open('/tmp/ark_chat.json'))
def chk(c,m):
    print(("  \033[32m✓\033[0m " if c else "  \033[31m✗\033[0m ")+m)
chk(d.get("model")==pub, f'model 字段脱敏为 {pub}（实际 {d.get("model")}）')
msg=d["choices"][0]["message"]
chk(bool(msg.get("content")), f'正文完整保留: {msg.get("content","")[:24]}…')
chk("reasoning_content" in msg, "reasoning_content 非标准字段被透传保留")
u=d.get("usage") or {}
chk(u.get("total_tokens",0)>0, f'usage 完整: {u.get("total_tokens")} tokens')
PY

# ============================================================
echo "【用例 2】/v1/chat/completions 流式"
# ============================================================
T0=$(perl -MTime::HiRes=time -e 'printf "%d",time*1000' 2>/dev/null || echo 0)
"${CURL[@]}" -N --max-time 120 "$GW/v1/chat/completions" "${H[@]}" -d "{
  \"model\": \"${PUB_MODEL}\", \"stream\": true,
  \"messages\": [{\"role\":\"user\",\"content\":\"用一句话介绍杭州\"}]}" >/tmp/ark_chat.sse
T1=$(perl -MTime::HiRes=time -e 'printf "%d",time*1000' 2>/dev/null || echo 0)

N=$(grep -c '^data: ' /tmp/ark_chat.sse)
if [[ $N -gt 5 ]]; then ok "收到 $N 个 data chunk（耗时 $((T1-T0))ms）"; else bad "只收到 $N 个 chunk"; fi
grep -q 'data: \[DONE\]' /tmp/ark_chat.sse && ok "[DONE] 哨兵保留" || bad "[DONE] 丢失"
noleak /tmp/ark_chat.sse "流式全文无泄漏"
M=$(grep -c "\"model\":\"${PUB_MODEL}\"" /tmp/ark_chat.sse)
if [[ $M -gt 5 ]]; then ok "$M 个 chunk 的 model 均已脱敏"; else bad "仅 $M 个 chunk 被脱敏"; fi

# ============================================================
echo "【用例 3】/v1/responses 流式 + web_search 工具"
# ============================================================
"${CURL[@]}" -N --max-time 180 "$GW/v1/responses" "${H[@]}" -d "{
  \"model\": \"${PUB_MODEL}\", \"stream\": true,
  \"tools\": [{\"type\":\"web_search\",\"max_keyword\":3}],
  \"input\": [{\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"今天有什么热点新闻\"}]}]
}" >/tmp/ark_resp.sse

LINES=$(wc -l </tmp/ark_resp.sse | tr -d ' ')
EVT=$(grep -c '^event: ' /tmp/ark_resp.sse)
KINDS=$(grep -o '^event: [a-z_.]*' /tmp/ark_resp.sse | sort -u | wc -l | tr -d ' ')
if [[ $EVT -gt 100 ]]; then ok "收到 $EVT 个事件 / $KINDS 种类型 / $LINES 行"; else bad "只收到 $EVT 个事件"; fi

noleak /tmp/ark_resp.sse "responses 流全文无泄漏（含 annotation / usage / tool 结果）"

for e in response.created response.in_progress response.web_search_call.completed \
         response.output_text.delta response.output_text.annotation.added response.completed; do
  grep -q "^event: $e\$" /tmp/ark_resp.sse && ok "事件 $e 保留" || bad "事件 $e 丢失"
done

python3 - "$PUB_MODEL" <<'PY'
import json,sys,re
pub=sys.argv[1]
def chk(c,m):
    print(("  \033[32m✓\033[0m " if c else "  \033[31m✗\033[0m ")+m)
models,texts,ann,usage=set(),[],0,None
for ln in open('/tmp/ark_resp.sse',encoding='utf-8'):
    if not ln.startswith('data: '): continue
    p=ln[6:].strip()
    if not p.startswith('{'): continue
    try: d=json.loads(p)
    except Exception: continue
    r=d.get("response") or {}
    if isinstance(r.get("model"),str): models.add(r["model"])
    if d.get("type")=="response.output_text.delta": texts.append(d.get("delta",""))
    if d.get("type")=="response.output_text.annotation.added": ann+=1
    if d.get("type")=="response.completed": usage=r.get("usage")
chk(models=={pub}, f'response.model 全部脱敏为 {pub}（实际 {models or "无"}）')
body="".join(texts)
chk(len(body)>100, f'正文完整重组: {len(body)} 字符 —— {body[:36]}…')
chk(ann>0, f'web_search 引用注记保留: {ann} 条')
chk(bool(usage and usage.get("total_tokens")), f'usage 保留: {usage}')
PY

# ============================================================
echo "【用例 4】/v1/responses 非流式（web_search 工具 + 完整 output 树）"
# ============================================================
C=$("${CURL[@]}" -o /tmp/ark_resp.json -D /tmp/ark_resp.hdr -w '%{http_code}' "$GW/v1/responses" "${H[@]}" -d "{
  \"model\": \"${PUB_MODEL}\", \"stream\": false,
  \"tools\": [{\"type\":\"web_search\",\"max_keyword\":2}],
  \"input\": [{\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"用一句话说说杭州天气\"}]}]}")
eq "$C" "200" "上游返回 200"
noleak /tmp/ark_resp.json "非流式 responses 无泄漏"
grep -qi 'x-tt-logid' /tmp/ark_resp.hdr && bad "X-Tt-Logid 未被清洗" || ok "X-Tt-Logid 响应头已清洗"
python3 - "$PUB_MODEL" <<'PY'
import json,sys
pub=sys.argv[1]; d=json.load(open('/tmp/ark_resp.json'))
def chk(c,m): print(("  \033[32m✓\033[0m " if c else "  \033[31m✗\033[0m ")+m)
chk(d.get("model")==pub, f'response.model = {d.get("model")}')
outs=[o.get("type") for o in d.get("output",[])]
chk(len(outs)>=2, f'output 树完整保留 {len(outs)} 项: {outs}')
chk(bool((d.get("usage") or {}).get("total_tokens")), f'usage 保留: {(d.get("usage") or {}).get("total_tokens")} tokens')
PY

# ============================================================
echo "【用例 5】映射语义：加权分流 / 多对一 / 未声明透传"
# ============================================================
# 加权分流：用两个不存在的上游名，只看网关选了谁（读内部审计日志），
# 不消耗真实 token，比统计成功率更精确。
: >/tmp/ark_dist.log
python3 - "$GW" "$ARK_KEY" <<'PY'
import json,sys,urllib.request,concurrent.futures as cf
gw=sys.argv[1]+"/v1/chat/completions"
op=urllib.request.build_opener(urllib.request.ProxyHandler({}))
H={"Content-Type":"application/json","Authorization":"Bearer "+sys.argv[2],
   "X-Model-Map":"wmix:UP-A-fake;wmix:UP-A-fake;wmix:UP-B-fake"}
def one(_):
    r=urllib.request.Request(gw,json.dumps(
        {"model":"wmix","messages":[{"role":"user","content":"x"}]}).encode(),H)
    try: op.open(r,timeout=60).read()
    except Exception: pass
with cf.ThreadPoolExecutor(12) as ex: list(ex.map(one,range(400)))
PY
python3 - <<'PY'
import re,collections
c=collections.Counter(re.findall(r'"upstream_model":"(UP-[AB]-fake)"',open('/tmp/ark_gw.log').read()))
a,b=c.get("UP-A-fake",0),c.get("UP-B-fake",0); r=a/max(b,1)
# 真机为抽样验证，阈值按 ~3σ 放宽（n=400 时 2:1 的 3σ 区间约 1.5~2.7）；
# 分布精度由单元测试 TestLookupDistribution 以 30000 次采样保证。
good = a>0 and b>0 and 1.5<=r<=2.7
print(("  \033[32m✓\033[0m " if good else "  \033[31m✗\033[0m ")
      +f'加权分流 2:1 生效 —— A={a} B={b} 实测 {r:.2f}:1 (n={a+b}，抽样)')
PY

# 多对一：两个对外模型共用同一上游，各自按请求维度脱敏
for pub in alpha beta; do
  "${CURL[@]}" -o /tmp/ark_m2o.json "$GW/v1/chat/completions" \
    -H "Content-Type: application/json" -H "Authorization: Bearer ${ARK_KEY}" \
    -H "X-Model-Map: alpha:${ARK_MODEL};beta:${ARK_MODEL}" \
    -d "{\"model\":\"$pub\",\"messages\":[{\"role\":\"user\",\"content\":\"回复ok\"}]}"
  M=$(python3 -c "import json;print(json.load(open('/tmp/ark_m2o.json')).get('model'))")
  if [[ "$M" == "$pub" ]] && ! grep -q "$ARK_MODEL" /tmp/ark_m2o.json; then
    ok "多对一：$pub 独立脱敏，无串味"
  else
    bad "多对一：$pub 脱敏为 $M"
  fi
done

# 未声明映射的模型原样透传，保证新模型零配置可用
C=$("${CURL[@]}" -o /dev/null -w '%{http_code}' "$GW/v1/chat/completions" \
  -H "Content-Type: application/json" -H "Authorization: Bearer ${ARK_KEY}" \
  -d "{\"model\":\"${ARK_MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}")
eq "$C" "200" "无 X-Model-Map 时原样透传"

# GET 无 body 的端点走纯透传路径
C=$("${CURL[@]}" -o /dev/null -w '%{http_code}' "$GW/v1/models" -H "Authorization: Bearer ${ARK_KEY}")
eq "$C" "200" "GET /v1/models 动态透传"

# ============================================================
echo "【用例 6】响应结构完整性：直连 vs 经网关（字段树 diff）"
# ============================================================
"${CURL[@]}" -o /tmp/ark_via.json "$GW/v1/chat/completions" "${H[@]}" \
  -d "{\"model\":\"${PUB_MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"说一个字\"}]}"
curl -s -o /tmp/ark_raw.json https://ark.cn-beijing.volces.com/api/v3/chat/completions \
  -H "Content-Type: application/json" -H "Authorization: Bearer ${ARK_KEY}" \
  -d "{\"model\":\"${ARK_MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"说一个字\"}]}"
python3 - <<'PY'
import json
def tree(o,p=""):
    s=set()
    if isinstance(o,dict):
        for k,v in o.items(): s.add(f"{p}.{k}"); s|=tree(v,f"{p}.{k}")
    elif isinstance(o,list) and o: s|=tree(o[0],p+"[]")
    return s
a=tree(json.load(open('/tmp/ark_raw.json'))); b=tree(json.load(open('/tmp/ark_via.json')))
def chk(c,m): print(("  \033[32m✓\033[0m " if c else "  \033[31m✗\033[0m ")+m)
chk(not (a-b), f'无字段丢失（直连 {len(a)} / 网关 {len(b)}）' + ("" if not (a-b) else f' 缺失 {sorted(a-b)}'))
chk(not (b-a), '无多余字段注入' + ("" if not (b-a) else f' 多出 {sorted(b-a)}'))
PY

# ============================================================
echo "【用例 7】凭据透传与错误透传"
# ============================================================
C=$("${CURL[@]}" -o /tmp/ark_err.json -w '%{http_code}' "$GW/v1/chat/completions" \
  -H "Content-Type: application/json" -H "Authorization: Bearer ${ARK_KEY}" \
  -H "X-Model-Map: ${PUB_MODEL}:model-that-does-not-exist-xyz" \
  -d "{\"model\":\"${PUB_MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}")
if [[ "$C" == "404" || "$C" == "400" ]]; then ok "上游错误状态码 $C 原样透传"; else bad "状态码 $C 异常"; fi
grep -q 'model-that-does-not-exist-xyz' /tmp/ark_err.json &&
  bad "错误体泄漏上游模型名" || ok "错误体已脱敏为对外模型"

# 网关不持有密钥：错误的客户端 Key 会拿到上游的 401，而非网关自己判定。
C=$("${CURL[@]}" -o /tmp/ark_401.json -w '%{http_code}' "$GW/v1/chat/completions" \
  -H "Content-Type: application/json" -H "Authorization: Bearer sk-totally-invalid" \
  -d "{\"model\":\"${PUB_MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}")
eq "$C" "401" "非法客户端 Key 由上游判定并透传 401"

# 完全不带凭据同样交由上游拒绝，网关不做准入拦截。
C=$("${CURL[@]}" -o /dev/null -w '%{http_code}' "$GW/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d "{\"model\":\"${PUB_MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}")
if [[ "$C" == "401" || "$C" == "403" ]]; then
  # 变量后紧跟中文全角字符时必须用 ${} 界定，否则 bash 会把多字节字符
  # 当成变量名的一部分，在 set -u 下报 unbound variable。
  ok "无凭据请求由上游拒绝（${C}），网关无准入鉴权"
else
  bad "无凭据请求返回 ${C}，期望 401/403"
fi

# ============================================================
echo "【用例 8】日志"
# ============================================================
# 指标已全部外发到 Logfire，本机不再有 /metrics 可断言。
# model 标签的基数与脱敏语义由单测 TestMetricModelCardinalityBounded 覆盖。
grep -q "\"upstream_model\":\"$ARK_MODEL\"" /tmp/ark_gw.log &&
  ok "内部日志保留真实上游模型（审计可用）" || bad "内部日志缺少审计字段"
grep -q "\"model\":\"${PUB_MODEL}\"" /tmp/ark_gw.log &&
  ok "日志同时记录对外模型，便于对账" || bad "日志缺少对外模型字段"

echo
printf '通过 \033[32m%d\033[0m 项，失败 \033[31m%d\033[0m 项\n' "$PASS" "$FAIL"
[[ $FAIL -eq 0 ]]
