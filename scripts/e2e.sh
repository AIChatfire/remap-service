#!/usr/bin/env bash
# 端到端联调：启动 mock 上游 + 网关，跑一轮验证后清理。
#
#   ./scripts/e2e.sh
#
# 验证项：凭据透传与协议转换、上游地址头覆盖、请求改写、字段级脱敏、
#        生成内容不被篡改、SSE 增量下发、错误脱敏与状态码透传、动态路由。
set -uo pipefail
cd "$(dirname "$0")/.."

GW_PORT=${GW_PORT:-18080}
UP_PORT=${UP_PORT:-19911}
PY=${PY:-python3}
PASS=0
FAIL=0
# 本机若设了 HTTP_PROXY，访问网关必须绕开，否则请求会被代理吞掉。
CURL=(curl -s --noproxy '*')

GW_PID=""
UP_PID=""
cleanup() {
  [[ -n "$GW_PID" ]] && kill "$GW_PID" 2>/dev/null
  [[ -n "$UP_PID" ]] && kill "$UP_PID" 2>/dev/null
  wait 2>/dev/null
  rm -f "${ENVFILE:-}"
}
trap cleanup EXIT

ok()  { PASS=$((PASS + 1)); printf '  \033[32m✓\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL + 1)); printf '  \033[31m✗\033[0m %s\n' "$1"; }

# 端口被别的服务占用时，请求会静默打到那个服务上，产生一大串
# 看起来像脱敏失效的假失败。提前查出来，直接给出可操作的提示。
check_port_free() {
  local port=$1 what=$2
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    local who
    who=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR==2{print $1" (pid "$2")"}')
    printf '\033[31m端口 %s 已被 %s 占用\033[0m —— 它会截获本该发给%s的请求。\n' \
      "$port" "${who:-未知进程}" "$what"
    printf '请换端口重跑：GW_PORT=18981 UP_PORT=19981 %s\n' "$0"
    exit 2
  fi
}
check_port_free "$GW_PORT" "网关"
check_port_free "$UP_PORT" "mock 上游"
have() { if [[ "$1" == *"$2"* ]]; then ok "$3"; else bad "$3"; printf '      实际: %.240s\n' "$1"; fi; }
lack() { if [[ "$1" != *"$2"* ]]; then ok "$3"; else bad "$3"; printf '      泄漏: %.240s\n' "$1"; fi; }
eq()   { if [[ "$1" == "$2" ]]; then ok "$3"; else bad "$3（实际 $1，期望 $2）"; fi; }

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%d\n", time*1000' 2>/dev/null || echo 0
}

# ---------- 启动 ----------
[[ -x bin/gateway ]] || { echo "请先执行 make build"; exit 1; }

: >/tmp/e2e_up.log
"$PY" scripts/mock_upstream.py "$UP_PORT" >/tmp/e2e_up.log 2>&1 &
UP_PID=$!

ENVFILE=$(mktemp -t gwenv.XXXXXX)
cat >"$ENVFILE" <<EOF
ADDR=127.0.0.1:${GW_PORT}
UPSTREAM_BASE=http://127.0.0.1:${UP_PORT}
SANITIZE_ALIASES=deepseek-v3=deepseek-v3-250101|DeepSeek-V3
SANITIZE_DROP_HEADERS=X-Upstream-Instance
LOG_LEVEL=warn
EOF

./bin/gateway -env "$ENVFILE" >/tmp/e2e_gw.log 2>&1 &
GW_PID=$!

for _ in $(seq 1 60); do
  if "${CURL[@]}" -f "http://127.0.0.1:${GW_PORT}/healthz" >/dev/null 2>&1 &&
     grep -q listening /tmp/e2e_up.log; then
    break
  fi
  sleep 0.1
done
grep -q listening /tmp/e2e_up.log || { echo "mock 上游未能启动:"; cat /tmp/e2e_up.log; exit 1; }

GW="http://127.0.0.1:${GW_PORT}"
AUTH=(-H "Authorization: Bearer sk-client-key")
MAP=(-H "X-Model-Map: deepseek-pro:deepseek-v3;deepseek-flash:deepseek-lite")

# ---------- 1. 非流式 ----------
echo "1) 非流式：请求改写 + 字段级脱敏"
R=$("${CURL[@]}" -X POST "$GW/v1/chat/completions" "${AUTH[@]}" "${MAP[@]}" \
  -d '{"model":"deepseek-pro","messages":[{"role":"user","content":"hi"}]}')
have "$R" '"model": "deepseek-pro"'      "响应 model 已脱敏"
have "$R" 'chatcmpl-deepseek-pro-abc123' "id 替换模型名并保留随机后缀"
have "$R" 'fp_deepseek-pro_01'           "system_fingerprint 已脱敏"
UP=$(grep 'model=' /tmp/e2e_up.log | tail -1)
have "$UP" 'model=deepseek-v3' "上游实际收到 deepseek-v3"

# ---------- 2. 凭据透传 ----------
echo "2) 凭据透传与协议自动转换"
have "$UP" 'auth=Bearer sk-client-key' "OpenAI 端点收到 Bearer + 客户端 Key"

"${CURL[@]}" -o /dev/null -X POST "$GW/v1/messages" "${AUTH[@]}" \
  -d '{"model":"m","max_tokens":8}'
UPM=$(grep 'apikey=' /tmp/e2e_up.log | tail -1)
have "$UPM" 'apikey=sk-client-key' "Anthropic 端点自动转成 x-api-key（无 Bearer 前缀）"
have "$UPM" 'ver=2023-06-01'       "anthropic-version 必需头自动补齐"

# 网关不持有密钥：客户端不带凭据，上游也收不到
"${CURL[@]}" -o /dev/null -X POST "$GW/v1/chat/completions" -d '{"model":"m"}'
UPN=$(grep 'auth=' /tmp/e2e_up.log | tail -1)
have "$UPN" 'auth=None' "无客户端凭据时不注入任何 Key"

# ---------- 3. 上游地址由 header 指定 ----------
echo "3) 上游地址：X-Upstream-Base > 环境变量"
C=$("${CURL[@]}" -o /dev/null -w '%{http_code}' -X POST "$GW/v1/chat/completions" \
  "${AUTH[@]}" -H "X-Upstream-Base: http://127.0.0.1:${UP_PORT}" -d '{"model":"m"}')
eq "$C" "200" "合法 X-Upstream-Base 生效"
UPH=$(grep 'basehdr=' /tmp/e2e_up.log | tail -1)
lack "$UPH" 'basehdr=http' "X-Upstream-Base 未转发给上游"

C=$("${CURL[@]}" -o /dev/null -w '%{http_code}' -X POST "$GW/v1/chat/completions" \
  "${AUTH[@]}" -H "X-Upstream-Base: ftp://evil.com" -d '{"model":"m"}')
eq "$C" "502" "非法 scheme 被拒绝（不静默回落）"

# ---------- 4. SSE ----------
echo "4) SSE 流式：逐 chunk 脱敏"
S=$("${CURL[@]}" -N -X POST "$GW/v1/chat/completions" "${AUTH[@]}" "${MAP[@]}" \
  -d '{"model":"deepseek-pro","stream":true}')
have "$S" '"model": "deepseek-pro"' "流式 chunk model 已脱敏"
have "$S" 'data: [DONE]'            "[DONE] 哨兵保留"
N=$(grep -c '^data: ' <<<"$S")
if [[ $N -ge 6 ]]; then ok "收到 $N 个 data 事件（未被合并）"; else bad "只收到 $N 个 data 事件"; fi

# ---------- 5. 生成内容不被篡改 ----------
echo "5) 脱敏边界：不碰模型生成的内容"
# mock 上游会把「回答来自 <上游模型>」写进 content
R=$("${CURL[@]}" -X POST "$GW/v1/chat/completions" "${AUTH[@]}" "${MAP[@]}" \
  -d '{"model":"deepseek-pro","messages":[{"role":"user","content":"你是什么模型"}]}')
have "$R" '"model": "deepseek-pro"' "model 字段已脱敏"
have "$R" '\u56de\u7b54\u6765\u81ea deepseek-v3' "content 里的模型名原样保留（未篡改生成内容）"

# ---------- 6. 增量性 ----------
echo "6) 流式增量下发（验证未整体缓冲）"
T0=$(now_ms)
FIRST=$("${CURL[@]}" -N --max-time 5 -X POST "$GW/v1/chat/completions" "${AUTH[@]}" "${MAP[@]}" \
  -d '{"model":"deepseek-pro","stream":true}' 2>/dev/null | head -1)
T1=$(now_ms)
if [[ "$T0" != 0 && "$T1" != 0 ]]; then
  MS=$((T1 - T0))
  # 上游每 token 间隔 50ms、共 5 个 token（总约 250ms）；缓冲则首块必然 >250ms
  if [[ $MS -lt 200 ]]; then ok "首块 ${MS}ms 到达（上游总耗时约 250ms）"
  else bad "首块 ${MS}ms，疑似被整体缓冲"; fi
fi
have "$FIRST" 'deepseek-pro' "首块内容已脱敏"

# ---------- 7. 错误 ----------
echo "7) 错误脱敏 + 状态码透传"
CODE=$("${CURL[@]}" -o /tmp/e2e_err.json -w '%{http_code}' -X POST "$GW/v1/chat/completions" \
  "${AUTH[@]}" -H 'X-Model-Map: deepseek-pro:boom-v3' -d '{"model":"deepseek-pro"}')
E=$(cat /tmp/e2e_err.json)
eq   "$CODE" "429"                      "上游 429 原样透传（容灾交给调用方）"
lack "$E" 'boom-v3'                     "错误体无上游模型"
have "$E" 'The model `deepseek-pro`'    "error.message 已脱敏"
have "$E" 'deepseek-pro_quota_exceeded' "error.code 已脱敏"

# ---------- 8. 响应头 ----------
echo "8) 响应头清洗"
H=$("${CURL[@]}" -D- -o /dev/null -X POST "$GW/v1/chat/completions" "${AUTH[@]}" -d '{"model":"m"}')
lack "$H" 'X-Upstream-Instance' "X-Upstream-Instance 已清除"

# ---------- 9. 动态路由 ----------
echo "9) 动态路由：三种协议端点"
for p in /v1/chat/completions /v1/responses /v1/messages; do
  C=$("${CURL[@]}" -o /dev/null -w '%{http_code}' -X POST "$GW$p" "${AUTH[@]}" -d '{"model":"m"}')
  eq "$C" "200" "$p 透传"
done
SEEN=$(grep -o '/v1/[a-z/]*' /tmp/e2e_up.log | sort -u | tr '\n' ' ')
have "$SEEN" '/v1/messages' "路径原样透传到上游: $SEEN"

echo
printf '通过 \033[32m%d\033[0m 项，失败 \033[31m%d\033[0m 项\n' "$PASS" "$FAIL"
[[ $FAIL -eq 0 ]]
