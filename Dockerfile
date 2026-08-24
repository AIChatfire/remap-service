# ---------- build ----------
# 版本须与 go.mod 的 go 指令保持一致，否则 go build 直接报
# "go.mod requires go >= 1.25.0"。
FROM golang:1.25-alpine AS builder

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE

WORKDIR /src

# 依赖层单独缓存，源码改动不触发重新下载
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# BUILD_DATE 未传时回落到构建机当前时间；显式传入才能得到可复现的镜像。
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}" \
    -o /out/gateway ./cmd/gateway

# ---------- runtime ----------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/gateway /gateway

USER nonroot:nonroot
EXPOSE 8080 9090

# 全部配置来自环境变量。挂载 .env 到 /app/.env 或直接用 -e 传入。
WORKDIR /app
ENTRYPOINT ["/gateway"]
