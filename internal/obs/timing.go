package obs

// 上游耗时分段与容量水位相关的 span 属性键。
//
// 与 errors.go 里那组键分开成文件：那组描述「失败了、为什么失败」，
// 这组描述「时间花在哪儿了、当时系统有多满」—— 后者在请求**成功**时
// 同样有价值，是判断「上游普遍变慢」与「网关自己饱和」的唯一依据。
const (
	// AttrUpstreamWroteRequest 是「请求体真正写完并交给内核」的时刻
	// （相对本次上游请求发起时刻的毫秒偏移）。
	//
	// 这是整组埋点里最关键的一个：它把超时切成两半。
	//   - 若它接近超时上限 → 请求一直没发出去，卡在本地（连接池排队、
	//     HTTP/2 流并发上限排队、代理握手、DNS），属于网关侧问题；
	//   - 若它只有几毫秒 → 请求早就交给了上游，时间全花在等响应，
	//     属于上游侧问题。
	//
	// 没有它，「上游超时」这个现象在架构问题与上游问题之间无法区分，
	// 只能靠外部证据（上游自己是否也在报障）猜。
	AttrUpstreamWroteRequest = "gateway.upstream.wrote_request_ms"
	// AttrUpstreamDNS / Connect / TLS 分别是 DNS 解析、TCP 建连、TLS 握手
	// 完成的时刻偏移。复用连接时三者都不会触发，属性缺失即为复用。
	AttrUpstreamDNS     = "gateway.upstream.dns_ms"
	AttrUpstreamConnect = "gateway.upstream.connect_ms"
	AttrUpstreamTLS     = "gateway.upstream.tls_ms"
	// AttrUpstreamFirstByte 是首个响应字节到达的时刻偏移。
	// 与 gateway.ttfb_ms 的区别：那个从「上游请求发起」算到「响应头处理完」，
	// 这个由传输层在收到第一个字节时打点，不受上层处理耗时干扰。
	AttrUpstreamFirstByte = "gateway.upstream.first_byte_ms"
	// AttrUpstreamConnReused 报告本次是否复用了已有连接。
	AttrUpstreamConnReused = "gateway.upstream.conn_reused"
	// AttrUpstreamProto 是实际协商到的协议版本（h2 / http/1.1）。
	//
	// 传输层强制开启了 HTTP/2：所有请求复用少数几条 TCP 连接，超出对端
	// SETTINGS_MAX_CONCURRENT_STREAMS 的部分会在客户端**排队**，而这段
	// 排队时间同样计入首字节超时。没有这个属性，「是不是 h2 流排队」
	// 这个假设永远无法被证实或排除。
	AttrUpstreamProto = "gateway.upstream.proto"

	// AttrRequestBytes / AttrResponseBytes 是本次请求与响应的字节数。
	//
	// 首字节延迟与 prefill 长度强相关：没有体积这个维度，
	// 「是长上下文把上游拖住了，还是上游普遍排队」无法区分。
	AttrRequestBytes  = "gateway.request.bytes"
	AttrResponseBytes = "gateway.response.bytes"

	// AttrInflight / AttrInflightLimit 是**本请求在处理期间**的在途数与容量。
	//
	// 此前只在过载 503 分支写，于是「网关自己是不是快满了」只能从
	// 拒绝计数反推。恒写才能回答「失败请求当时的水位是多少」。
	AttrInflight      = "gateway.inflight"
	AttrInflightLimit = "gateway.inflight_limit"

	// AttrRequestID 是客户端传入的请求标识（原样透传的 X-Request-Id 等）。
	//
	// 上游超时时拿不到对方的 logid —— 一个字节都没回，也就没有任何凭据
	// 可以拿去和上游对账。此时客户端自己的 request id 是唯一能横向串联
	// 客户端日志、网关看板与调用方的把手。
	//
	// 只在客户端提供了才写：网关自己生成的 ID 客户端并不知道，
	// 起不到串联作用。
	AttrRequestID = "gateway.request.id"

	// AttrCredentialFingerprint 是本次使用凭据的指纹（前 8 位十六进制）。
	//
	// 上游的限流与配额是**按账号/渠道**计的。不记指纹，就无法按渠道聚合
	// 看「是不是某个 key 被打爆了」，所有渠道的错误混在一条曲线里。
	//
	// 只记哈希前缀，永不记原文：凭据是客户端透传进来的，
	// 落到第三方可观测后端等于泄露。
	AttrCredentialFingerprint = "gateway.credential.fingerprint"
)
