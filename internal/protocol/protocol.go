// Package protocol 按请求路径识别上游协议族，并集中描述各协议的差异。
//
// 网关只暴露 /v1/{path}，协议由路径自动判定，无需任何配置：
//
//	/v1/chat/completions   /v1/completions   /v1/embeddings  …  → openai
//	/v1/responses                                               → responses
//	/v1/messages           /v1/messages/count_tokens            → anthropic
//
// 各协议的认证头、必需请求头、响应中模型名与 ID 的字段路径全部内聚在
// Spec 里，调用方不需要再关心「这是哪家的接口」。
package protocol

import "strings"

// Spec 描述一个协议族的全部差异点。进程内只读。
type Spec struct {
	// Name 用于配置覆盖、日志与指标标签。
	Name string

	// AuthHeader 是注入上游凭据的头名。
	AuthHeader string
	// AuthScheme 是凭据前缀；空串表示直接写入原始 Key（如 Anthropic 的 x-api-key）。
	AuthScheme string
	// RequiredHeaders 是该协议必需但客户端可能未提供的头，缺失时补齐。
	RequiredHeaders map[string]string

	// ModelPaths 是响应体中承载模型名的字段路径（按出现频率排序）。
	ModelPaths []string
	// IDPaths 是响应体中承载 ID 的字段路径，需替换模型名部分并保留随机后缀。
	IDPaths []string
}

var (
	openAISpec = &Spec{
		Name:       "openai",
		AuthHeader: "Authorization",
		AuthScheme: "Bearer",
		ModelPaths: []string{"model"},
		IDPaths:    []string{"id"},
	}

	responsesSpec = &Spec{
		Name:       "responses",
		AuthHeader: "Authorization",
		AuthScheme: "Bearer",
		// 流式事件把响应对象包在 response 下（response.created / .completed 等），
		// 非流式则是顶层对象，两种形态都要覆盖。
		ModelPaths: []string{"model", "response.model"},
		IDPaths:    []string{"id", "response.id"},
	}

	anthropicSpec = &Spec{
		Name:       "anthropic",
		AuthHeader: "x-api-key",
		AuthScheme: "", // 原始 Key，不加前缀
		RequiredHeaders: map[string]string{
			"anthropic-version": "2023-06-01",
		},
		// message_start 事件把消息对象包在 message 下。
		ModelPaths: []string{"model", "message.model"},
		IDPaths:    []string{"id", "message.id"},
	}

	// unknownSpec 兜底：未识别的 /v1 路径按 OpenAI 惯例透传，
	// 保证上游新增端点时网关无需改代码。
	unknownSpec = &Spec{
		Name:       "other",
		AuthHeader: "Authorization",
		AuthScheme: "Bearer",
		ModelPaths: []string{"model"},
		IDPaths:    []string{"id"},
	}
)

// All 返回全部具名协议，供配置校验与文档生成使用。
func All() []*Spec { return []*Spec{openAISpec, responsesSpec, anthropicSpec} }

// ByName 按协议名查找，用于解析配置中的按协议覆盖。
func ByName(name string) *Spec {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "openai", "chat":
		return openAISpec
	case "responses", "response":
		return responsesSpec
	case "anthropic", "claude", "messages":
		return anthropicSpec
	default:
		return nil
	}
}

// Detect 按请求路径识别协议。任何 /v1 下的路径都能得到一个可用的 Spec。
func Detect(path string) *Spec {
	// 去掉 /v1 前缀后只看第一段，避免为每个端点单独登记。
	rest := path
	if i := strings.IndexByte(rest, '/'); i == 0 {
		rest = rest[1:]
	}
	// 跳过版本段（v1 / v1beta / v2 …）
	if j := strings.IndexByte(rest, '/'); j > 0 && isVersion(rest[:j]) {
		rest = rest[j+1:]
	}
	seg := rest
	if k := strings.IndexByte(seg, '/'); k >= 0 {
		seg = seg[:k]
	}

	switch seg {
	case "messages":
		return anthropicSpec
	case "responses":
		return responsesSpec
	case "chat", "completions", "embeddings", "models", "moderations", "images", "audio":
		return openAISpec
	default:
		return unknownSpec
	}
}

// isVersion 判断路径段是否为 v1 / v2 / v1beta 之类的版本标识。
func isVersion(s string) bool {
	return len(s) >= 2 && s[0] == 'v' && s[1] >= '0' && s[1] <= '9'
}

// RouteLabel 返回用于指标的低基数路由标签。
func RouteLabel(path string) string {
	switch {
	case strings.HasPrefix(path, "/v1/chat/completions"):
		return "/v1/chat/completions"
	case strings.HasPrefix(path, "/v1/responses"):
		return "/v1/responses"
	case strings.HasPrefix(path, "/v1/messages"):
		return "/v1/messages"
	case strings.HasPrefix(path, "/v1/completions"):
		return "/v1/completions"
	case strings.HasPrefix(path, "/v1/embeddings"):
		return "/v1/embeddings"
	case strings.HasPrefix(path, "/v1/models"):
		return "/v1/models"
	case strings.HasPrefix(path, "/v1/"):
		return "/v1/*"
	default:
		return "other"
	}
}

// AuthValue 按协议约定拼装凭据头取值。key 为空返回空串表示不注入。
func (s *Spec) AuthValue(key string) string {
	if key == "" {
		return ""
	}
	if s.AuthScheme == "" {
		return key
	}
	return s.AuthScheme + " " + key
}
