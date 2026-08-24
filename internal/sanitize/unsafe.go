package sanitize

import "unsafe"

// bytesToString 零拷贝地把只读字节切片视作字符串。
// 仅用于随后不会修改 b 的只读场景（strings.Replacer 的输入）。
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
