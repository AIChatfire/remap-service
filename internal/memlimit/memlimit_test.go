package memlimit

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
)

// withCgroup 把 cgroupPaths 指向一个内容为 content 的临时文件。
// content 为空串表示该文件不存在。
func withCgroup(t *testing.T, content string) {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "memory.max")
	if content != "" {
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := cgroupPaths
	cgroupPaths = []string{p}
	t.Cleanup(func() { cgroupPaths = old })
}

// 必须同时还原内存上限，否则一个用例设小了会让同包其它用例在 GC 压力下跑。
func restoreMemLimit(t *testing.T) {
	t.Helper()
	old := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(old) })
}

func TestApplyCgroupV2(t *testing.T) {
	restoreMemLimit(t)
	withCgroup(t, "1073741824") // 1GiB
	got := Apply()
	// 与实现用同一个整数表达式，避免把「浮点截断差一字节」当成回归。
	if want := int64(1073741824 / 100 * percent); got != want {
		t.Fatalf("Apply() = %d, want %d", got, want)
	}
	if cur := debug.SetMemoryLimit(-1); cur != got {
		t.Errorf("运行时内存上限 = %d, 与返回值 %d 不一致", cur, got)
	}
}

func TestApplySkipsUnlimited(t *testing.T) {
	restoreMemLimit(t)
	// cgroup v2 的无上限标记：必须跳过，否则会算出天文数字。
	withCgroup(t, "max")
	if got := Apply(); got != 0 {
		t.Fatalf("无上限时应返回 0, got %d", got)
	}
}

func TestApplySkipsV1Sentinel(t *testing.T) {
	restoreMemLimit(t)
	// cgroup v1 的无上限写法：0x7FFFFFFFFFFFF000（一个能通过 ParseInt
	// 的合法数字）。只按「解析成功 + 正数」判断会把它当成真实的上限，
	// 于是 GOMEMLIMIT 被设成一个天文数字，等于没设 ——
	// 而那正是最需要它的部署形态。
	withCgroup(t, "9223372036854771712")
	if got := Apply(); got != 0 {
		t.Fatalf("v1 哨兵值应视为无上限, got %d", got)
	}
}

func TestApplySkipsTinyLimit(t *testing.T) {
	restoreMemLimit(t)
	// 40MiB × 0.8 = 32MiB < minLimit(64MiB) → 宁可不设。
	withCgroup(t, "41943040")
	if got := Apply(); got != 0 {
		t.Fatalf("低于下限时应返回 0, got %d", got)
	}
}

func TestApplyMissingFile(t *testing.T) {
	restoreMemLimit(t)
	withCgroup(t, "") // 文件不存在（宿主裸跑、macOS 开发）
	if got := Apply(); got != 0 {
		t.Fatalf("读不到 cgroup 时应返回 0, got %d", got)
	}
}

// 显式配置必须优先：GOMEMLIMIT 支持 "8GiB" 这类带单位写法，
// 本包不解析它，所以只能在有人显式写了的时候整体让路。
func TestApplyRespectsExplicitGOMEMLIMIT(t *testing.T) {
	restoreMemLimit(t)
	withCgroup(t, "1073741824") // 1GiB，本可以算出 819MiB
	t.Setenv("GOMEMLIMIT", "512MiB")
	if got := Apply(); got != 0 {
		t.Fatalf("显式配置 GOMEMLIMIT 时不应覆盖, got %d", got)
	}
	if cur := debug.SetMemoryLimit(-1); cur != int64(^uint64(0)>>1) {
		t.Errorf("显式配置下不应改动运行时的内存上限, 当前 %d", cur)
	}
}

// 上限为 0 / 负数（畸形文件）同样视为「读不到」，不能算出负的内存上限。
func TestApplyMalformedValue(t *testing.T) {
	restoreMemLimit(t)
	for _, c := range []string{"0", "-1", "abc", ""} {
		t.Run(c, func(t *testing.T) {
			withCgroup(t, c)
			if got := Apply(); got != 0 {
				t.Fatalf("畸形容器值 %q 应返回 0, got %d", c, got)
			}
		})
	}
}
