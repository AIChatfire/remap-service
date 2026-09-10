// Package memlimit 依据容器内存上限设置 Go 运行时的软内存上限（GOMEMLIMIT）。
//
// 为什么需要它：网关给 GC 的目标是 debug.SetGCPercent(300)，含义是「堆涨到
// live set 的 4 倍才触发 GC」—— 一个**没有天花板**的策略。而网关是短生命
// 周期对象密集型负载（每请求构造若干 []byte、map 与 gjson 结果），上游一慢，
// 上万个在途请求会把各自的请求体与响应缓冲同时抱在内存里。
//
// GOMEMLIMIT 与 GOGC 不是二选一：GOGC 决定「平时多懒」，GOMEMLIMIT 决定
// 「最坏能到多少」。前者省 CPU，后者防 OOM。只设 GOGC 的部署在压力下会一路
// 涨到被 cgroup OOM-kill，而 OOM-kill 不带堆栈、不留遗言，比「GC 变激进」
// 难查得多。
//
// 为什么是推导而不是配置项：容器内存上限已经写在编排文件里（compose 的
// mem_limit），再让运维手填一个与之成比例的数，就多了一处必然漂移的同步点
// —— 而「两个互不知情的常量」正是本项目反复吃过亏的形态（见
// config.Lifecycle 的说明）。显式配置 GOMEMLIMIT 时以显式值为准。
package memlimit

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	// percent 是软上限占容器内存上限的比例（百分比）。
	//
	// 留出的 20% 不是保守，是两个必须覆盖的开销：
	//   - 非堆内存：goroutine 栈、连接读写缓冲、运行时元数据、arena 外的映射；
	//   - GOMEMLIMIT 是**软**上限，它让 GC 变激进而不是拒绝分配 ——
	//     逼近上限的瞬间堆仍可能短暂超出，必须留出这段冲高空间。
	//
	// 用整数百分比而不是浮点系数：total*0.8 在 total 接近 1<<60 时会丢精度，
	// 而同一个上限在不同架构上算出相差一两个字节的值毫无意义，却会让
	// 「看板上的数字」与「配置文件里的数字」对不上，白费一轮排查。
	percent = 80

	// minLimit 是生效下限。低于它宁可什么都不设：容器内存极小时按比例算出
	// 的值会小于运行时的最低需求，结果 GC 持续空转（thrashing）—— 表现为
	// CPU 打满而吞吐归零，比直接 OOM 更难定位。
	minLimit = 64 << 20

	// unlimitedSentinel：cgroup 未设置内存上限时，文件里写的是一个极大的数
	// （v1 是 0x7FFFFFFFFFFFF000）。把它当真实上限会算出一个天文数字并
	// 让 GOMEMLIMIT 形同虚设 —— 而那正好是最需要它的部署形态。
	unlimitedSentinel = int64(1) << 60
)

// cgroupPaths 按 cgroup v2 → v1 的顺序查找内存上限文件。
//
// 是可变量而非常量，唯一目的是让测试能指向临时文件：这两个路径在本机
// （macOS）不存在，不注入就永远走不到解析逻辑，那部分会变成「测不到的死代码」。
var cgroupPaths = []string{
	"/sys/fs/cgroup/memory.max",                   // cgroup v2
	"/sys/fs/cgroup/memory/memory.limit_in_bytes", // cgroup v1
}

// Apply 按容器内存上限设置 GOMEMLIMIT，返回生效的字节数。
//
// 返回 0 表示未设置，且三种情况都属正常：不在受限容器里（宿主裸跑、
// cgroup 无上限）、GOMEMLIMIT 已被显式配置、或按比例算出的值小于 minLimit。
// 调用方应当只在返回非 0 时记录「已生效」——把 0 也报成生效，
// 会让「配了没效果」与「本来就不需要」同形。
func Apply() int64 {
	// 显式配置优先。不解析它的取值：GOMEMLIMIT 支持 "8GiB" 这类带单位写法，
	// 复制一份单位解析逻辑只会多一个与运行时不一致的地方。
	if strings.TrimSpace(os.Getenv("GOMEMLIMIT")) != "" {
		return 0
	}
	total := cgroupLimit()
	if total <= 0 {
		return 0
	}
	limit := total / 100 * percent
	if limit < minLimit {
		return 0
	}
	debug.SetMemoryLimit(limit)
	return limit
}

// cgroupLimit 返回容器内存上限，0 表示「无上限或读不到」。
func cgroupLimit() int64 {
	for _, p := range cgroupPaths {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(b))
		// cgroup v2 用 "max" 表示无上限，不是数字也不是错误。
		if s == "max" {
			continue
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n <= 0 || n >= unlimitedSentinel {
			continue
		}
		return n
	}
	return 0
}

// Format 把字节数渲染成便于比对的人类可读形式，只用于日志。
//
// 存在的理由是「日志要能跟编排文件对着看」：compose 里写的是 2g，
// 日志里打 2147483648 就得心算一遍才能确认是不是同一个数 ——
// 而这类「数字对不上」正是靠启动日志排查的那类问题。
func Format(n int64) string {
	switch {
	case n >= 1<<30:
		return strconv.FormatFloat(float64(n)/(1<<30), 'f', 2, 64) + "GiB"
	case n >= 1<<20:
		return strconv.FormatInt(n>>20, 10) + "MiB"
	default:
		return strconv.FormatInt(n, 10) + "B"
	}
}
