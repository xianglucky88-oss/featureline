// Package feature 定义特征的统一 schema。
//
// 这是 Go 侧与 Python 侧唯一共享的"契约"：
// Python 写 Parquet 时的列名、类型必须与这里的字段一致。
//
// 当前问题：两边只靠"约定"对齐，没有任何自动校验。
// Python 侧用 event_time，Go 侧用写入时间，这类偏差只能靠 parity 脚本发现。
package feature

import "time"

// Kind 是特征类型。
type Kind string

const (
	KindNumeric     Kind = "numeric"
	KindCategorical Kind = "categorical"
	KindCount       Kind = "count"
	KindRatio       Kind = "ratio"
)

// Spec 是一个特征的定义。
type Spec struct {
	Name string `json:"name"`
	Kind Kind   `json:"kind"`
	// Window 是滑动窗口长度。0 表示全量累计。
	Window time.Duration `json:"window_ns"`
	// EventTimeColumn 是离线侧用于窗口归属的时间列。
	// 线上侧没有这个选项，它用的是"当前时间"——这就是漂移的源头。
	EventTimeColumn string `json:"event_time_column"`
	// Default 是缺失时的填充值。
	// 离线与线上对缺失值的处理不同。
	Default float64 `json:"default"`
	Description string `json:"description"`
}

// Registry 是特征定义的集合。
type Registry struct {
	Specs map[string]Spec
}

// DefaultRegistry 返回当前线上的 6 个特征。
func DefaultRegistry() *Registry {
	return &Registry{Specs: map[string]Spec{
		"user_7d_order_cnt": {
			Name:            "user_7d_order_cnt",
			Kind:            KindCount,
			Window:          7 * 24 * time.Hour,
			EventTimeColumn: "event_time",
			Default:         0,
			Description:     "用户近 7 天订单数",
		},
		"user_7d_gmv_cents": {
			Name:            "user_7d_gmv_cents",
			Kind:            KindNumeric,
			Window:          7 * 24 * time.Hour,
			EventTimeColumn: "event_time",
			Default:         0,
			Description:     "用户近 7 天成交金额（分）",
		},
		"user_30d_order_cnt": {
			Name:            "user_30d_order_cnt",
			Kind:            KindCount,
			Window:          30 * 24 * time.Hour,
			EventTimeColumn: "event_time",
			Default:         0,
			Description:     "用户近 30 天订单数",
		},
		"item_7d_ctr": {
			Name:            "item_7d_ctr",
			Kind:            KindRatio,
			Window:          7 * 24 * time.Hour,
			EventTimeColumn: "event_time",
			Default:         0,
			Description:     "商品近 7 天点击率",
		},
		"item_7d_exposure": {
			Name:            "item_7d_exposure",
			Kind:            KindCount,
			Window:          7 * 24 * time.Hour,
			EventTimeColumn: "event_time",
			Default:         0,
			Description:     "商品近 7 天曝光次数",
		},
		"user_category_affinity": {
			Name:            "user_category_affinity",
			Kind:            KindCategorical,
			Window:          30 * 24 * time.Hour,
			EventTimeColumn: "event_time",
			Default:         0,
			Description:     "用户偏好品类（编码后的类别 id）",
		},
	}}
}

// Names 返回按字典序排列的特征名。
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.Specs))
	for k := range r.Specs {
		out = append(out, k)
	}
	// 手工排序，避免额外 import
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
