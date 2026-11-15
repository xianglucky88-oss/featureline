// Package registry 是特征定义的注册与演进中心。
//
// 核心概念：
//
//   - Definition：特征的语义本体（实体粒度、值类型、时间语义、窗口、缺失默认值）。
//   - Version：Definition 发布后得到的不可变版本，标识为 FQVN（name@major.minor），
//     并带一个跨语言一致的规范化指纹 Fingerprint。
//   - Lifecycle：draft（可改，带 revision 乐观锁）→ published（不可变）→ deprecated（仍可读）。
//   - Binding：消费者（训练任务 / 快照 / 在线实例）固定引用的一组已发布版本，
//     自身也有指纹；任何一次特征交付都能凭它追溯语义来源。
//
// 治理规则不阻断服务：弃用、新版本发布都不会删除或改写旧版本，已确认的绑定永远可解析。
package registry

import (
	"regexp"
	"time"
)

// ValueType 是特征值的跨语言类型枚举。
type ValueType string

const (
	ValueFloat64 ValueType = "float64"
	ValueInt64   ValueType = "int64"
	ValueString  ValueType = "string"
)

// Entity 是特征依附的实体粒度。
type Entity string

const (
	EntityUser Entity = "user"
	EntityItem Entity = "item"
)

// WindowConvention 是窗口区间约定。全平台只允许左闭右开，
// 枚举值本身即序列化形态，避免跨语言对 "[start, end)" 这类字符串的空格分歧。
type WindowConvention string

const WindowHalfOpenLeft WindowConvention = "[start,end)"

// WindowSpec 描述窗口长度与区间约定。SizeSeconds=0 表示全量累计。
type WindowSpec struct {
	SizeSeconds int64            `json:"size_seconds"`
	Convention  WindowConvention `json:"convention"`
}

// TimeSemantics 描述窗口归属的时间语义。
type TimeSemantics struct {
	// TimeColumn 是离线侧用于窗口归属的事件时间列。
	TimeColumn string `json:"time_column"`
	// RightBound 是窗口右端基准：event_time（事件时间）或 request_time（请求时刻）。
	RightBound string `json:"time_right_bound"`
	// TimeZone 是窗口对齐的时区，IANA 名称，UTC 写 "UTC"。
	TimeZone string `json:"time_zone"`
}

// DefaultSpec 是缺失默认值。值统一用字符串承载，配合 Type 解析，
// 使 int64/float64/string 三类默认值在 JSON 里只有一种表示。
type DefaultSpec struct {
	Type  ValueType `json:"type"`
	Value string    `json:"value"`
}

// Definition 是特征的语义本体。发布之后内容不可变。
type Definition struct {
	Name        string        `json:"name"`
	Entity      Entity        `json:"entity"`
	ValueType   ValueType     `json:"value_type"`
	Kind        string        `json:"kind"`
	Window      WindowSpec    `json:"window"`
	Time        TimeSemantics `json:"time_semantics"`
	Default     DefaultSpec   `json:"default"`
	Description string        `json:"description"`
	Owner       string        `json:"owner"`
	Tags        []string      `json:"tags"`
}

// LifecycleStatus 是版本生命周期状态。
type LifecycleStatus string

const (
	StatusDraft      LifecycleStatus = "draft"
	StatusPublished  LifecycleStatus = "published"
	StatusDeprecated LifecycleStatus = "deprecated"
)

// Version 是一个特征定义的版本记录。
type Version struct {
	FQVN       string          `json:"fqvn"`
	Name       string          `json:"name"`
	Major      int             `json:"major"`
	Minor      int             `json:"minor"`
	Status     LifecycleStatus `json:"status"`
	Definition Definition      `json:"definition"`
	// Fingerprint 是 Definition 规范化编码后的 SHA-256，跨语言必须一致。
	Fingerprint string `json:"fingerprint"`
	// BasedOn 是演进链上一版的 FQVN；首个版本为空。
	BasedOn string `json:"based_on,omitempty"`
	// ChangeNote 是本次变更说明。
	ChangeNote string `json:"change_note,omitempty"`
	// Revision 是草稿的乐观锁版本；发布版本固定为 -1。
	Revision     int64      `json:"revision"`
	CreatedAt    time.Time  `json:"created_at"`
	PublishedAt  *time.Time `json:"published_at,omitempty"`
	DeprecatedAt *time.Time `json:"deprecated_at,omitempty"`
	// Successor 是弃用迁移指向的新版本 FQVN。
	Successor string     `json:"successor,omitempty"`
	SunsetAt  *time.Time `json:"sunset_at,omitempty"`
}

// State 是注册表的持久化状态，整体原子替换。
type State struct {
	SchemaVersion int                `json:"schema_version"`
	Versions      map[string]Version `json:"versions"`
	Drafts        map[string]Version `json:"drafts"`
	// Active 是每个特征名当前默认发布版本的 FQVN。
	Active    map[string]string `json:"active"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// BindingEntry 是绑定中的一条版本引用。
type BindingEntry struct {
	Name        string `json:"name"`
	FQVN        string `json:"fqvn"`
	Fingerprint string `json:"fingerprint"`
}

// Binding 是消费者固定引用的一组不可变版本。
type Binding struct {
	ManifestID string `json:"manifest_id"`
	// Fingerprint 覆盖 manifest_id + entries，清单被手改即可发现。
	Fingerprint string         `json:"fingerprint"`
	AsOf        *time.Time     `json:"as_of,omitempty"`
	Entries     []BindingEntry `json:"entries"`
	CreatedAt   time.Time      `json:"created_at"`
}

// ChangeClass 是相邻两版定义的兼容性判定结果。
type ChangeClass string

const (
	ChangeCompatible ChangeClass = "compatible"
	ChangeBreaking   ChangeClass = "breaking"
)

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// ValidName 校验特征名：小写字母开头、小写字母数字下划线，避免跨语言命名分歧。
func ValidName(name string) bool { return namePattern.MatchString(name) }

// Classify 判定 next 相对 prev 的变化级别。
//
// 任何语义字段（实体粒度、值类型、类型语义 kind、窗口、时间语义、默认值）变化都是
// breaking——旧模型在无感知下读到新含义数值的唯一根因就是这些字段；
// 描述、负责人、标签等元数据变化属于 compatible，只产生 minor 版本。
func Classify(prev, next Definition) ChangeClass {
	switch {
	case prev.Entity != next.Entity:
		return ChangeBreaking
	case prev.ValueType != next.ValueType:
		return ChangeBreaking
	case prev.Kind != next.Kind:
		return ChangeBreaking
	case prev.Window != next.Window:
		return ChangeBreaking
	case prev.Time != next.Time:
		return ChangeBreaking
	case prev.Default != next.Default:
		return ChangeBreaking
	}
	return ChangeCompatible
}

// SeedDefinitions 返回线上最初的 6 个特征定义，全部采用统一的事件时间、
// UTC、左闭右开窗口语义，作为注册表的初始发布内容。
func SeedDefinitions() []Definition {
	week := int64(7 * 24 * 3600)
	month := int64(30 * 24 * 3600)
	ev := TimeSemantics{TimeColumn: "event_time", RightBound: "event_time", TimeZone: "UTC"}
	def := func(name string, entity Entity, kind string, window int64, desc string) Definition {
		return Definition{
			Name: name, Entity: entity, ValueType: ValueFloat64, Kind: kind,
			Window:      WindowSpec{SizeSeconds: window, Convention: WindowHalfOpenLeft},
			Time:        ev,
			Default:     DefaultSpec{Type: ValueFloat64, Value: "0"},
			Description: desc, Owner: "platform", Tags: []string{},
		}
	}
	return []Definition{
		def("user_7d_order_cnt", EntityUser, "count", week, "用户近 7 天订单数"),
		def("user_7d_gmv_cents", EntityUser, "numeric", week, "用户近 7 天成交金额（分）"),
		def("user_30d_order_cnt", EntityUser, "count", month, "用户近 30 天订单数"),
		def("item_7d_ctr", EntityItem, "ratio", week, "商品近 7 天点击率"),
		def("item_7d_exposure", EntityItem, "count", week, "商品近 7 天曝光次数"),
		def("user_category_affinity", EntityUser, "categorical", month, "用户偏好品类（编码后的类别 id）"),
	}
}
