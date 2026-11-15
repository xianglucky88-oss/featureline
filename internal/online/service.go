// Package online 是线上特征计算服务。
//
// 服务不再持有“当前特征名集合”，而是持有一份不可变 Binding：
// 每个特征按 FQVN（name@major.minor）从存储读值，窗口、默认值、时间语义全部
// 来自绑定解析出的已发布版本。旧模型绑定旧 major，就永远读到旧语义的值。
//
// 窗口口径由注册表统一定义为左闭右开 [T-size, T)，时区 UTC。
package online

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/internal-platform/featureline/internal/registry"
	"github.com/internal-platform/featureline/internal/store"
)

// Service 计算线上特征。
type Service struct {
	store    store.Store
	reg      *registry.Registry
	binding  registry.Binding
	versions map[string]registry.Version // name -> pinned version
	// now 可在测试中替换，用于固定时间。
	now func() time.Time
	// asOfOverride 非空时强制使用该时间基准（来自快照清单的事件时间）。
	asOfOverride *time.Time
}

// New 用绑定清单构造服务。绑定在启动时一次性解析失败即拒绝启动，
// 不允许“带着未知语义”上线。
func New(st store.Store, reg *registry.Registry, binding registry.Binding) (*Service, error) {
	versions, err := reg.Resolve(binding)
	if err != nil {
		return nil, fmt.Errorf("online: resolve binding: %w", err)
	}
	s := &Service{
		store: st, reg: reg, binding: binding, versions: versions,
		now: time.Now,
	}
	// 快照清单声明了事件时间基准时，以它为准，与离线窗口右端对齐。
	if ms, ok := st.(interface{ SnapshotAsOf() *time.Time }); ok {
		s.asOfOverride = ms.SnapshotAsOf()
	}
	return s, nil
}

// WithClock 替换时钟（测试用）。
func (s *Service) WithClock(f func() time.Time) *Service {
	s.now = f
	return s
}

// Binding 返回服务当前固定的绑定。
func (s *Service) Binding() registry.Binding { return s.binding }

// AsOf 返回本次实例采用的时间基准。
func (s *Service) AsOf() time.Time {
	if s.asOfOverride != nil {
		return s.asOfOverride.UTC()
	}
	return s.now().UTC()
}

// FeatureVector 是一次线上请求的结果。
type FeatureVector struct {
	UserID   string             `json:"user_id"`
	Features map[string]float64 `json:"features"`
	// Versions 给出每个特征值实际采用的 FQVN，消费者可据此核对语义来源。
	Versions map[string]string `json:"versions"`
	// ManifestID / BindingFingerprint 标识本实例固定的定义集合。
	ManifestID         string `json:"manifest_id"`
	BindingFingerprint string `json:"binding_fingerprint"`
	// Debug 记录每个特征的来源与缺失情况，供 parity 排查。
	Debug map[string]FeatureDebug `json:"debug"`
	// AsOf 是本次计算的时间基准。
	AsOf time.Time `json:"as_of"`
}

// FeatureDebug 记录单个特征的计算细节。
type FeatureDebug struct {
	FQVN        string    `json:"fqvn"`
	Found       bool      `json:"found"`
	Raw         float64   `json:"-"`
	Filled      float64   `json:"filled"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Note        string    `json:"note,omitempty"`
}

// MarshalJSON 把 NaN / Inf 序列化成 null。
//
// 背景：encoding/json 不支持 NaN，直接 Encode 会返回错误且写出 0 字节。
func (d FeatureDebug) MarshalJSON() ([]byte, error) {
	type alias FeatureDebug
	raw := any(nil)
	if !math.IsNaN(d.Raw) && !math.IsInf(d.Raw, 0) {
		raw = d.Raw
	}
	return json.Marshal(struct {
		alias
		Raw any `json:"raw"`
	}{alias: alias(d), Raw: raw})
}

// Compute 计算一个用户的全部特征（严格按绑定版本）。
func (s *Service) Compute(ctx context.Context, userID string) (*FeatureVector, error) {
	now := s.AsOf()
	names := s.pinnedNames()
	fqvns := make([]string, 0, len(names))
	for _, n := range names {
		fqvns = append(fqvns, s.versions[n].FQVN)
	}
	values, err := s.store.MGet(ctx, userID, fqvns)
	if err != nil {
		return nil, fmt.Errorf("mget: %w", err)
	}

	fv := &FeatureVector{
		UserID:             userID,
		Features:           make(map[string]float64, len(names)),
		Versions:           make(map[string]string, len(names)),
		ManifestID:         s.binding.ManifestID,
		BindingFingerprint: s.binding.Fingerprint,
		Debug:              make(map[string]FeatureDebug, len(names)),
		AsOf:               now,
	}

	for _, name := range names {
		ver := s.versions[name]
		def := ver.Definition
		windowEnd := now
		windowStart := windowEnd.Add(-time.Duration(def.Window.SizeSeconds) * time.Second)

		v, ok := values[ver.FQVN]
		dbg := FeatureDebug{
			FQVN: ver.FQVN, Found: ok,
			WindowStart: windowStart, WindowEnd: windowEnd,
		}

		defVal, perr := parseDefault(def.Default)
		if perr != nil {
			return nil, fmt.Errorf("default of %s: %w", ver.FQVN, perr)
		}

		if !ok {
			dbg.Raw = math.NaN()
			dbg.Note = "missing -> NaN -> default (serving)"
			dbg.Filled = defVal
			fv.Features[name] = defVal
			fv.Versions[name] = ver.FQVN
			fv.Debug[name] = dbg
			continue
		}

		dbg.Raw = v.Score
		dbg.UpdatedAt = v.UpdatedAt
		dbg.Filled = sanitize(v.Score, defVal)
		if dbg.Filled != v.Score {
			dbg.Note = "non-finite value replaced with default"
		}
		fv.Features[name] = dbg.Filled
		fv.Versions[name] = ver.FQVN
		fv.Debug[name] = dbg
	}

	return fv, nil
}

// WindowFor 返回某绑定特征在给定时间点的窗口区间（供 parity 脚本调用）。
func (s *Service) WindowFor(name string, asOf time.Time) (time.Time, time.Time, error) {
	ver, ok := s.versions[name]
	if !ok {
		return time.Time{}, time.Time{}, fmt.Errorf("feature %q not in binding %s", name, s.binding.ManifestID)
	}
	end := asOf.UTC()
	start := end.Add(-time.Duration(ver.Definition.Window.SizeSeconds) * time.Second)
	return start, end, nil
}

// PinnedVersion 暴露某特征的绑定版本，供管理面与 parity 使用。
func (s *Service) PinnedVersion(name string) (registry.Version, bool) {
	v, ok := s.versions[name]
	return v, ok
}

func (s *Service) pinnedNames() []string {
	names := make([]string, 0, len(s.versions))
	for n := range s.versions {
		names = append(names, n)
	}
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	return names
}

// sanitize 把 NaN / Inf 替换成默认值。
func sanitize(v, def float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return def
	}
	return v
}

// parseDefault 按定义里的类型解析默认值字符串，float/int 都归一到 float64 输出。
func parseDefault(d registry.DefaultSpec) (float64, error) {
	switch d.Type {
	case registry.ValueFloat64, registry.ValueInt64:
		var f float64
		if _, err := fmt.Sscanf(d.Value, "%g", &f); err != nil {
			return 0, fmt.Errorf("parse numeric default %q: %w", d.Value, err)
		}
		return f, nil
	case registry.ValueString:
		// 字符串类型特征不参与数值向量，预留分支。
		return 0, nil
	default:
		return 0, fmt.Errorf("unknown default type %q", d.Type)
	}
}
