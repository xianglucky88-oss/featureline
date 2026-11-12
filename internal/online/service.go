// Package online 是线上特征计算服务。
//
// 线上侧的计算口径：
//
//	窗口右端 = 请求时刻（time.Now）
//	窗口区间 = [T-窗口长度, T)，左闭右开
//	缺失特征先记 NaN，再替换为默认值
package online

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/internal-platform/featureline/internal/feature"
	"github.com/internal-platform/featureline/internal/store"
)

// Service 计算线上特征。
type Service struct {
	store store.Store
	reg   *feature.Registry
	// now 可在测试中替换，用于固定时间。
	now func() time.Time
}

// New 构造服务。
func New(st store.Store, reg *feature.Registry) *Service {
	return &Service{store: st, reg: reg, now: time.Now}
}

// WithClock 替换时钟（测试用）。
func (s *Service) WithClock(f func() time.Time) *Service {
	s.now = f
	return s
}

// FeatureVector 是一次线上请求的结果。
type FeatureVector struct {
	UserID   string             `json:"user_id"`
	Features map[string]float64 `json:"features"`
	// Debug 记录每个特征的来源与缺失情况，供 parity 排查。
	Debug map[string]FeatureDebug `json:"debug"`
	// AsOf 是本次计算的时间基准。
	AsOf time.Time `json:"as_of"`
}

// FeatureDebug 记录单个特征的计算细节。
type FeatureDebug struct {
	Found     bool      `json:"found"`
	Raw       float64   `json:"-"`
	Filled    float64   `json:"filled"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	// WindowStart / WindowEnd 是本次查询用的窗口。
	// 离线侧应当能算出同样的区间——这是 parity 的核对点。
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	// Note 记录本次取值的处理说明，例如 "missing -> NaN -> 0"。
	Note string `json:"note,omitempty"`
}

// MarshalJSON 把 NaN / Inf 序列化成 null。
//
// 背景：encoding/json 不支持 NaN，直接 Encode 会返回错误且写出 0 字节。
// 早期版本没有这个处理，导致任何含缺失特征的请求都返回空 body（HTTP 200 但无内容），
// 排查了很久才发现是 NaN 而不是"服务挂了"。
func (d FeatureDebug) MarshalJSON() ([]byte, error) {
	type alias FeatureDebug // 避免递归
	raw := any(nil)
	if !math.IsNaN(d.Raw) && !math.IsInf(d.Raw, 0) {
		raw = d.Raw
	}
	return json.Marshal(struct {
		alias
		Raw any `json:"raw"`
	}{alias: alias(d), Raw: raw})
}

// Compute 计算一个用户的全部特征。
func (s *Service) Compute(ctx context.Context, userID string) (*FeatureVector, error) {
	now := s.now().UTC()
	names := s.reg.Names()
	values, err := s.store.MGet(ctx, userID, names)
	if err != nil {
		return nil, fmt.Errorf("mget: %w", err)
	}

	fv := &FeatureVector{
		UserID:   userID,
		Features: make(map[string]float64, len(names)),
		Debug:    make(map[string]FeatureDebug, len(names)),
		AsOf:     now,
	}

	for _, name := range names {
		spec := s.reg.Specs[name]
		// 窗口右端取当前时刻。
		windowEnd := now
		windowStart := windowEnd.Add(-spec.Window)

		v, ok := values[name]
		dbg := FeatureDebug{
			Found:       ok,
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
		}

		if !ok {
			// 缺失时先记 NaN，再替换为默认值。
			dbg.Raw = math.NaN()
			dbg.Note = "missing -> NaN -> 0 (serving)"
			dbg.Filled = spec.Default
			fv.Features[name] = spec.Default
			fv.Debug[name] = dbg
			continue
		}

		dbg.Raw = v.Score
		dbg.UpdatedAt = v.UpdatedAt
		dbg.Filled = sanitize(v.Score, spec.Default)
		if dbg.Filled != v.Score {
			dbg.Note = "non-finite value replaced with default"
		}
		fv.Features[name] = dbg.Filled
		fv.Debug[name] = dbg
	}

	return fv, nil
}

// sanitize 把 NaN / Inf 替换成默认值。
//
// 替换发生在返回前，但原始 NaN 已经写进 debug 字段。
func sanitize(v, def float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return def
	}
	return v
}

// WindowFor 返回某特征在给定时间点的窗口区间（供 parity 脚本调用）。
func (s *Service) WindowFor(name string, asOf time.Time) (time.Time, time.Time, error) {
	spec, ok := s.reg.Specs[name]
	if !ok {
		return time.Time{}, time.Time{}, fmt.Errorf("unknown feature %q", name)
	}
	end := asOf.UTC()
	start := end.Add(-spec.Window)
	return start, end, nil
}
