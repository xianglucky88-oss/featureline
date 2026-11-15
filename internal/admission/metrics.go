package admission

import (
	"sync/atomic"
	"time"
)

// 直方图桶上界（秒）。最后一个 +Inf 桶在 histogram.snapshot 中补全。
var defaultBucketBounds = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// histogram 是固定边界的无锁计数直方图，只记录落桶次数，
// 避免引入外部依赖，也避免在热路径上做切片分配。
type histogram struct {
	bounds []float64
	counts []atomic.Int64
	sum    atomic.Int64 // 纳秒累计
	count  atomic.Int64
}

func newHistogram(bounds []float64) *histogram {
	return &histogram{bounds: bounds, counts: make([]atomic.Int64, len(bounds)+1)}
}

func (h *histogram) observe(d time.Duration) {
	secs := d.Seconds()
	idx := len(h.bounds) // 默认落入 +Inf 桶
	for i, b := range h.bounds {
		if secs <= b {
			idx = i
			break
		}
	}
	h.counts[idx].Add(1)
	h.sum.Add(int64(d))
	h.count.Add(1)
}

type histogramSnapshot struct {
	Bounds []float64 `json:"bounds_seconds"`
	// Counts 比 Bounds 多一个元素，末尾是 +Inf 桶。
	Counts         []int64 `json:"counts"`
	Observations   int64   `json:"observations"`
	SumSeconds     float64 `json:"sum_seconds"`
	AverageSeconds float64 `json:"avg_seconds"`
}

func (h *histogram) snapshot() histogramSnapshot {
	counts := make([]int64, len(h.counts))
	for i := range h.counts {
		counts[i] = h.counts[i].Load()
	}
	bounds := append([]float64(nil), h.bounds...)
	n := h.count.Load()
	avg := 0.0
	if n > 0 {
		avg = float64(h.sum.Load()) / float64(n) / float64(time.Second)
	}
	return histogramSnapshot{
		Bounds:         bounds,
		Counts:         counts,
		Observations:   n,
		SumSeconds:     float64(h.sum.Load()) / float64(time.Second),
		AverageSeconds: avg,
	}
}

// laneMetrics 是单个泳道的全部计数器与水位。
//
// 计数器只增不减（gauge 类指标除外），失败语义的每个分支都有独立计数，
// 这样"主动拒绝 / 排队超时 / 处理超时 / 客户端取消 / 内部故障"在
// /metrics 上能直接区分，不需要从日志里猜。
type laneMetrics struct {
	submitted         atomic.Int64
	admitted          atomic.Int64
	completed         atomic.Int64
	rejectedQueueFull atomic.Int64
	queueTimeouts     atomic.Int64
	handleTimeouts    atomic.Int64
	clientCanceled    atomic.Int64
	internalErrors    atomic.Int64

	queued            atomic.Int64
	queuedWatermark   atomic.Int64
	inFlight          atomic.Int64
	inFlightWatermark atomic.Int64

	queueWait   *histogram
	serviceTime *histogram
}

func newLaneMetrics() *laneMetrics {
	return &laneMetrics{
		queueWait:   newHistogram(defaultBucketBounds),
		serviceTime: newHistogram(defaultBucketBounds),
	}
}

func (m *laneMetrics) recordQueueWait(d time.Duration)   { m.queueWait.observe(d) }
func (m *laneMetrics) recordServiceTime(d time.Duration) { m.serviceTime.observe(d) }

// LaneMetrics 是某一时刻的泳道指标快照。
type LaneMetrics struct {
	Lane string `json:"lane"`

	MaxInFlight int `json:"max_in_flight"`
	MaxQueued   int `json:"max_queued"`

	Submitted         int64 `json:"submitted"`
	Admitted          int64 `json:"admitted"`
	Completed         int64 `json:"completed"`
	RejectedQueueFull int64 `json:"rejected_queue_full"`
	QueueTimeouts     int64 `json:"queue_timeouts"`
	HandleTimeouts    int64 `json:"handle_timeouts"`
	ClientCanceled    int64 `json:"client_canceled"`
	InternalErrors    int64 `json:"internal_errors"`

	Queued            int64 `json:"queued_now"`
	QueuedWatermark   int64 `json:"queued_watermark"`
	InFlight          int64 `json:"in_flight_now"`
	InFlightWatermark int64 `json:"in_flight_watermark"`

	QueueWaitHistogram   histogramSnapshot `json:"queue_wait"`
	ServiceTimeHistogram histogramSnapshot `json:"service_time"`
}

func (m *laneMetrics) snapshot(name string, cfg LaneConfig) LaneMetrics {
	return LaneMetrics{
		Lane:                 name,
		MaxInFlight:          cfg.MaxInFlight,
		MaxQueued:            cfg.MaxQueued,
		Submitted:            m.submitted.Load(),
		Admitted:             m.admitted.Load(),
		Completed:            m.completed.Load(),
		RejectedQueueFull:    m.rejectedQueueFull.Load(),
		QueueTimeouts:        m.queueTimeouts.Load(),
		HandleTimeouts:       m.handleTimeouts.Load(),
		ClientCanceled:       m.clientCanceled.Load(),
		InternalErrors:       m.internalErrors.Load(),
		Queued:               m.queued.Load(),
		QueuedWatermark:      m.queuedWatermark.Load(),
		InFlight:             m.inFlight.Load(),
		InFlightWatermark:    m.inFlightWatermark.Load(),
		QueueWaitHistogram:   m.queueWait.snapshot(),
		ServiceTimeHistogram: m.serviceTime.snapshot(),
	}
}

// Snapshot 是整个控制器的指标快照。
type Snapshot struct {
	At        time.Time     `json:"at"`
	Accepting bool          `json:"accepting"`
	Lanes     []LaneMetrics `json:"lanes"`
}
