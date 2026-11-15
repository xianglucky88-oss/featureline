// Package admission 实现面向在线流量的舱壁隔离与过载控制。
//
// 设计要点：
//
//   - 泳道（lane）之间互相独立：在线读取与后台写入各持有自己的并发槽和
//     有界队列，一个泳道被打满不会拖垮另一个。
//   - 每个请求依次经过两道有界边界：先取得一个"排队位"（有界 channel），
//     再等待"在途槽"（计数器 + 条件变量）。排队位满时立即拒绝，不存在
//     无限排队。
//   - 两类超时分别可观测：排队超时（还没被处理就放弃）与处理超时
//     （已接受但下游在期限内没做完），二者都通过 context 向下传播。
//   - 关闭时进入 draining：拒绝新请求，已排队/在途的请求继续做完，
//     不会被粗暴终止；Shutdown 的等待期限只约束"等多久"，不强杀 goroutine。
//
// 在途槽用互斥锁保护的计数器而不是带缓冲 channel 实现，是因为
// "释放槽"与"等待者入槽"必须和并发 gauge 的更新构成同一个原子临界区；
// 拆成两个 channel 会在释放与重新获取之间留下窗口，让 gauge 超调。
//
// 失败分类见 Kind，HTTP 层据此映射成不同状态码与可重试标记。
package admission

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// 泳道名。HTTP 层按路由把请求归入对应泳道。
const (
	LaneRead  = "read"
	LaneAdmin = "admin"
)

// Kind 是准入/执行失败的分类，调用方据此决定是否重试。
type Kind string

const (
	// KindRejected 主动拒绝：泳道队列已满或正在排空，请求未被接受。
	KindRejected Kind = "rejected"
	// KindQueueTimeout 排队超时：在 QueueTimeout 内没等到在途槽。
	KindQueueTimeout Kind = "queue_timeout"
	// KindDeadlineExceeded 处理超时：请求已接受，但 HandleTimeout 内未完成。
	// 结果是否生效不可知，调用方只能安全重试。
	KindDeadlineExceeded Kind = "deadline_exceeded"
	// KindCanceled 客户端取消：上游 context 先于完成被取消。
	KindCanceled Kind = "canceled"
	// KindDraining 服务关闭排空中，不接受新请求。
	KindDraining Kind = "draining"
)

// ErrHandleDeadline 是处理超时写入 context 的 cause，
// 用于和"父 context 被客户端取消"区分开。
var ErrHandleDeadline = errors.New("admission: handle deadline exceeded")

// Error 是带分类的准入错误，transport 无关。
type Error struct {
	Kind   Kind
	Lane   string
	Reason string
}

func (e *Error) Error() string {
	return fmt.Sprintf("admission %s on lane %q: %s", e.Kind, e.Lane, e.Reason)
}

// LaneConfig 是单个泳道的容量与超时边界。
type LaneConfig struct {
	Name string
	// MaxInFlight 是同时执行的请求上限（并发槽），必须 > 0。
	MaxInFlight int
	// MaxQueued 是等待队列长度，0 表示不允许排队（槽满即拒绝）。
	MaxQueued int
	// QueueTimeout 是排队位的最长等待，0 表示不设排队超时。
	QueueTimeout time.Duration
	// HandleTimeout 是单个请求拿到槽之后的处理期限，0 表示不设处理超时。
	HandleTimeout time.Duration
}

// Controller 管理一组互相隔离的泳道。零值不可用，用 New 构造。
type Controller struct {
	lanes map[string]*lane
	order []string
}

// New 构造控制器并校验容量配置。
func New(cfgs ...LaneConfig) (*Controller, error) {
	c := &Controller{lanes: map[string]*lane{}}
	for _, cfg := range cfgs {
		if cfg.Name == "" {
			return nil, errors.New("admission: lane name required")
		}
		if _, dup := c.lanes[cfg.Name]; dup {
			return nil, fmt.Errorf("admission: duplicate lane %q", cfg.Name)
		}
		if cfg.MaxInFlight <= 0 {
			return nil, fmt.Errorf("admission: lane %q MaxInFlight must be > 0", cfg.Name)
		}
		if cfg.MaxQueued < 0 {
			return nil, fmt.Errorf("admission: lane %q MaxQueued must be >= 0", cfg.Name)
		}
		if cfg.QueueTimeout < 0 || cfg.HandleTimeout < 0 {
			return nil, fmt.Errorf("admission: lane %q timeouts must be >= 0", cfg.Name)
		}
		c.lanes[cfg.Name] = newLane(cfg)
		c.order = append(c.order, cfg.Name)
	}
	return c, nil
}

// Grant 是一次成功准入的凭证，用 Run 执行业务逻辑。
type Grant struct {
	Lane     string
	QueuedAt time.Time
	// Wait 是从入队到拿到在途槽的排队耗时。
	Wait time.Duration

	lane *lane
}

// Acquire 尝试进入指定泳道。
//
// 返回的 *Error 永远非 nil 即表示请求未被接受（拒绝/排队超时/取消/排空中），
// 调用方不需要也不能释放任何资源；成功时返回 Grant，必须经由 Run 释放槽位。
func (c *Controller) Acquire(ctx context.Context, name string) (*Grant, *Error) {
	l, ok := c.lanes[name]
	if !ok {
		return nil, &Error{Kind: KindRejected, Lane: name, Reason: "unknown lane"}
	}
	if err := ctx.Err(); err != nil {
		l.metrics.clientCanceled.Add(1)
		return nil, l.fail(KindCanceled, "context already done: "+err.Error())
	}
	if l.isDraining() {
		return nil, l.fail(KindDraining, "lane is draining")
	}

	l.metrics.submitted.Add(1)

	// 第一道边界：有界排队位。拿不到立即拒绝，绝不阻塞调用方。
	// MaxQueued=0 时 queue 为 nil，直接进入在途槽竞争。
	queued := false
	if l.queue != nil {
		select {
		case l.queue <- struct{}{}:
			queued = true
		default:
			l.metrics.rejectedQueueFull.Add(1)
			return nil, l.fail(KindRejected, "queue is full")
		}
		l.queuedIn()
	}
	queuedAt := time.Now()
	releaseQueue := func() {
		if queued {
			<-l.queue
			l.queuedOut()
		}
	}

	// 第二道边界：在途槽。block=false 用于无队列泳道的即时探测。
	got, kind := l.acquireSlot(ctx, queued, l.cfg.QueueTimeout)
	if !got {
		releaseQueue()
		switch kind {
		case KindCanceled:
			l.metrics.clientCanceled.Add(1)
			return nil, l.fail(KindCanceled, "canceled while waiting for a slot: "+ctx.Err().Error())
		case KindQueueTimeout:
			l.metrics.queueTimeouts.Add(1)
			return nil, l.fail(KindQueueTimeout, "timed out waiting for an in-flight slot")
		case KindDraining:
			return nil, l.fail(KindDraining, "shutdown started while queued")
		case KindRejected:
			l.metrics.rejectedQueueFull.Add(1)
			return nil, l.fail(KindRejected, "all in-flight slots are busy")
		default:
			return nil, l.fail(KindRejected, "slot unavailable")
		}
	}

	releaseQueue()
	wait := time.Since(queuedAt)
	l.metrics.admitted.Add(1)
	l.metrics.recordQueueWait(wait)
	return &Grant{Lane: name, QueuedAt: queuedAt, Wait: wait, lane: l}, nil
}

// Run 在已获得的槽位内执行业务函数，并负责分类结果与释放槽位。
//
// fn 收到的 ctx 同时受父 context（客户端连接）和 HandleTimeout 约束：
//   - 处理期限先到 -> *Error{Kind: KindDeadlineExceeded}
//   - 父 context 先取消 -> *Error{Kind: KindCanceled}
//   - fn 自身返回的其它错误原样向上传递，由 transport 层归为内部故障
func (g *Grant) Run(parent context.Context, fn func(ctx context.Context) error) error {
	l := g.lane

	ctx, cancel := context.WithCancel(parent)
	if l.cfg.HandleTimeout > 0 {
		ctx, cancel = context.WithDeadlineCause(parent, time.Now().Add(l.cfg.HandleTimeout), ErrHandleDeadline)
	}
	defer cancel()

	start := time.Now()
	runErr := fn(ctx)
	elapsed := time.Since(start)

	l.metrics.completed.Add(1)
	l.metrics.recordServiceTime(elapsed)

	// 无论业务怎么返回，先按 context 终态归类控制类失败——
	// 期限到了即使 fn 返回 nil，结果也可能不可信，必须显式失败。
	var classified error
	switch {
	case parent.Err() != nil:
		l.metrics.clientCanceled.Add(1)
		classified = l.fail(KindCanceled, "client canceled during handling: "+parent.Err().Error())
	case errors.Is(context.Cause(ctx), ErrHandleDeadline):
		l.metrics.handleTimeouts.Add(1)
		classified = l.fail(KindDeadlineExceeded, "handle timeout after "+l.cfg.HandleTimeout.String())
	default:
		if runErr != nil {
			l.metrics.internalErrors.Add(1)
		}
		classified = runErr
	}

	// 释放在途槽并唤醒一个等待者；与 gauge 减量在同一临界区内。
	l.releaseSlot()
	return classified
}

// BeginShutdown 把所有泳道切到 draining：新请求立刻收到 KindDraining，
// 已排队/在途的请求不受影响。可重复调用。
func (c *Controller) BeginShutdown() {
	for _, l := range c.lanes {
		l.beginDrain()
	}
}

// Accepting 报告是否仍接受新请求，供 readiness 探针使用。
func (c *Controller) Accepting() bool {
	for _, l := range c.lanes {
		if l.isDraining() {
			return false
		}
	}
	return true
}

// Shutdown 先进入 draining，再等待所有在途与排队请求结束。
//
// ctx 到期仍未排空时返回 ctx.Err()，但不会强杀仍在执行的 goroutine——
// 调用方可以选择继续等待或带着未完成请求退出，语义由调用方决定。
func (c *Controller) Shutdown(ctx context.Context) error {
	c.BeginShutdown()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if c.allIdle() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Controller) allIdle() bool {
	for _, l := range c.lanes {
		if l.inFlight() != 0 || len(l.queue) != 0 {
			return false
		}
	}
	return true
}

// Snapshot 导出全部泳道的可观测指标。
func (c *Controller) Snapshot() Snapshot {
	snap := Snapshot{At: time.Now().UTC(), Accepting: c.Accepting(), Lanes: make([]LaneMetrics, 0, len(c.order))}
	for _, name := range c.order {
		snap.Lanes = append(snap.Lanes, c.lanes[name].metrics.snapshot(name, c.lanes[name].cfg))
	}
	return snap
}

// lane 是单个泳道的内部状态。
type lane struct {
	cfg LaneConfig

	// gate 保护 inFlight 计数与等待者唤醒，保证"入槽/出槽 + gauge"原子。
	gate      sync.Mutex
	cond      *sync.Cond
	inflightC int64
	draining  bool

	// queue 是有界排队位；nil 表示该泳道不允许排队。
	queue chan struct{}
	drain chan struct{}

	// drainState 供无锁的 isDraining 快路径使用。
	drainState atomic.Bool

	metrics laneMetrics
}

func newLane(cfg LaneConfig) *lane {
	l := &lane{
		cfg:     cfg,
		drain:   make(chan struct{}),
		metrics: *newLaneMetrics(),
	}
	l.cond = sync.NewCond(&l.gate)
	if cfg.MaxQueued > 0 {
		l.queue = make(chan struct{}, cfg.MaxQueued)
	}
	return l
}

// acquireSlot 在临界区内等待在途槽。
//
//	block=false：仅非阻塞探测一次，失败返回 KindRejected。
//	block=true ：阻塞直到拿到槽，或被 ctx 取消、排队超时、排空唤醒。
//
// 等待通过一个看门狗 goroutine 在终止事件发生时 Broadcast 实现，
// 因此条件变量的等待不需要、也不能依赖轮询。
func (l *lane) acquireSlot(ctx context.Context, block bool, queueTimeout time.Duration) (bool, Kind) {
	// 非阻塞快路径：无队列泳道，槽满立即拒绝。
	l.gate.Lock()
	if l.inflightC < int64(l.cfg.MaxInFlight) && !l.draining {
		l.inflightC++
		l.metrics.inFlight.Store(l.inflightC)
		l.touchInflightWatermarkLocked()
		l.gate.Unlock()
		return true, ""
	}
	l.gate.Unlock()
	if !block {
		if l.isDraining() {
			return false, KindDraining
		}
		return false, KindRejected
	}

	// 阻塞路径：注册终止看门狗。
	stop := make(chan struct{})
	defer close(stop)
	result := make(chan Kind, 1)

	var timer *time.Timer
	var timeoutC <-chan time.Time
	if queueTimeout > 0 {
		timer = time.NewTimer(queueTimeout)
		timeoutC = timer.C
	}
	go func() {
		select {
		case <-stop:
			if timer != nil {
				timer.Stop()
			}
		case <-ctx.Done():
			result <- KindCanceled
			l.cond.Broadcast()
		case <-timeoutC:
			result <- KindQueueTimeout
			l.cond.Broadcast()
		case <-l.drain:
			result <- KindDraining
			l.cond.Broadcast()
		}
	}()

	l.gate.Lock()
	for l.inflightC >= int64(l.cfg.MaxInFlight) || l.draining {
		if l.draining {
			l.gate.Unlock()
			return false, KindDraining
		}
		// 看门狗可能已经触发；先检查终止信号再睡。
		select {
		case kind := <-result:
			l.gate.Unlock()
			return false, kind
		default:
		}
		l.cond.Wait()
		// 被唤醒后重新评估，并消费可能已到达的终止结果。
		select {
		case kind := <-result:
			l.gate.Unlock()
			return false, kind
		default:
		}
	}
	l.inflightC++
	l.metrics.inFlight.Store(l.inflightC)
	l.touchInflightWatermarkLocked()
	l.gate.Unlock()
	return true, ""
}

func (l *lane) releaseSlot() {
	l.gate.Lock()
	l.inflightC--
	if l.inflightC < 0 {
		l.gate.Unlock()
		panic("admission: in-flight counter went negative")
	}
	l.metrics.inFlight.Store(l.inflightC)
	l.cond.Signal()
	l.gate.Unlock()
}

func (l *lane) inFlight() int64 {
	l.gate.Lock()
	v := l.inflightC
	l.gate.Unlock()
	return v
}

func (l *lane) touchInflightWatermarkLocked() {
	watermark(&l.metrics.inFlightWatermark, l.inflightC)
}

func (l *lane) beginDrain() {
	l.gate.Lock()
	if !l.draining {
		l.draining = true
		l.drainState.Store(true)
		close(l.drain)
		l.cond.Broadcast()
	}
	l.gate.Unlock()
}

func (l *lane) isDraining() bool { return l.drainState.Load() }

func (l *lane) fail(kind Kind, reason string) *Error {
	return &Error{Kind: kind, Lane: l.cfg.Name, Reason: reason}
}

func (l *lane) queuedIn() {
	v := l.metrics.queued.Add(1)
	watermark(&l.metrics.queuedWatermark, v)
}

func (l *lane) queuedOut() { l.metrics.queued.Add(-1) }

// watermark 用 CAS 把 gauge 的历史最大值记录下来。
func watermark(p *atomic.Int64, v int64) {
	for {
		old := p.Load()
		if v <= old || p.CompareAndSwap(old, v) {
			return
		}
	}
}
