package admission

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestInFlightNeverExceedsCapUnderExtremeLoad 用大规模并发 + 随机取消，
// 验证在途计数在任何时刻都不超过 MaxInFlight（这是舱壁隔离的核心承诺）。
func TestInFlightNeverExceedsCapUnderExtremeLoad(t *testing.T) {
	cfg := LaneConfig{
		Name: "stress", MaxInFlight: 8, MaxQueued: 32,
		QueueTimeout: 20 * time.Millisecond, HandleTimeout: 30 * time.Millisecond,
	}
	c := newTestController(t, cfg)

	var violated atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			g, err := c.Acquire(ctx, "stress")
			if err != nil {
				return // 被拒绝/超时/取消，均为合法结果
			}
			// 拿到槽后立即检查全局在途量是否越界。
			if c.Snapshot().Lanes[0].InFlight > int64(cfg.MaxInFlight) {
				violated.Add(1)
			}
			_ = g.Run(ctx, func(ctx context.Context) error {
				select {
				case <-time.After(time.Duration(i%5) * time.Millisecond):
				case <-ctx.Done():
				}
				return nil
			})
		}(i)
	}
	wg.Wait()

	if v := violated.Load(); v != 0 {
		t.Fatalf("in-flight cap violated %d times", v)
	}
	snap := c.Snapshot().Lanes[0]
	if snap.InFlightWatermark > int64(cfg.MaxInFlight) {
		t.Fatalf("watermark %d exceeds cap %d", snap.InFlightWatermark, cfg.MaxInFlight)
	}
	if snap.InFlight != 0 {
		t.Fatalf("lane not empty: in_flight=%d", snap.InFlight)
	}
	// 所有提交的请求都必须有明确归宿：接受 + 各类拒绝 = 提交总数。
	accounted := snap.Admitted + snap.RejectedQueueFull + snap.QueueTimeouts +
		snap.HandleTimeouts + snap.ClientCanceled
	if accounted != snap.Submitted {
		t.Fatalf("accounting mismatch: submitted=%d accounted=%d", snap.Submitted, accounted)
	}
}
