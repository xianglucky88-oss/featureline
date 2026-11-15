package admission

import (
	"context"
	"testing"
	"time"
)

// TestZeroTimeoutsMeanNoDeadline 验证超时取 0 时表示"不设该层超时"，
// 而不是立即超时：排队者能一直等到槽释放，处理函数不被强加期限。
func TestZeroTimeoutsMeanNoDeadline(t *testing.T) {
	cfg := LaneConfig{
		Name: "nz", MaxInFlight: 1, MaxQueued: 1,
		QueueTimeout:  0, // 不设排队超时
		HandleTimeout: 0, // 不设处理超时
	}
	c := newTestController(t, cfg)

	hold, err := c.Acquire(context.Background(), "nz")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	got := make(chan *Grant, 1)
	go func() {
		g, e := c.Acquire(context.Background(), "nz")
		if e != nil {
			t.Errorf("queued acquire with no queue timeout: %v", e)
			return
		}
		got <- g
	}()
	// 明显超过任何默认排队超时的时长，验证零值确实意味着"无限等待槽"。
	time.Sleep(80 * time.Millisecond)

	if err := hold.Run(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("run: %v", err)
	}
	select {
	case g := <-got:
		// 无处理期限：慢函数只要正常结束就应成功，不会被改判为 deadline_exceeded。
		runErr := g.Run(context.Background(), func(ctx context.Context) error {
			select {
			case <-time.After(50 * time.Millisecond):
				return nil
			case <-ctx.Done():
				t.Error("context was canceled despite zero handle timeout")
				return ctx.Err()
			}
		})
		if runErr != nil {
			t.Fatalf("run with no handle timeout: %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("queued request not admitted after slot release")
	}
}
