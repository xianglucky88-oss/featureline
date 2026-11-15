package admission

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func testCfg() LaneConfig {
	return LaneConfig{
		Name:          "t",
		MaxInFlight:   2,
		MaxQueued:     2,
		QueueTimeout:  200 * time.Millisecond,
		HandleTimeout: 200 * time.Millisecond,
	}
}

func newTestController(t *testing.T, cfg LaneConfig) *Controller {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestConfigValidation(t *testing.T) {
	bad := []LaneConfig{
		{Name: "x", MaxInFlight: 0},
		{Name: "x", MaxInFlight: 1, MaxQueued: -1},
		{Name: "x", MaxInFlight: 1, QueueTimeout: -1},
		{Name: "x", MaxInFlight: 1, HandleTimeout: -1},
		{Name: ""},
	}
	for i, cfg := range bad {
		if _, err := New(cfg); err == nil {
			t.Fatalf("case %d: expected validation error", i)
		}
	}
	if _, err := New(
		LaneConfig{Name: "a", MaxInFlight: 1},
		LaneConfig{Name: "a", MaxInFlight: 1},
	); err == nil {
		t.Fatal("duplicate lane must be rejected")
	}
}

func TestQueueFullRejectsImmediately(t *testing.T) {
	cfg := testCfg()
	cfg.MaxInFlight = 1
	cfg.MaxQueued = 0
	c := newTestController(t, cfg)

	hold, err := c.Acquire(context.Background(), "t")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	start := time.Now()
	_, admErr := c.Acquire(context.Background(), "t")
	waited := time.Since(start)
	if admErr == nil || admErr.Kind != KindRejected {
		t.Fatalf("want rejected, got %v", admErr)
	}
	// 主动拒绝必须是即时的，不能在满队列上阻塞调用方。
	if waited > 20*time.Millisecond {
		t.Fatalf("rejection took %s, must be immediate", waited)
	}

	if err := hold.Run(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("run: %v", err)
	}
	snap := c.Snapshot().Lanes[0]
	if snap.RejectedQueueFull != 1 {
		t.Fatalf("rejected counter = %d, want 1", snap.RejectedQueueFull)
	}
}

func TestQueueTimeoutClassification(t *testing.T) {
	cfg := testCfg()
	cfg.MaxInFlight = 1
	cfg.MaxQueued = 1
	cfg.QueueTimeout = 40 * time.Millisecond
	c := newTestController(t, cfg)

	hold, _ := c.Acquire(context.Background(), "t")

	start := time.Now()
	_, admErr := c.Acquire(context.Background(), "t")
	if admErr == nil || admErr.Kind != KindQueueTimeout {
		t.Fatalf("want queue_timeout, got %v", admErr)
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("queue timeout returned earlier than configured")
	}
	if c.Snapshot().Lanes[0].QueueTimeouts != 1 {
		t.Fatal("queue timeout metric not recorded")
	}

	// 超时后排队位必须归还：再入队一个请求，释放槽后它应能拿到槽并完成。
	done := make(chan error, 1)
	go func() {
		g, e := c.Acquire(context.Background(), "t")
		if e != nil {
			done <- e
			return
		}
		done <- g.Run(context.Background(), func(context.Context) error { return nil })
	}()
	// 确认 goroutine 已进入等待，而不是直接被拒。
	time.Sleep(20 * time.Millisecond)
	if err := hold.Run(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("release run: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("queued request after timeout: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("queued request did not complete after slot release")
	}
}

func TestQueuedRequestAdmittedAfterRelease(t *testing.T) {
	cfg := testCfg()
	cfg.MaxInFlight = 1
	cfg.MaxQueued = 1
	cfg.QueueTimeout = time.Second
	c := newTestController(t, cfg)

	hold, _ := c.Acquire(context.Background(), "t")
	got := make(chan *Grant, 1)
	go func() {
		g, e := c.Acquire(context.Background(), "t")
		if e != nil {
			t.Errorf("queued acquire: %v", e)
			return
		}
		got <- g
	}()
	time.Sleep(20 * time.Millisecond)
	if err := hold.Run(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("run: %v", err)
	}
	select {
	case g := <-got:
		if err := g.Run(context.Background(), func(context.Context) error { return nil }); err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("queued request was not admitted after slot release")
	}
}

func TestHandleTimeoutWinsEvenWhenFnReturnsNil(t *testing.T) {
	cfg := testCfg()
	cfg.HandleTimeout = 40 * time.Millisecond
	c := newTestController(t, cfg)

	g, admErr := c.Acquire(context.Background(), "t")
	if admErr != nil {
		t.Fatalf("acquire: %v", admErr)
	}
	// fn 无视 ctx，睡过期限后返回 nil——结果仍必须按处理超时失败，
	// 不允许把一个"期限外完成"的结果当成成功交给调用方。
	runErr := g.Run(context.Background(), func(ctx context.Context) error {
		select {
		case <-time.After(150 * time.Millisecond):
		case <-ctx.Done():
		}
		return nil
	})
	var ae *Error
	if !errors.As(runErr, &ae) || ae.Kind != KindDeadlineExceeded {
		t.Fatalf("want deadline_exceeded, got %v", runErr)
	}
	if c.Snapshot().Lanes[0].HandleTimeouts != 1 {
		t.Fatal("handle timeout metric not recorded")
	}
}

func TestHandleTimeoutPropagatesContext(t *testing.T) {
	cfg := testCfg()
	cfg.HandleTimeout = 40 * time.Millisecond
	c := newTestController(t, cfg)

	g, _ := c.Acquire(context.Background(), "t")
	observed := make(chan error, 1)
	runErr := g.Run(context.Background(), func(ctx context.Context) error {
		<-ctx.Done()
		observed <- ctx.Err()
		return ctx.Err()
	})
	var ae *Error
	if !errors.As(runErr, &ae) || ae.Kind != KindDeadlineExceeded {
		t.Fatalf("want deadline_exceeded, got %v", runErr)
	}
	select {
	case got := <-observed:
		if !errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("fn ctx err = %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("fn never observed context cancellation")
	}
}

func TestClientCancelWhileQueued(t *testing.T) {
	cfg := testCfg()
	cfg.MaxInFlight = 1
	cfg.MaxQueued = 1
	cfg.QueueTimeout = time.Second
	c := newTestController(t, cfg)

	hold, _ := c.Acquire(context.Background(), "t")

	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan *Error, 1)
	go func() {
		_, e := c.Acquire(ctx, "t")
		res <- e
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case e := <-res:
		if e == nil || e.Kind != KindCanceled {
			t.Fatalf("want canceled, got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not unblock queued acquire")
	}

	// 取消后释放原先占住的槽，泳道应能立即服务新请求（验证排队位已归还）。
	if err := hold.Run(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("release held slot: %v", err)
	}
	g, e := c.Acquire(context.Background(), "t")
	if e != nil {
		t.Fatalf("lane unusable after cancel: %v", e)
	}
	if err := g.Run(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestClientCancelDuringHandling(t *testing.T) {
	cfg := testCfg()
	c := newTestController(t, cfg)

	g, _ := c.Acquire(context.Background(), "t")
	parent, cancel := context.WithCancel(context.Background())
	fnSaw := make(chan struct{})
	go func() {
		cancel()
	}()
	runErr := g.Run(parent, func(ctx context.Context) error {
		<-ctx.Done()
		close(fnSaw)
		return ctx.Err()
	})
	var ae *Error
	if !errors.As(runErr, &ae) || ae.Kind != KindCanceled {
		t.Fatalf("want client_canceled, got %v", runErr)
	}
	<-fnSaw
	if c.Snapshot().Lanes[0].ClientCanceled < 1 {
		t.Fatal("cancel metric not recorded")
	}
}

func TestAlreadyCanceledContextRejected(t *testing.T) {
	c := newTestController(t, testCfg())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e := c.Acquire(ctx, "t")
	if e == nil || e.Kind != KindCanceled {
		t.Fatalf("want canceled, got %v", e)
	}
}

func TestBulkheadLanesAreIndependent(t *testing.T) {
	read := LaneConfig{
		Name: "read", MaxInFlight: 4, MaxQueued: 4,
		QueueTimeout: 100 * time.Millisecond, HandleTimeout: time.Second,
	}
	admin := LaneConfig{
		Name: "admin", MaxInFlight: 1, MaxQueued: 0,
		QueueTimeout: 50 * time.Millisecond, HandleTimeout: time.Second,
	}
	c, err := New(read, admin)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 打满 admin 泳道唯一的槽，并让它一直占着。
	adminGrant, e := c.Acquire(context.Background(), "admin")
	if e != nil {
		t.Fatalf("admin acquire: %v", e)
	}
	release := make(chan struct{})
	go func() {
		_ = adminGrant.Run(context.Background(), func(ctx context.Context) error {
			<-release
			return nil
		})
	}()
	// admin 现在应主动拒绝。
	if _, e := c.Acquire(context.Background(), "admin"); e == nil || e.Kind != KindRejected {
		t.Fatalf("admin lane should be saturated, got %v", e)
	}

	// read 泳道完全不受影响：并发打满自己的槽位也全部成功。
	var wg sync.WaitGroup
	for i := 0; i < read.MaxInFlight; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, err := c.Acquire(context.Background(), "read")
			if err != nil {
				t.Errorf("read acquire while admin saturated: %v", err)
				return
			}
			if err := g.Run(context.Background(), func(context.Context) error { return nil }); err != nil {
				t.Errorf("read run: %v", err)
			}
		}()
	}
	wg.Wait()
	close(release)
}

func TestDrainingRejectsNewButCompletesInFlight(t *testing.T) {
	c := newTestController(t, testCfg())

	g, e := c.Acquire(context.Background(), "t")
	if e != nil {
		t.Fatalf("acquire: %v", e)
	}
	c.BeginShutdown()
	if c.Accepting() {
		t.Fatal("controller should report not accepting")
	}
	if _, e := c.Acquire(context.Background(), "t"); e == nil || e.Kind != KindDraining {
		t.Fatalf("want draining, got %v", e)
	}
	// 重复 BeginShutdown 不应 panic。
	c.BeginShutdown()

	// 已接受的请求照常完成，不被终止。
	ran := false
	if err := g.Run(context.Background(), func(context.Context) error {
		ran = true
		return nil
	}); err != nil {
		t.Fatalf("in-flight run during draining: %v", err)
	}
	if !ran {
		t.Fatal("in-flight request was not executed")
	}
}

func TestDrainWhileQueued(t *testing.T) {
	cfg := testCfg()
	cfg.MaxInFlight = 1
	cfg.MaxQueued = 1
	c := newTestController(t, cfg)

	hold, _ := c.Acquire(context.Background(), "t")
	res := make(chan *Error, 1)
	go func() {
		_, e := c.Acquire(context.Background(), "t")
		res <- e
	}()
	time.Sleep(20 * time.Millisecond)
	c.BeginShutdown()
	select {
	case e := <-res:
		if e == nil || e.Kind != KindDraining {
			t.Fatalf("want draining for queued request, got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("queued request not unblocked by drain")
	}
	_ = hold.Run(context.Background(), func(context.Context) error { return nil })
}

func TestShutdownWaitsForInFlight(t *testing.T) {
	cfg := testCfg()
	cfg.HandleTimeout = 0 // 本用例只验证关闭等待，期限由 Shutdown ctx 控制
	c := newTestController(t, cfg)
	g, _ := c.Acquire(context.Background(), "t")

	finished := make(chan struct{})
	go func() {
		_ = g.Run(context.Background(), func(ctx context.Context) error {
			time.Sleep(40 * time.Millisecond)
			return nil
		})
		close(finished)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	// Shutdown 以在途计数归零为准；goroutine 回到 Run 调用点可能略晚，
	// 因此这里用带超时等待而不是非阻塞探测。
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("in-flight goroutine did not return after Shutdown")
	}
}

func TestShutdownTimeoutDoesNotKillWork(t *testing.T) {
	cfg := testCfg()
	cfg.HandleTimeout = 0 // 关闭等待不应强杀业务
	c := newTestController(t, cfg)
	g, _ := c.Acquire(context.Background(), "t")

	keptRunning := make(chan struct{})
	go func() {
		_ = g.Run(context.Background(), func(ctx context.Context) error {
			select {
			case <-time.After(300 * time.Millisecond):
				close(keptRunning)
			case <-ctx.Done():
				t.Error("work context was canceled by shutdown")
			}
			return nil
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	select {
	case <-keptRunning:
	case <-time.After(time.Second):
		t.Fatal("in-flight work was killed by shutdown")
	}
}

func TestMetricsCountersAndWatermarks(t *testing.T) {
	cfg := testCfg()
	cfg.MaxInFlight = 1
	cfg.MaxQueued = 2
	c := newTestController(t, cfg)

	g1, _ := c.Acquire(context.Background(), "t")
	// 两个排队 + 一个被拒。
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, e := c.Acquire(context.Background(), "t")
			if e == nil {
				_ = g.Run(context.Background(), func(context.Context) error { return nil })
			}
		}()
	}
	// 先确认唯一在途槽与两个排队位都被占住，再做拒绝断言，
	// 避免依赖固定 sleep 造成的时序竞争。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s := c.Snapshot().Lanes[0]
		if s.InFlight == 1 && s.Queued == 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if s := c.Snapshot().Lanes[0]; s.InFlight != 1 || s.Queued != 2 {
		t.Fatalf("lane not saturated as expected: inflight=%d queued=%d", s.InFlight, s.Queued)
	}
	if _, e := c.Acquire(context.Background(), "t"); e == nil || e.Kind != KindRejected {
		t.Fatalf("want rejection, got %v", e)
	}
	if err := g1.Run(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("run: %v", err)
	}
	wg.Wait()

	snap := c.Snapshot().Lanes[0]
	if snap.Submitted != 4 {
		t.Fatalf("submitted = %d, want 4", snap.Submitted)
	}
	if snap.Admitted != 3 || snap.Completed != 3 {
		t.Fatalf("admitted=%d completed=%d, want 3/3", snap.Admitted, snap.Completed)
	}
	if snap.RejectedQueueFull != 1 {
		t.Fatalf("rejected = %d, want 1", snap.RejectedQueueFull)
	}
	if snap.QueuedWatermark != 2 {
		t.Fatalf("queue watermark = %d, want 2", snap.QueuedWatermark)
	}
	if snap.InFlightWatermark != 1 {
		t.Fatalf("in-flight watermark = %d, want 1", snap.InFlightWatermark)
	}
	if snap.InFlight != 0 || snap.Queued != 0 {
		t.Fatal("lane not empty after all requests finished")
	}
	if snap.QueueWaitHistogram.Observations != 3 {
		t.Fatalf("queue wait observations = %d, want 3", snap.QueueWaitHistogram.Observations)
	}
}

func TestErrorString(t *testing.T) {
	e := &Error{Kind: KindRejected, Lane: "read", Reason: "queue is full"}
	if !strings.Contains(e.Error(), "rejected") || !strings.Contains(e.Error(), "read") {
		t.Fatalf("unexpected error text: %s", e.Error())
	}
}

func TestUnknownLane(t *testing.T) {
	c := newTestController(t, testCfg())
	if _, e := c.Acquire(context.Background(), "nope"); e == nil || e.Kind != KindRejected {
		t.Fatalf("want rejection for unknown lane, got %v", e)
	}
}
