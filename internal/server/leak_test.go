package server

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"
)

// TestNoGoroutineLeakUnderChurn 验证在大量请求（含拒绝、排队超时、
// 处理超时、客户端取消）后，准入层的看门狗与等待 goroutine 都被回收，
// 不会随着流量累积泄漏。
func TestNoGoroutineLeakUnderChurn(t *testing.T) {
	st := newControllableStore()
	// 用一个常驻阻塞点制造持续的处理超时/排队压力。
	release := st.block("user_7d_order_cnt")
	defer close(release)
	srv := newTestServer(t, st)

	// 预热，让相关 goroutine 路径都走过一遍。
	for i := 0; i < 50; i++ {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/Uwarm", nil))
	}

	// 给被取消/超时的看门狗留出退出时间，再取基线。
	settle()
	baseline := runtime.NumGoroutine()

	// 高并发冲击：多数会被拒绝或排队/处理超时。
	for round := 0; round < 5; round++ {
		done := make(chan struct{}, 40)
		for i := 0; i < 40; i++ {
			go func() {
				defer func() { done <- struct{}{} }()
				rec := httptest.NewRecorder()
				srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/Ux", nil))
			}()
		}
		for i := 0; i < 40; i++ {
			<-done
		}
	}

	settle()
	after := runtime.NumGoroutine()
	growth := after - baseline
	// 允许少量与本测试无关的后台 goroutine 抖动，但不允许线性泄漏。
	if growth > 10 {
		t.Fatalf("goroutine leak suspected: baseline=%d after churn=%d (growth=%d)", baseline, after, growth)
	}
}

func settle() {
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
}
