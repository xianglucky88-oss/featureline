package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestGracefulShutdownCompletesInFlight 用真实 HTTP 连接验证关闭语义：
// 进入 draining 后新请求被拒，但已经在途的请求不被终止，能拿到完整结果。
func TestGracefulShutdownCompletesInFlight(t *testing.T) {
	st := newControllableStore()
	release := st.block("user_7d_order_cnt")
	srv := newTestServer(t, st)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	inFlight := make(chan struct{})
	go func() {
		resp, err := http.Get(ts.URL + "/v1/features/U1")
		if err != nil {
			t.Errorf("in-flight request failed: %v", err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("in-flight request status = %d body=%s", resp.StatusCode, body)
			return
		}
		close(inFlight)
	}()

	waitForInflight(t, srv, "read", 1)

	// 关闭准入：新请求立即收到 draining，而不是挂起或 500。
	srv.adm.BeginShutdown()
	resp, err := http.Get(ts.URL + "/v1/features/U2")
	if err != nil {
		t.Fatalf("request during drain: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("want 503 during drain, got %d", resp.StatusCode)
	}

	// 放行在途请求；它必须正常完成，证明关闭没有强杀在途工作。
	close(release)
	select {
	case <-inFlight:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight request did not complete during shutdown")
	}

	// 排空后 Shutdown 应立即返回 nil。
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.adm.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown after drain: %v", err)
	}
}
