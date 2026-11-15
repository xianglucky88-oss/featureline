package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/internal-platform/featureline/internal/admission"
	"github.com/internal-platform/featureline/internal/feature"
	"github.com/internal-platform/featureline/internal/online"
	"github.com/internal-platform/featureline/internal/store"
)

// controllableStore 包装内存存储，可按名字阻塞读/写、注入故障，
// 用于模拟慢依赖、部分依赖变慢与存储故障。
type controllableStore struct {
	inner *store.MemoryStore

	mu       sync.Mutex
	blocking map[string]chan struct{} // 按特征名阻塞 MGet
	blockSet map[string]chan struct{} // 按特征名阻塞 Set
	failNext bool
	failMGet bool
}

func newControllableStore() *controllableStore {
	return &controllableStore{
		inner:    store.NewMemory(""),
		blocking: map[string]chan struct{}{},
		blockSet: map[string]chan struct{}{},
	}
}

func (c *controllableStore) block(name string) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan struct{})
	c.blocking[name] = ch
	return ch
}

func (c *controllableStore) holdSet(name string) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan struct{})
	c.blockSet[name] = ch
	return ch
}

func (c *controllableStore) Get(ctx context.Context, userID, name string) (store.Value, bool, error) {
	return c.inner.Get(ctx, userID, name)
}

func (c *controllableStore) Set(ctx context.Context, userID, name string, v store.Value) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	wait := c.blockSet[name]
	fail := c.failNext
	c.failNext = false
	c.mu.Unlock()
	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if fail {
		return errors.New("simulated disk failure")
	}
	return c.inner.Set(ctx, userID, name, v)
}

func (c *controllableStore) MGet(ctx context.Context, userID string, names []string) (map[string]store.Value, error) {
	c.mu.Lock()
	var waits []chan struct{}
	for _, n := range names {
		if ch, ok := c.blocking[n]; ok {
			waits = append(waits, ch)
		}
	}
	c.mu.Unlock()
	// 慢依赖：要么等到放行，要么被请求 context 打断，不能无视取消。
	for _, ch := range waits {
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c.mu.Lock()
	fail := c.failMGet
	c.mu.Unlock()
	if fail {
		return nil, errors.New("simulated replica failure")
	}
	return c.inner.MGet(ctx, userID, names)
}

func (c *controllableStore) Keys(ctx context.Context) ([]string, error) {
	return c.inner.Keys(ctx)
}

func (c *controllableStore) Close() error { return c.inner.Close() }

func (c *controllableStore) Stats() map[string]int { return c.inner.Stats() }

func testConfig() Config {
	return Config{
		Read: admission.LaneConfig{
			Name: "read", MaxInFlight: 1, MaxQueued: 1,
			QueueTimeout: 60 * time.Millisecond, HandleTimeout: 120 * time.Millisecond,
		},
		Admin: admission.LaneConfig{
			Name: "admin", MaxInFlight: 1, MaxQueued: 0,
			QueueTimeout: 60 * time.Millisecond, HandleTimeout: time.Second,
		},
	}
}

func newTestServer(t *testing.T, st store.Store) *Server {
	t.Helper()
	reg := feature.DefaultRegistry()
	srv, err := New(online.New(st, reg), st, reg, testConfig())
	if err != nil {
		t.Fatalf("New server: %v", err)
	}
	return srv
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	return body
}

func TestHappyPathAndAdmissionHeaders(t *testing.T) {
	srv := newTestServer(t, newControllableStore())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/features/U1", nil)
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Admission-Lane"); got != "read" {
		t.Fatalf("lane header = %q", got)
	}
	if rec.Header().Get("X-Admission-Queued-Ms") == "" {
		t.Fatal("queue timing header missing")
	}
	body := decodeBody(t, rec)
	if body["user_id"] != "U1" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestOverloadRejectedWhenSaturated(t *testing.T) {
	st := newControllableStore()
	release := st.block("user_7d_order_cnt")
	srv := newTestServer(t, st)

	firstDone := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U1", nil))
		close(firstDone)
	}()

	// 等第一个请求占住唯一在途槽。
	waitFor := func(cond func() bool) {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("condition not met in time")
	}
	waitFor(func() bool { return srv.adm.Snapshot().Lanes[0].InFlight == 1 })

	// 第二个请求占住唯一排队位，第三个应被主动拒绝（不是超时、不是 500）。
	queuedDone := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U2", nil))
		close(queuedDone)
	}()
	waitFor(func() bool { return srv.adm.Snapshot().Lanes[0].Queued == 1 })

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U3", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["error"] != "overload_rejected" || body["retryable"] != true {
		t.Fatalf("want retryable overload_rejected, got %v", body)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("Retry-After missing on rejection")
	}

	close(release)
	<-firstDone
	<-queuedDone
}

func TestQueueTimeoutClassification(t *testing.T) {
	st := newControllableStore()
	release := st.block("user_7d_order_cnt")
	srv := newTestServer(t, st)

	occupy := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U1", nil))
		close(occupy)
	}()
	time.Sleep(30 * time.Millisecond)

	// MaxQueued=1：这个请求排队，60ms 内拿不到槽 -> queue_timeout。
	rec := httptest.NewRecorder()
	start := time.Now()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U2", nil))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("want 504, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["error"] != "queue_timeout" || body["retryable"] != true {
		t.Fatalf("want retryable queue_timeout, got %v", body)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("queue timeout fired earlier than configured")
	}

	close(release)
	<-occupy
}

func TestHandleTimeoutFromSlowDependency(t *testing.T) {
	st := newControllableStore()
	release := st.block("user_7d_order_cnt")
	srv := newTestServer(t, st)

	rec := httptest.NewRecorder()
	start := time.Now()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U1", nil))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("want 504, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["error"] != "deadline_exceeded" || body["retryable"] != true {
		t.Fatalf("want retryable deadline_exceeded, got %v", body)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("handle timeout fired earlier than configured deadline")
	}
	// 超时后在途槽必须释放：放行慢依赖后，后续请求能正常成功。
	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U2", nil))
		if rec.Code == http.StatusOK {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("lane did not recover after handle timeout")
}

func TestInternalErrorNotMarkedRetryable(t *testing.T) {
	st := newControllableStore()
	st.failNext = true
	srv := newTestServer(t, st)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/put",
		strings.NewReader(`{"user_id":"U1","name":"user_7d_order_cnt","score":1}`))
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["error"] != "internal_error" || body["retryable"] != false {
		t.Fatalf("want non-retryable internal_error, got %v", body)
	}
}

func TestReadDependencyFailureIsInternalError(t *testing.T) {
	st := newControllableStore()
	st.failMGet = true
	srv := newTestServer(t, st)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U1", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["error"] != "internal_error" || body["retryable"] != false {
		t.Fatalf("want non-retryable internal_error, got %v", body)
	}
}

func TestAdminLaneIsolatedFromRead(t *testing.T) {
	st := newControllableStore()
	readRelease := st.block("user_7d_order_cnt")
	adminRelease := st.holdSet("user_7d_order_cnt")
	srv := newTestServer(t, st)

	// 打满 read 泳道（唯一槽 + 阻塞中的存储调用）。
	readDone := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U1", nil))
		close(readDone)
	}()
	waitForInflight(t, srv, admission.LaneRead, 1)

	// read 被占死时，admin 写入仍能在自己的泳道内被接受并执行。
	adminDone := make(chan struct{})
	var adminCode int
	go func() {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/put",
			strings.NewReader(`{"user_id":"U9","name":"user_7d_order_cnt","score":7}`)))
		adminCode = rec.Code
		close(adminDone)
	}()
	waitForInflight(t, srv, admission.LaneAdmin, 1)

	// admin 是无队列泳道（MaxQueued=0）：槽满后再来的写请求必须被它
	// 自己的泳道立即拒绝，而不是排队或影响 read。
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/admin/put",
		strings.NewReader(`{"user_id":"U9","name":"user_7d_order_cnt","score":8}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 from saturated admin lane, got %d body=%s", rec.Code, rec.Body.String())
	}
	if decodeBody(t, rec)["error"] != "overload_rejected" {
		t.Fatalf("want overload_rejected, got %s", rec.Body.String())
	}

	close(adminRelease)
	<-adminDone
	if adminCode != http.StatusOK {
		t.Fatalf("admin write failed: %d", adminCode)
	}
	close(readRelease)
	<-readDone
}

func waitForInflight(t *testing.T, srv *Server, lane string, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, lm := range srv.adm.Snapshot().Lanes {
			if lm.Lane == lane && lm.InFlight == want {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("lane %q did not reach in_flight=%d", lane, want)
}

func TestDrainingRejectsNewRequestsAndServesReadyz(t *testing.T) {
	srv := newTestServer(t, newControllableStore())

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz before drain = %d", rec.Code)
	}

	srv.adm.BeginShutdown()

	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz after drain = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U1", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 while draining, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["error"] != "server_draining" {
		t.Fatalf("want server_draining, got %v", body)
	}

	// 可观测端点在排空中仍然可用。
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics during drain = %d", rec.Code)
	}
}

func TestClientCancellationIsClassified(t *testing.T) {
	st := newControllableStore()
	release := st.block("user_7d_order_cnt")
	srv := newTestServer(t, st)

	// 用真实 httptest 服务，才能通过关闭请求体模拟客户端断开。
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v1/features/U1", nil)
	resCh := make(chan int, 1)
	go func() {
		resp, err := ts.Client().Do(req)
		if err != nil {
			resCh <- 0
			return
		}
		resCh <- resp.StatusCode
		_ = resp.Body.Close()
	}()

	// 等请求进入在途处理后取消。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && srv.adm.Snapshot().Lanes[0].InFlight == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case code := <-resCh:
		// 客户端断开后，transport 通常返回错误（code=0）；关键验证是
		// 取消被分类计数，且槽位最终释放。
		_ = code
	case <-time.After(time.Second):
		t.Fatal("request did not return after cancellation")
	}

	waitFor := func(cond func() bool) {
		dl := time.Now().Add(time.Second)
		for time.Now().Before(dl) {
			if cond() {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("condition not met in time")
	}
	waitFor(func() bool { return srv.adm.Snapshot().Lanes[0].InFlight == 0 })
	if got := srv.adm.Snapshot().Lanes[0].ClientCanceled; got == 0 {
		t.Fatalf("client_canceled counter = %d, want >= 1", got)
	}
	close(release)
}

func TestMetricsEndpointExposesCounters(t *testing.T) {
	srv := newTestServer(t, newControllableStore())
	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/U1", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: %d", i, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	var snap struct {
		Accepting bool `json:"accepting"`
		Lanes     []struct {
			Lane      string `json:"lane"`
			Submitted int64  `json:"submitted"`
			Admitted  int64  `json:"admitted"`
		} `json:"lanes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	if !snap.Accepting || len(snap.Lanes) != 2 {
		t.Fatalf("unexpected metrics snapshot: %+v", snap)
	}
}

func TestBadRequestDoesNotConsumeErrorSemantics(t *testing.T) {
	srv := newTestServer(t, newControllableStore())
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/features/", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestMethodNotAllowedClassification(t *testing.T) {
	srv := newTestServer(t, newControllableStore())
	rec := httptest.NewRecorder()
	// 用 GET 打只接受 POST 的管理端点。
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/admin/put", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["error"] != "method_not_allowed" || body["retryable"] != false {
		t.Fatalf("want non-retryable method_not_allowed, got %v", body)
	}
	if rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("Allow header = %q, want POST", rec.Header().Get("Allow"))
	}
}
