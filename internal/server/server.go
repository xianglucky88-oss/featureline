// Package server 把在线查询与后台写入装配成一个带过载保护的 HTTP 服务。
//
// 路由按资源归属分两条准入泳道：
//
//	read  泳道：/v1/features/*（在线特征查询）
//	admin 泳道：/v1/admin/*（后台写入等管理操作）
//
// 探针、注册表、指标等纯内存只读端点不走泳道，避免过载时把可观测性
// 自身也保护没了。所有失败都通过 writeError 输出统一的错误结构，
// 调用方可以按 error.type / retryable 区分主动拒绝、可重试超时与内部故障。
package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/internal-platform/featureline/internal/admission"
	"github.com/internal-platform/featureline/internal/feature"
	"github.com/internal-platform/featureline/internal/online"
	"github.com/internal-platform/featureline/internal/store"
)

// DefaultConfig 返回容量边界的默认值。
//
// read 泳道给足并发但队列很短：宁可快速拒绝也不让请求在服务内堆积；
// admin 泳道只保留少量并发与极短队列，批量写最多占住自己那份资源。
func DefaultConfig() Config {
	return Config{
		Read: admission.LaneConfig{
			Name:          admission.LaneRead,
			MaxInFlight:   64,
			MaxQueued:     128,
			QueueTimeout:  200 * time.Millisecond,
			HandleTimeout: 2 * time.Second,
		},
		Admin: admission.LaneConfig{
			Name:          admission.LaneAdmin,
			MaxInFlight:   4,
			MaxQueued:     16,
			QueueTimeout:  500 * time.Millisecond,
			HandleTimeout: 5 * time.Second,
		},
	}
}

// Config 是服务的过载控制配置。
type Config struct {
	Read  admission.LaneConfig
	Admin admission.LaneConfig
}

// Server 持有路由、准入控制器与业务依赖。
type Server struct {
	mux *http.ServeMux
	adm *admission.Controller
	cfg Config

	svc *online.Service
	st  store.Store
	reg *feature.Registry

	logf func(format string, args ...any)
}

// New 构造服务。配置非法时返回错误，避免带着坏容量上线。
func New(svc *online.Service, st store.Store, reg *feature.Registry, cfg Config) (*Server, error) {
	adm, err := admission.New(cfg.Read, cfg.Admin)
	if err != nil {
		return nil, err
	}
	s := &Server{
		mux:  http.NewServeMux(),
		adm:  adm,
		cfg:  cfg,
		svc:  svc,
		st:   st,
		reg:  reg,
		logf: func(string, ...any) {},
	}
	s.routes()
	return s, nil
}

// WithLogger 注入日志函数（默认静默，main 里接 log.Printf）。
func (s *Server) WithLogger(f func(format string, args ...any)) *Server {
	if f != nil {
		s.logf = f
	}
	return s
}

// Handler 返回带访问日志的根 handler。
func (s *Server) Handler() http.Handler { return s.accessLog(s.mux) }

// Admission 暴露准入控制器，供 main 编排优雅关闭与探针读取。
func (s *Server) Admission() *admission.Controller { return s.adm }

func (s *Server) routes() {
	// ---- 不走泳道的端点：探针 / 只读元数据 / 可观测性 ----
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "features": len(s.reg.Specs)})
	})
	s.mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.adm.Accepting() {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
			return
		}
		writeError(w, http.StatusServiceUnavailable, apiError{
			Type: "server_draining", Message: "server is shutting down", Retryable: true,
		})
	})
	s.mux.HandleFunc("/v1/registry", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"features": s.reg.Specs})
	})
	s.mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.adm.Snapshot())
	})
	s.mux.HandleFunc("/v1/debug", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"store":     s.storeStats(),
			"features":  s.reg.Names(),
			"server_tz": time.Now().Format("MST"),
		})
	})

	// ---- read 泳道：在线特征查询 ----
	s.mux.HandleFunc("/v1/features/", s.guarded(admission.LaneRead, s.handleFeatures))
	// window 查询会进入同一计算路径，归 read 泳道。
	s.mux.HandleFunc("/v1/window", s.guarded(admission.LaneRead, s.handleWindow))

	// ---- admin 泳道：后台写入 ----
	s.mux.HandleFunc("/v1/admin/put", s.guarded(admission.LaneAdmin, s.handleAdminPut))
}

func (s *Server) storeStats() map[string]int {
	type statser interface{ Stats() map[string]int }
	if ss, ok := s.st.(statser); ok {
		return ss.Stats()
	}
	return nil
}

// guarded 把业务 handler 包进指定泳道的准入/超时边界。
//
// 约定：业务 handler 必须先完成全部计算、最后才能写响应；
// 这样一旦 Run 返回控制类错误且尚未写响应，我们还能把正确的
// 失败语义返回给调用方，而不是写出半截 200。
func (s *Server) guarded(lane string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		g, admErr := s.adm.Acquire(r.Context(), lane)
		if admErr != nil {
			status, body, ra := mapAdmissionError(admErr, s.retryAfter(lane))
			setRetryAfter(w, ra)
			writeError(w, status, body)
			s.logf("lane=%s reject kind=%s path=%s detail=%q", lane, admErr.Kind, r.URL.Path, admErr.Reason)
			return
		}

		sw := &statusWriter{ResponseWriter: w}
		// 可观测的准入痕迹：排队多久、走哪条泳道。响应未写出前设置均有效。
		sw.Header().Set("X-Admission-Lane", lane)
		sw.Header().Set("X-Admission-Queued-Ms", formatMillis(g.Wait))

		start := time.Now()
		runErr := g.Run(r.Context(), func(ctx context.Context) error {
			h(sw, r.WithContext(ctx))
			return nil
		})
		elapsed := time.Since(start)

		if runErr != nil {
			if sw.wrote {
				// 响应已提交，无法改写状态码——记录下来供事后排查，
				// 但绝不伪造成功语义或静默吞掉。
				s.logf("lane=%s error after response committed path=%s err=%v", lane, r.URL.Path, runErr)
				return
			}
			var ae *admission.Error
			if errors.As(runErr, &ae) {
				status, body, ra := mapAdmissionError(ae, s.retryAfter(lane))
				setRetryAfter(sw, ra)
				writeError(sw, status, body)
			} else {
				writeError(sw, http.StatusInternalServerError, apiError{
					Type: "internal_error", Message: runErr.Error(), Retryable: false,
				})
			}
		}
		s.logf("lane=%s method=%s path=%s status=%d queued=%s served=%s",
			lane, r.Method, r.URL.Path, sw.status, g.Wait.Round(time.Microsecond), elapsed.Round(time.Microsecond))
	}
}

func (s *Server) retryAfter(lane string) time.Duration {
	switch lane {
	case admission.LaneRead:
		return s.cfg.Read.QueueTimeout
	case admission.LaneAdmin:
		return s.cfg.Admin.QueueTimeout
	default:
		return 0
	}
}

// ---- 业务 handler（计算完成前不写响应）----

func (s *Server) handleFeatures(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Path[len("/v1/features/"):]
	if userID == "" {
		writeError(w, http.StatusBadRequest, apiError{Type: "bad_request", Message: "user_id required"})
		return
	}
	fv, err := s.svc.Compute(r.Context(), userID)
	if err != nil {
		// 请求已被期限/取消终止时不要写 500：失败分类交给 Grant.Run，
		// 保证超时统一是可重试的 504、取消是 499，而不是不可重试的内部故障。
		if r.Context().Err() != nil {
			return
		}
		// 下游（存储等）在期限内主动报错：明确的内部错误，
		// 不填默认值、不伪装成功。
		writeError(w, http.StatusInternalServerError, apiError{
			Type: "internal_error", Message: "feature compute failed: " + err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, fv)
}

func (s *Server) handleWindow(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	asOf := r.URL.Query().Get("as_of")
	t := time.Now().UTC()
	if asOf != "" {
		parsed, err := time.Parse(time.RFC3339, asOf)
		if err != nil {
			writeError(w, http.StatusBadRequest, apiError{Type: "bad_request", Message: "invalid as_of: " + err.Error()})
			return
		}
		t = parsed
	}
	start, end, err := s.svc.WindowFor(name, t)
	if err != nil {
		writeError(w, http.StatusBadRequest, apiError{Type: "bad_request", Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"name":         name,
		"window_start": start.Format(time.RFC3339),
		"window_end":   end.Format(time.RFC3339),
		"convention":   "[start, end) — 左闭右开",
	})
}

func (s *Server) handleAdminPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, apiError{Type: "method_not_allowed", Message: "POST only"})
		return
	}
	var body struct {
		UserID string  `json:"user_id"`
		Name   string  `json:"name"`
		Score  float64 `json:"score"`
		Source string  `json:"source"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, apiError{Type: "bad_request", Message: err.Error()})
		return
	}
	if _, ok := s.reg.Specs[body.Name]; !ok {
		writeError(w, http.StatusBadRequest, apiError{Type: "bad_request", Message: "unknown feature " + body.Name})
		return
	}
	if body.Source == "" {
		body.Source = "online"
	}
	if err := s.st.Set(r.Context(), body.UserID, body.Name, store.Value{
		Score: body.Score, UpdatedAt: time.Now().UTC(), Source: body.Source,
	}); err != nil {
		// 同读路径：context 终止的分类权交给准入层。
		if r.Context().Err() != nil {
			return
		}
		writeError(w, http.StatusInternalServerError, apiError{
			Type: "internal_error", Message: "store set failed: " + err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// ---- 统一错误结构与状态码映射 ----

// apiError 是返回给调用方的统一失败结构。
// retryable 让客户端无需猜测状态码语义即可决定重试策略。
type apiError struct {
	Type      string `json:"error"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func mapAdmissionError(e *admission.Error, retryAfter time.Duration) (int, apiError, time.Duration) {
	switch e.Kind {
	case admission.KindRejected:
		return http.StatusServiceUnavailable, apiError{
			Type: "overload_rejected", Message: e.Reason, Retryable: true,
		}, retryAfter
	case admission.KindDraining:
		return http.StatusServiceUnavailable, apiError{
			Type: "server_draining", Message: e.Reason, Retryable: true,
		}, 5 * time.Second
	case admission.KindQueueTimeout:
		return http.StatusGatewayTimeout, apiError{
			Type: "queue_timeout", Message: e.Reason, Retryable: true,
		}, retryAfter
	case admission.KindDeadlineExceeded:
		// 处理超时意味着结果是否生效未知：显式告知可安全重试，
		// 而不是让调用方对着一个普通 500 无法判断。
		return http.StatusGatewayTimeout, apiError{
			Type: "deadline_exceeded", Message: e.Reason, Retryable: true,
		}, retryAfter
	case admission.KindCanceled:
		// 499 是非标准但被广泛识别的"客户端关闭连接"语义。
		return 499, apiError{Type: "client_canceled", Message: e.Reason}, 0
	default:
		return http.StatusInternalServerError, apiError{
			Type: "internal_error", Message: e.Reason,
		}, 0
	}
}

func writeError(w http.ResponseWriter, status int, body apiError) {
	writeJSON(w, status, body)
}

// setRetryAfter 在可重试的拒绝/超时响应上附带退避建议（秒，向上取整）。
func setRetryAfter(w http.ResponseWriter, d time.Duration) {
	if d <= 0 {
		return
	}
	secs := int64(d.Round(time.Second).Seconds())
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// statusWriter 记录最终状态码并跟踪响应是否已提交，
// 供访问日志与"错误发生在写响应之后"的分支判断使用。
type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusWriter) WriteHeader(code int) {
	if !s.wrote {
		s.status = code
		s.wrote = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if !s.wrote {
		s.status = http.StatusOK
		s.wrote = true
	}
	return s.ResponseWriter.Write(b)
}

func formatMillis(d time.Duration) string {
	ms := d.Milliseconds()
	if ms == 0 && d > 0 {
		ms = 1
	}
	return strconv.FormatInt(ms, 10)
}

// accessLog 给未被 guarded 覆盖的端点也补上统一格式的访问日志。
func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		if r.URL.Path == "/healthz" {
			return
		}
		s.logf("method=%s path=%s status=%d dur=%s",
			r.Method, r.URL.Path, sw.status, time.Since(start).Round(time.Microsecond))
	})
}
