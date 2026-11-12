package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/internal-platform/featureline/internal/feature"
	"github.com/internal-platform/featureline/internal/online"
	"github.com/internal-platform/featureline/internal/store"
)

func main() {
	var (
		addr     = flag.String("addr", ":9500", "listen address")
		snapshot = flag.String("snapshot", "data/redis_snapshot.json", "feature snapshot path")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.LUTC)

	st := store.NewMemory(*snapshot)
	reg := feature.DefaultRegistry()
	svc := online.New(st, reg)

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok", "features": len(reg.Specs)})
	})

	// 线上特征查询
	mux.HandleFunc("/v1/features/", func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Path[len("/v1/features/"):]
		if userID == "" {
			writeJSON(w, 400, map[string]string{"error": "user_id required"})
			return
		}
		fv, err := svc.Compute(r.Context(), userID)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, fv)
	})

	// 特征注册表
	mux.HandleFunc("/v1/registry", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"features": reg.Specs})
	})

	// 写入特征（离线回填与调试都用这个）
	mux.HandleFunc("/v1/admin/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "POST only"})
			return
		}
		var body struct {
			UserID string  `json:"user_id"`
			Name   string  `json:"name"`
			Score  float64 `json:"score"`
			Source string  `json:"source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if _, ok := reg.Specs[body.Name]; !ok {
			writeJSON(w, 400, map[string]string{"error": "unknown feature " + body.Name})
			return
		}
		if body.Source == "" {
			body.Source = "online"
		}
		err := st.Set(r.Context(), body.UserID, body.Name, store.Value{
			Score: body.Score, UpdatedAt: time.Now().UTC(), Source: body.Source,
		})
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]bool{"ok": true})
	})

	// 窗口查询：给 parity 脚本用，返回线上侧认定的窗口区间
	mux.HandleFunc("/v1/window", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		asOf := r.URL.Query().Get("as_of")
		t := time.Now().UTC()
		if asOf != "" {
			if parsed, err := time.Parse(time.RFC3339, asOf); err == nil {
				t = parsed
			}
		}
		start, end, err := svc.WindowFor(name, t)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]string{
			"name":         name,
			"window_start": start.Format(time.RFC3339),
			"window_end":   end.Format(time.RFC3339),
			"convention":   "[start, end) — 左闭右开",
		})
	})

	mux.HandleFunc("/v1/debug", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"store":     st.Stats(),
			"features":  reg.Names(),
			"server_tz": time.Now().Format("MST"),
		})
	})

	srv := &http.Server{Addr: *addr, Handler: logMW(mux), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("featureline serving on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func logMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

var _ = strconv.Itoa
var _ = fmt.Sprintf
