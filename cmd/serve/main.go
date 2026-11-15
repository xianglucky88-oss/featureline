// Command serve 启动带舱壁隔离与过载控制的线上特征服务。
//
// 容量边界全部可通过 flag 或环境变量配置（flag 优先），
// 环境变量与默认值见 .env.example。
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/internal-platform/featureline/internal/admission"
	"github.com/internal-platform/featureline/internal/feature"
	"github.com/internal-platform/featureline/internal/online"
	"github.com/internal-platform/featureline/internal/server"
	"github.com/internal-platform/featureline/internal/store"
)

func main() {
	var (
		addr     = flag.String("addr", envStr("ADDR", ":9500"), "listen address")
		snapshot = flag.String("snapshot", envStr("SNAPSHOT", "data/redis_snapshot.json"), "feature snapshot path")

		readMaxInFlight = flag.Int("read-max-in-flight", envInt("READ_MAX_IN_FLIGHT", 64), "read lane: max concurrent requests")
		readMaxQueued   = flag.Int("read-max-queued", envInt("READ_MAX_QUEUED", 128), "read lane: max queued requests")
		readQueueTO     = flag.Duration("read-queue-timeout", envDur("READ_QUEUE_TIMEOUT", 200*time.Millisecond), "read lane: max queue wait")
		readHandleTO    = flag.Duration("read-handle-timeout", envDur("READ_HANDLE_TIMEOUT", 2*time.Second), "read lane: per-request handle deadline")

		adminMaxInFlight = flag.Int("admin-max-in-flight", envInt("ADMIN_MAX_IN_FLIGHT", 4), "admin lane: max concurrent requests")
		adminMaxQueued   = flag.Int("admin-max-queued", envInt("ADMIN_MAX_QUEUED", 16), "admin lane: max queued requests")
		adminQueueTO     = flag.Duration("admin-queue-timeout", envDur("ADMIN_QUEUE_TIMEOUT", 500*time.Millisecond), "admin lane: max queue wait")
		adminHandleTO    = flag.Duration("admin-handle-timeout", envDur("ADMIN_HANDLE_TIMEOUT", 5*time.Second), "admin lane: per-request handle deadline")

		shutdownTimeout = flag.Duration("shutdown-timeout", envDur("SHUTDOWN_TIMEOUT", 10*time.Second), "graceful shutdown budget")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.LUTC)

	st := store.NewMemory(*snapshot)
	reg := feature.DefaultRegistry()
	svc := online.New(st, reg)

	cfg := server.Config{
		Read: admission.LaneConfig{
			Name:          admission.LaneRead,
			MaxInFlight:   *readMaxInFlight,
			MaxQueued:     *readMaxQueued,
			QueueTimeout:  *readQueueTO,
			HandleTimeout: *readHandleTO,
		},
		Admin: admission.LaneConfig{
			Name:          admission.LaneAdmin,
			MaxInFlight:   *adminMaxInFlight,
			MaxQueued:     *adminMaxQueued,
			QueueTimeout:  *adminQueueTO,
			HandleTimeout: *adminHandleTO,
		},
	}

	srv, err := server.New(svc, st, reg, cfg)
	if err != nil {
		log.Fatalf("server config invalid: %v", err)
	}
	srv.WithLogger(log.Printf)

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// 信号 -> 先让准入层进入 draining（新请求立即收到 503 draining），
	// 再让 http.Server 停止接收连接并等待在途处理，最后由准入层兜底确认排空。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("featureline serving on %s (read: slots=%d queue=%d, admin: slots=%d queue=%d)",
			*addr, *readMaxInFlight, *readMaxQueued, *adminMaxInFlight, *adminMaxQueued)
		serveErr <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining in-flight requests (budget %s)", *shutdownTimeout)
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()

	// http.Server.Shutdown 关闭监听并等待已接受连接处理完；
	// 它不主动取消请求 context，因此在途请求不会被粗暴终止。
	shutErr := httpServer.Shutdown(shutdownCtx)

	// 再用准入层确认没有遗留的在途/排队请求；剩余预算内等不到就如实记录。
	adm := srv.Admission()
	adm.BeginShutdown()
	if waitErr := adm.Shutdown(shutdownCtx); waitErr != nil {
		snap := adm.Snapshot()
		for _, lm := range snap.Lanes {
			log.Printf("lane %s not drained: in_flight=%d queued=%d", lm.Lane, lm.InFlight, lm.Queued)
		}
		os.Exit(1)
	}
	if shutErr != nil {
		log.Printf("http shutdown: %v", shutErr)
	}
	log.Printf("shutdown complete")
}

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("env %s=%q is not an int, using default %d", key, v, def)
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("env %s=%q is not a duration, using default %s", key, v, def)
	}
	return def
}
