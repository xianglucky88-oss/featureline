package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/internal-platform/featureline/internal/online"
	"github.com/internal-platform/featureline/internal/registry"
	"github.com/internal-platform/featureline/internal/store"
)

func main() {
	var (
		addr        = flag.String("addr", ":9500", "listen address")
		snapshot    = flag.String("snapshot", "data/redis_snapshot.json", "feature snapshot path")
		registryDir = flag.String("registry", "data/registry", "feature definition registry directory")
		bindingPath = flag.String("binding", "data/binding.json", "binding manifest pinned by this instance")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.LUTC)

	// 1. 打开注册表（带跨进程文件锁）并在空库时播种初始定义。
	reg, err := registry.Open(*registryDir)
	if err != nil {
		log.Fatalf("open registry: %v", err)
	}
	defer reg.Close()
	if err := reg.Bootstrap(); err != nil {
		log.Fatalf("bootstrap registry: %v", err)
	}

	// 2. 加载本实例固定的绑定清单；不存在则用当前全部激活版本生成一份。
	binding, err := loadOrCreateBinding(reg, *bindingPath)
	if err != nil {
		log.Fatalf("binding: %v", err)
	}

	// 3. 存储层加载快照；若快照自带清单，与启动绑定做交叉校验。
	st := store.NewMemory(*snapshot)
	if m := st.Manifest(); m != nil {
		if m.ManifestID != binding.ManifestID || m.Fingerprint != binding.Fingerprint {
			log.Printf("WARNING: snapshot manifest %s differs from startup binding %s; "+
				"serving startup binding, values without matching FQVN will be reported missing",
				m.ManifestID, binding.ManifestID)
		}
	}

	svc, err := online.New(st, reg, binding)
	if err != nil {
		log.Fatalf("online service: %v", err)
	}
	log.Printf("featureline serving on %s with binding %s (%d features)",
		*addr, binding.ManifestID, len(binding.Entries))

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"status": "ok", "binding": binding.ManifestID, "features": len(binding.Entries),
		})
	})

	// 线上特征查询：只返回绑定版本内的特征。
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

	mux.HandleFunc("/v1/window", handleWindow(svc))
	mux.HandleFunc("/v1/admin/put", handlePut(reg, st))
	mux.HandleFunc("/v1/debug", handleDebug(st, binding))

	// 注册表治理面。
	mux.HandleFunc("/v2/registry/active", handleActive(reg))
	mux.HandleFunc("/v2/registry/versions/", handleVersions(reg))
	mux.HandleFunc("/v2/registry/drafts", handleDrafts(reg))
	mux.HandleFunc("/v2/registry/publish/", handlePublish(reg))
	mux.HandleFunc("/v2/registry/activate/", handleActivate(reg))
	mux.HandleFunc("/v2/registry/rollback/", handleRollback(reg))
	mux.HandleFunc("/v2/registry/deprecate/", handleDeprecate(reg))
	mux.HandleFunc("/v2/registry/bindings", handleBindings(reg))
	mux.HandleFunc("/v2/registry/audit", handleAudit(reg))

	srv := &http.Server{Addr: *addr, Handler: logMW(mux), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}

func loadOrCreateBinding(reg *registry.Registry, path string) (registry.Binding, error) {
	b, err := registry.ReadBinding(path)
	if err == nil {
		// 解析失败直接拒绝启动：绝不带着漂移的定义上线。
		if _, err := reg.Resolve(b); err != nil {
			return registry.Binding{}, err
		}
		return b, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return registry.Binding{}, err
	}
	created, err := reg.BindActive(nil)
	if err != nil {
		return registry.Binding{}, err
	}
	if err := registry.WriteBinding(path, created); err != nil {
		return registry.Binding{}, err
	}
	return created, nil
}

func handleWindow(svc *online.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		ver, _ := svc.PinnedVersion(name)
		writeJSON(w, 200, map[string]string{
			"name":         name,
			"fqvn":         ver.FQVN,
			"window_start": start.Format(time.RFC3339),
			"window_end":   end.Format(time.RFC3339),
			"convention":   string(ver.Definition.Window.Convention),
		})
	}
}

// handlePut 写入特征值。FQVN 必填且必须是注册表里的已发布版本，
// 从入口阻止“无版本坐标的值”进入存储。
func handlePut(reg *registry.Registry, st *store.MemoryStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "POST only"})
			return
		}
		var body struct {
			UserID string  `json:"user_id"`
			FQVN   string  `json:"fqvn"`
			Score  float64 `json:"score"`
			Source string  `json:"source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		ver, err := reg.Get(body.FQVN)
		if err != nil {
			writeRegistryError(w, err)
			return
		}
		if ver.Status == registry.StatusDeprecated {
			writeJSON(w, 409, map[string]string{
				"error":     "version deprecated; new writes must use the successor",
				"successor": ver.Successor,
			})
			return
		}
		if body.Source == "" {
			body.Source = "online"
		}
		err = st.Set(r.Context(), body.UserID, body.FQVN, store.Value{
			Score: body.Score, UpdatedAt: time.Now().UTC(), Source: body.Source, FQVN: body.FQVN,
		})
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "name": ver.Name, "fqvn": body.FQVN})
	}
}

func handleDebug(st *store.MemoryStore, b registry.Binding) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"store":               st.Stats(),
			"binding":             b.ManifestID,
			"binding_fingerprint": b.Fingerprint,
			"features":            boundNames(b),
			"server_tz":           time.Now().Format("MST"),
		})
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

func boundNames(b registry.Binding) []string {
	out := make([]string, 0, len(b.Entries))
	for _, e := range b.Entries {
		out = append(out, e.Name)
	}
	return out
}
