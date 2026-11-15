package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/internal-platform/featureline/internal/registry"
)

// 注册表治理面 HTTP API：
//
//	GET  /v2/registry/active                全部激活版本
//	GET  /v2/registry/versions/{name}       某特征的全部版本（含弃用）
//	GET  /v2/registry/drafts                全部草稿
//	PUT  /v2/registry/drafts                新建/更新草稿（body: definition + revision + change_note）
//	POST /v2/registry/publish/{name}        发布草稿
//	POST /v2/registry/activate/{fqvn}       显式激活（受控切换/回退）
//	POST /v2/registry/rollback/{name}       body: {fqvn}
//	POST /v2/registry/deprecate/{fqvn}      body: {successor, sunset_at}
//	POST /v2/registry/bindings              body: {fqvns:[...]} 或空（绑定全部激活版本）
//	GET  /v2/registry/audit                 审计链

func handleActive(reg *registry.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"versions": reg.ListActive()})
	}
}

func handleVersions(reg *registry.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/v2/registry/versions/")
		if name == "" {
			writeJSON(w, 400, map[string]string{"error": "feature name required"})
			return
		}
		versions := reg.ListVersions(name)
		if len(versions) == 0 {
			writeJSON(w, 404, map[string]string{"error": "unknown feature " + name})
			return
		}
		writeJSON(w, 200, map[string]any{"name": name, "versions": versions})
	}
}

func handleDrafts(reg *registry.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, 200, map[string]any{"drafts": reg.ListDrafts()})
		case http.MethodPut, http.MethodPost:
			var body struct {
				Definition registry.Definition `json:"definition"`
				Revision   int64               `json:"revision"`
				ChangeNote string              `json:"change_note"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				writeJSON(w, 400, map[string]string{"error": err.Error()})
				return
			}
			draft, err := reg.SaveDraft(body.Definition, body.Revision, body.ChangeNote)
			if err != nil {
				writeRegistryError(w, err)
				return
			}
			writeJSON(w, 200, map[string]any{"draft": draft})
		default:
			writeJSON(w, 405, map[string]string{"error": "GET or PUT only"})
		}
	}
}

func handlePublish(reg *registry.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "POST only"})
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/v2/registry/publish/")
		var body struct {
			Actor string `json:"actor"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		res, err := reg.Publish(name, nonEmpty(body.Actor, "api"))
		if err != nil {
			writeRegistryError(w, err)
			return
		}
		code := 200
		if !res.Activated {
			// 不兼容发布已落库但未激活：用 202 告诉调用方“需要显式 activate”。
			code = 202
		}
		writeJSON(w, code, map[string]any{
			"version": res.Version, "change_class": res.Class, "activated": res.Activated,
			"hint": registryHint(res),
		})
	}
}

func handleActivate(reg *registry.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "POST only"})
			return
		}
		fqvn := strings.TrimPrefix(r.URL.Path, "/v2/registry/activate/")
		var body struct {
			Actor string `json:"actor"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if err := reg.Activate(fqvn, nonEmpty(body.Actor, "api")); err != nil {
			writeRegistryError(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "active": fqvn})
	}
}

func handleRollback(reg *registry.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "POST only"})
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/v2/registry/rollback/")
		var body struct {
			FQVN  string `json:"fqvn"`
			Actor string `json:"actor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if err := reg.Rollback(name, body.FQVN, nonEmpty(body.Actor, "api")); err != nil {
			writeRegistryError(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "active": body.FQVN})
	}
}

func handleDeprecate(reg *registry.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "POST only"})
			return
		}
		fqvn := strings.TrimPrefix(r.URL.Path, "/v2/registry/deprecate/")
		var body struct {
			Successor string `json:"successor"`
			SunsetAt  string `json:"sunset_at"`
			Actor     string `json:"actor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		var sunset *time.Time
		if body.SunsetAt != "" {
			t, err := time.Parse(time.RFC3339, body.SunsetAt)
			if err != nil {
				writeJSON(w, 400, map[string]string{"error": "bad sunset_at: " + err.Error()})
				return
			}
			sunset = &t
		}
		if err := reg.Deprecate(fqvn, body.Successor, sunset, nonEmpty(body.Actor, "api")); err != nil {
			writeRegistryError(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "deprecated": fqvn, "successor": body.Successor})
	}
}

func handleBindings(reg *registry.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "POST only"})
			return
		}
		var body struct {
			FQVNS []string   `json:"fqvns"`
			AsOf  *time.Time `json:"as_of"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		var (
			b   registry.Binding
			err error
		)
		if len(body.FQVNS) == 0 {
			b, err = reg.BindActive(body.AsOf)
		} else {
			b, err = reg.Bind(body.FQVNS, body.AsOf)
		}
		if err != nil {
			writeRegistryError(w, err)
			return
		}
		writeJSON(w, 200, b)
	}
}

func handleAudit(reg *registry.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entries, err := reg.Audit()
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"audit": entries})
	}
}

// writeRegistryError 把领域错误映射成合适的 HTTP 状态码。
func writeRegistryError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, registry.ErrConflict):
		writeJSON(w, 409, map[string]string{"error": err.Error()})
	case errors.Is(err, registry.ErrNotFound):
		writeJSON(w, 404, map[string]string{"error": err.Error()})
	case errors.Is(err, registry.ErrImmutable):
		writeJSON(w, 409, map[string]string{"error": err.Error()})
	case errors.Is(err, registry.ErrDeprecated):
		writeJSON(w, 409, map[string]string{"error": err.Error()})
	case errors.Is(err, registry.ErrBindingStale):
		writeJSON(w, 409, map[string]string{"error": err.Error()})
	case errors.Is(err, registry.ErrInvalidName), errors.Is(err, registry.ErrInvalidDefinition):
		writeJSON(w, 400, map[string]string{"error": err.Error()})
	case errors.Is(err, registry.ErrNoDraft):
		writeJSON(w, 409, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, 500, map[string]string{"error": err.Error()})
	}
}

func registryHint(res registry.PublishResult) string {
	if res.Activated {
		return ""
	}
	return "breaking release is inactive; call /v2/registry/activate/" + res.Version.FQVN + " to switch"
}

func nonEmpty(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}
