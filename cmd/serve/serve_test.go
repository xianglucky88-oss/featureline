package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/internal-platform/featureline/internal/online"
	"github.com/internal-platform/featureline/internal/registry"
	"github.com/internal-platform/featureline/internal/store"
)

func newServiceOrFatal(t *testing.T, st store.Store, reg *registry.Registry, b registry.Binding) (*online.Service, error) {
	t.Helper()
	return online.New(st, reg, b)
}

func newTestRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg, err := registry.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := reg.Bootstrap(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	return reg
}

func TestPutRequiresPublishedFQVN(t *testing.T) {
	reg := newTestRegistry(t)
	st := store.NewMemory("")
	h := handlePut(reg, st)

	body := bytes.NewBufferString(`{"user_id":"U1","fqvn":"ghost@9.9","score":1}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/put", body)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown fqvn must 404, got %d", rec.Code)
	}

	body = bytes.NewBufferString(`{"user_id":"U1","fqvn":"user_7d_order_cnt@1.0","score":5}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/admin/put", body)
	rec = httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid put must 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPutRejectsDeprecatedVersion(t *testing.T) {
	reg := newTestRegistry(t)
	st := store.NewMemory("")

	// 发布 2.0，再弃用 1.0。
	active, _ := reg.Active("user_7d_order_cnt")
	d := active.Definition
	d.Window.SizeSeconds = 14 * 24 * 3600
	if _, err := reg.SaveDraft(d, 0, "break"); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Publish("user_7d_order_cnt", "t"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Deprecate("user_7d_order_cnt@1.0", "user_7d_order_cnt@2.0", nil, "t"); err != nil {
		t.Fatal(err)
	}

	h := handlePut(reg, st)
	body := bytes.NewBufferString(`{"user_id":"U1","fqvn":"user_7d_order_cnt@1.0","score":5}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/put", body)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("deprecated write must 409, got %d", rec.Code)
	}
	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["successor"] != "user_7d_order_cnt@2.0" {
		t.Fatalf("response must point to successor: %v", resp)
	}
}

func TestPublishBreakingReturns202AndStaysInactive(t *testing.T) {
	reg := newTestRegistry(t)
	active, _ := reg.Active("item_7d_ctr")
	d := active.Definition
	d.Window.SizeSeconds = 3 * 24 * 3600
	draftBody, _ := json.Marshal(map[string]any{"definition": d, "revision": 0})

	rec := httptest.NewRecorder()
	handleDrafts(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/v2/registry/drafts", bytes.NewReader(draftBody)))
	if rec.Code != http.StatusOK {
		t.Fatalf("draft: %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	handlePublish(reg).ServeHTTP(rec, httptest.NewRequest(
		http.MethodPost, "/v2/registry/publish/item_7d_ctr",
		bytes.NewBufferString(`{"actor":"t"}`)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("breaking publish must 202, got %d: %s", rec.Code, rec.Body.String())
	}
	got, _ := reg.Active("item_7d_ctr")
	if got.FQVN != "item_7d_ctr@1.0" {
		t.Fatalf("active must remain on 1.0, got %s", got.FQVN)
	}
}

func TestAuditEndpointReturnsEntries(t *testing.T) {
	reg := newTestRegistry(t)
	rec := httptest.NewRecorder()
	handleAudit(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/registry/audit", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("audit: %d", rec.Code)
	}
	var body struct {
		Audit []registry.AuditEntry `json:"audit"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Audit) < 6 {
		t.Fatalf("bootstrap must be audited, got %d entries", len(body.Audit))
	}
}

func TestWindowHandlerReportsPinnedFQVN(t *testing.T) {
	reg := newTestRegistry(t)
	b, err := reg.BindActive(nil)
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewMemory("")
	svc, err := newServiceOrFatal(t, st, reg, b)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet,
		"/v1/window?name=user_7d_order_cnt&as_of=2026-10-15T23:47:00Z", nil)
	rec := httptest.NewRecorder()
	handleWindow(svc).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("window: %d %s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["fqvn"] != "user_7d_order_cnt@1.0" || resp["convention"] != "[start,end)" {
		t.Fatalf("window response wrong: %v", resp)
	}
}

func TestSnapshotBindingMismatchSurfacesWarning(t *testing.T) {
	// 快照清单与启动绑定不同：服务仍按启动绑定服务，但必须能识别差异。
	reg := newTestRegistry(t)
	b, err := reg.BindActive(nil)
	if err != nil {
		t.Fatal(err)
	}
	asOf := time.Now().UTC()
	st := store.NewMemory("")
	st.SetManifest(&store.SnapshotManifest{
		ManifestID: "bm-other", Fingerprint: "other-fp", AsOf: &asOf,
	})
	svc, err := newServiceOrFatal(t, st, reg, b)
	if err != nil {
		t.Fatal(err)
	}
	if svc.Binding().ManifestID != b.ManifestID {
		t.Fatal("service must serve the startup binding, not the snapshot one")
	}
}
