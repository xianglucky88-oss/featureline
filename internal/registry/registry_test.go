package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openBootstrapped(t *testing.T) (*Registry, string) {
	t.Helper()
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := r.Bootstrap(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, dir
}

func TestBootstrapSeedsSixPublishedFeatures(t *testing.T) {
	r, _ := openBootstrapped(t)
	active := r.ListActive()
	if len(active) != 6 {
		t.Fatalf("want 6 active features, got %d", len(active))
	}
	for _, v := range active {
		if v.Status != StatusPublished || v.Major != 1 || v.Minor != 0 {
			t.Fatalf("seed version wrong: %+v", v)
		}
		if v.Fingerprint == "" {
			t.Fatalf("missing fingerprint: %s", v.FQVN)
		}
	}
}

func TestBootstrapIdempotent(t *testing.T) {
	r, dir := openBootstrapped(t)
	if err := r.Bootstrap(); err != nil {
		t.Fatalf("second bootstrap: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	r2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r2.Close()
	if len(r2.ListActive()) != 6 {
		t.Fatalf("reopen changed state")
	}
}

func TestDraftOptimisticLockRejectsConcurrentWrites(t *testing.T) {
	r, _ := openBootstrapped(t)
	active, _ := r.Active("user_7d_order_cnt")
	d := active.Definition
	d.Description = "first writer"
	d1, err := r.SaveDraft(d, 0, "edit-1")
	if err != nil {
		t.Fatalf("save draft: %v", err)
	}
	// 第二方拿着过期的 revision=0 提交。
	stale := d
	stale.Description = "second writer"
	if _, err := r.SaveDraft(stale, 0, "edit-2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	// 第一方用最新 revision 提交成功。
	d1.Definition.Description = "first writer again"
	if _, err := r.SaveDraft(d1.Definition, d1.Revision, "edit-3"); err != nil {
		t.Fatalf("save with fresh revision: %v", err)
	}
}

func TestPublishCompatibleBumpsMinorAndAutoActivates(t *testing.T) {
	r, _ := openBootstrapped(t)
	active, _ := r.Active("user_7d_order_cnt")
	d := active.Definition
	d.Description = "文档修订（兼容）"
	draft, err := r.SaveDraft(d, 0, "docs only")
	if err != nil {
		t.Fatalf("draft: %v", err)
	}
	res, err := r.Publish("user_7d_order_cnt", "alice")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if res.Class != ChangeCompatible || !res.Activated {
		t.Fatalf("compatible release should auto-activate: %+v", res)
	}
	if res.Version.FQVN != "user_7d_order_cnt@1.1" {
		t.Fatalf("want @1.1, got %s", res.Version.FQVN)
	}
	if got, _ := r.Active("user_7d_order_cnt"); got.FQVN != "user_7d_order_cnt@1.1" {
		t.Fatalf("active not moved: %s", got.FQVN)
	}
	// 旧版本仍在，支持回退。
	if _, err := r.Get("user_7d_order_cnt@1.0"); err != nil {
		t.Fatalf("old version must remain: %v", err)
	}
	_ = draft
}

func TestPublishBreakingBumpsMajorAndStaysInactive(t *testing.T) {
	r, _ := openBootstrapped(t)
	active, _ := r.Active("user_7d_order_cnt")
	d := active.Definition
	// 窗口 7d → 14d 是语义级不兼容变化。
	d.Window.SizeSeconds = 14 * 24 * 3600
	if _, err := r.SaveDraft(d, 0, "widen window"); err != nil {
		t.Fatalf("draft: %v", err)
	}
	res, err := r.Publish("user_7d_order_cnt", "bob")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if res.Class != ChangeBreaking || res.Activated {
		t.Fatalf("breaking release must stay inactive: %+v", res)
	}
	if res.Version.FQVN != "user_7d_order_cnt@2.0" {
		t.Fatalf("want @2.0, got %s", res.Version.FQVN)
	}
	// 关键不变量：默认版本仍指向旧语义，旧模型不会无感知读到新数值。
	if got, _ := r.Active("user_7d_order_cnt"); got.FQVN != "user_7d_order_cnt@1.0" {
		t.Fatalf("active moved without explicit activate: %s", got.FQVN)
	}
	if err := r.Activate("user_7d_order_cnt@2.0", "bob"); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if got, _ := r.Active("user_7d_order_cnt"); got.FQVN != "user_7d_order_cnt@2.0" {
		t.Fatalf("activate failed: %s", got.FQVN)
	}
}

func TestAllSemanticFieldsAreBreaking(t *testing.T) {
	r, _ := openBootstrapped(t)
	base, _ := r.Active("user_7d_order_cnt")
	cases := map[string]func(*Definition){
		"entity": func(d *Definition) { d.Entity = EntityItem },
		"value_type": func(d *Definition) {
			d.ValueType = ValueInt64
			d.Default = DefaultSpec{Type: ValueInt64, Value: "0"}
		},
		"kind":        func(d *Definition) { d.Kind = "ratio" },
		"window":      func(d *Definition) { d.Window.SizeSeconds = 0 },
		"time_column": func(d *Definition) { d.Time.TimeColumn = "processing_time" },
		"time_bound":  func(d *Definition) { d.Time.RightBound = "request_time" },
		"time_zone":   func(d *Definition) { d.Time.TimeZone = "Asia/Shanghai" },
		"default":     func(d *Definition) { d.Default = DefaultSpec{Type: ValueFloat64, Value: "-1"} },
	}
	for name, mutate := range cases {
		d := base.Definition
		mutate(&d)
		if got := Classify(base.Definition, d); got != ChangeBreaking {
			t.Fatalf("%s change must be breaking, got %s", name, got)
		}
	}
	meta := base.Definition
	meta.Description = "x"
	meta.Owner = "new-team"
	meta.Tags = []string{"a"}
	if got := Classify(base.Definition, meta); got != ChangeCompatible {
		t.Fatalf("metadata change must be compatible, got %s", got)
	}
}

func TestPublishedVersionIsImmutable(t *testing.T) {
	r, _ := openBootstrapped(t)
	v, _ := r.Get("user_7d_order_cnt@1.0")
	// 注册表没有提供任何修改已发布版本的入口；尝试把它当草稿保存也只会生成新草稿，
	// 原版本指针与指纹不变。
	d := v.Definition
	d.Window.SizeSeconds = 0
	if _, err := r.SaveDraft(d, 0, "attempted mutation"); err != nil {
		t.Fatalf("draft: %v", err)
	}
	again, _ := r.Get("user_7d_order_cnt@1.0")
	if again.Fingerprint != v.Fingerprint {
		t.Fatal("published version fingerprint changed")
	}
}

func TestPublishIdempotentAfterInterruption(t *testing.T) {
	r, _ := openBootstrapped(t)
	active, _ := r.Active("user_7d_order_cnt")
	d := active.Definition
	d.Description = "重试安全"
	if _, err := r.SaveDraft(d, 0, "n"); err != nil {
		t.Fatalf("draft: %v", err)
	}
	first, err := r.Publish("user_7d_order_cnt", "c")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	// 模拟“发布中断后重试”：发布已删草稿，重试方从 revision=0 重新提交同内容草稿再发布。
	if _, err := r.SaveDraft(d, 0, "n-retry"); err != nil {
		t.Fatalf("redraft: %v", err)
	}
	second, err := r.Publish("user_7d_order_cnt", "c")
	if err != nil {
		t.Fatalf("republish: %v", err)
	}
	if first.Version.FQVN != second.Version.FQVN {
		t.Fatalf("retry created duplicate version: %s vs %s",
			first.Version.FQVN, second.Version.FQVN)
	}
	if len(r.ListVersions("user_7d_order_cnt")) != 2 { // 1.0 + 1.1
		t.Fatalf("version count wrong: %d", len(r.ListVersions("user_7d_order_cnt")))
	}
}

func TestRollbackRestoresPreviousDefault(t *testing.T) {
	r, _ := openBootstrapped(t)
	active, _ := r.Active("user_7d_order_cnt")

	d2 := active.Definition
	d2.Description = "minor"
	if _, err := r.SaveDraft(d2, 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish("user_7d_order_cnt", "a"); err != nil {
		t.Fatal(err)
	}

	d3 := active.Definition
	d3.Window.SizeSeconds = 14 * 24 * 3600
	if _, err := r.SaveDraft(d3, 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish("user_7d_order_cnt", "a"); err != nil {
		t.Fatal(err)
	}
	if err := r.Activate("user_7d_order_cnt@2.0", "a"); err != nil {
		t.Fatal(err)
	}
	if err := r.Rollback("user_7d_order_cnt", "user_7d_order_cnt@1.1", "a"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	got, _ := r.Active("user_7d_order_cnt")
	if got.FQVN != "user_7d_order_cnt@1.1" {
		t.Fatalf("rollback failed: %s", got.FQVN)
	}
	// 2.0 仍然存在，可再次切回。
	if _, err := r.Get("user_7d_order_cnt@2.0"); err != nil {
		t.Fatalf("rolled-back version must be retained: %v", err)
	}
}

func TestDeprecationKeepsOldBindingsServingButBlocksNewOnes(t *testing.T) {
	r, _ := openBootstrapped(t)
	oldBinding, err := r.BindActive(nil)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	// 发布 2.0 并弃用 1.0，指向后继。
	active, _ := r.Active("user_7d_order_cnt")
	d := active.Definition
	d.Window.SizeSeconds = 14 * 24 * 3600
	if _, err := r.SaveDraft(d, 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish("user_7d_order_cnt", "a"); err != nil {
		t.Fatal(err)
	}
	if err := r.Deprecate("user_7d_order_cnt@1.0", "user_7d_order_cnt@2.0", nil, "a"); err != nil {
		t.Fatalf("deprecate: %v", err)
	}
	// 旧绑定仍可解析——消费者升级滞后不阻断服务。
	if _, err := r.Resolve(oldBinding); err != nil {
		t.Fatalf("old binding must still resolve after deprecation: %v", err)
	}
	// 新绑定拒绝引用弃用版本，错误里带 successor，迁移路径可验证。
	_, err = r.Bind([]string{"user_7d_order_cnt@1.0"}, nil)
	if !errors.Is(err, ErrDeprecated) {
		t.Fatalf("want ErrDeprecated, got %v", err)
	}
	// 后继可以正常绑定。
	if _, err := r.Bind([]string{"user_7d_order_cnt@2.0"}, nil); err != nil {
		t.Fatalf("successor binding: %v", err)
	}
}

func TestParallelMajorVersionsCoexist(t *testing.T) {
	r, _ := openBootstrapped(t)
	active, _ := r.Active("user_7d_gmv_cents")
	d := active.Definition
	d.Window.SizeSeconds = 3 * 24 * 3600
	if _, err := r.SaveDraft(d, 0, ""); err != nil {
		t.Fatal(err)
	}
	res, err := r.Publish("user_7d_gmv_cents", "a")
	if err != nil {
		t.Fatal(err)
	}
	versions := r.ListVersions("user_7d_gmv_cents")
	if len(versions) != 2 || versions[0].FQVN != "user_7d_gmv_cents@1.0" ||
		versions[1].FQVN != "user_7d_gmv_cents@2.0" {
		t.Fatalf("parallel versions wrong: %+v", versions)
	}
	// 两个 major 可以分别被不同消费者绑定，互不影响。
	b1, err := r.Bind([]string{"user_7d_gmv_cents@1.0"}, nil)
	if err != nil {
		t.Fatalf("bind v1: %v", err)
	}
	b2, err := r.Bind([]string{"user_7d_gmv_cents@2.0"}, nil)
	if err != nil {
		t.Fatalf("bind v2: %v", err)
	}
	m1, _ := r.Resolve(b1)
	m2, _ := r.Resolve(b2)
	if m1["user_7d_gmv_cents"].Fingerprint == m2["user_7d_gmv_cents"].Fingerprint {
		t.Fatal("parallel majors resolved to identical definition")
	}
	_ = res
}

func TestBindingRoundTripAndTamperDetection(t *testing.T) {
	r, dir := openBootstrapped(t)
	b, err := r.BindActive(nil)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	path := filepath.Join(dir, "binding.json")
	if err := WriteBinding(path, b); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadBinding(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if _, err := r.Resolve(got); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// 手改清单：指纹校验必须失败。
	got.Entries[0].Fingerprint = "deadbeef"
	if err := WriteBinding(path, got); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBinding(path); !errors.Is(err, ErrBindingStale) {
		t.Fatalf("tampered binding must be rejected, got %v", err)
	}
}

func TestResolveFailsOnUnknownVersion(t *testing.T) {
	r, _ := openBootstrapped(t)
	b, _ := r.BindActive(nil)
	b.Entries[0].FQVN = "ghost_feature@9.9"
	b.Fingerprint = BindingFingerprint(b.ManifestID, b.Entries)
	if _, err := r.Resolve(b); !errors.Is(err, ErrBindingStale) {
		t.Fatalf("want ErrBindingStale, got %v", err)
	}
}

func TestStateSurvivesRestartWithNoHalfPublishedVersion(t *testing.T) {
	r1, dir := openBootstrapped(t)
	if err := r1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// 重新打开：状态应完整恢复（flock 随 Close 释放）。
	r2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r2.Close()
	if len(r2.ListActive()) != 6 {
		t.Fatal("state lost across restart")
	}
	for _, fqvn := range []string{
		"user_7d_order_cnt@1.0", "user_7d_gmv_cents@1.0", "user_30d_order_cnt@1.0",
		"item_7d_ctr@1.0", "item_7d_exposure@1.0", "user_category_affinity@1.0",
	} {
		v, err := r2.Get(fqvn)
		if err != nil {
			t.Fatalf("missing %s after restart: %v", fqvn, err)
		}
		if v.Status != StatusPublished {
			t.Fatalf("%s not published after restart", fqvn)
		}
	}
}

func TestCorruptStateIsRejectedNotSilentlyReset(t *testing.T) {
	_, dir := openBootstrapped(t)
	if err := os.WriteFile(filepath.Join(dir, stateFile), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("corrupt state must fail loudly, never reset to empty")
	}
}

func TestCrossProcessLock(t *testing.T) {
	_, dir := openBootstrapped(t)
	second, err := Open(dir)
	if err == nil {
		_ = second.Close()
		t.Fatal("second concurrent Open must be rejected by file lock")
	}
}

func TestAuditTrailCapturesEveryTransition(t *testing.T) {
	r, _ := openBootstrapped(t)
	active, _ := r.Active("user_7d_order_cnt")
	d := active.Definition
	d.Description = "审计"
	if _, err := r.SaveDraft(d, 0, "n"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish("user_7d_order_cnt", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := r.Rollback("user_7d_order_cnt", "user_7d_order_cnt@1.0", "alice"); err != nil {
		t.Fatal(err)
	}
	entries, err := r.Audit()
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	actions := map[string]bool{}
	for _, e := range entries {
		actions[e.Action] = true
		if e.Seq <= 0 {
			t.Fatalf("audit seq must be positive: %+v", e)
		}
	}
	for _, want := range []string{"publish", "save_draft", "rollback"} {
		if !actions[want] {
			t.Fatalf("audit missing action %s, have %v", want, actions)
		}
	}
	// 审计序号严格递增，可按序重建任意一次交付的语义来源。
	for i := 1; i < len(entries); i++ {
		if entries[i].Seq <= entries[i-1].Seq {
			t.Fatal("audit seq not monotonic")
		}
	}
}

func TestValidationRejectsBadDefinitions(t *testing.T) {
	r, _ := openBootstrapped(t)
	good := SeedDefinitions()[0]
	cases := map[string]Definition{
		"bad name":         withName(good, "Bad_Name"),
		"bad convention":   withConvention(good, "(start,end]"),
		"negative window":  withWindow(good, -1),
		"missing tz":       withTZ(good, ""),
		"default mismatch": withDefault(good, DefaultSpec{Type: ValueString, Value: "0"}),
	}
	for label, d := range cases {
		if _, err := r.SaveDraft(d, 0, label); err == nil {
			t.Fatalf("%s should be rejected", label)
		}
	}
}

func TestFQVNRoundTrip(t *testing.T) {
	name, maj, min, ok := ParseFQVN("user_7d_order_cnt@2.1")
	if !ok || name != "user_7d_order_cnt" || maj != 2 || min != 1 {
		t.Fatal("parse failed")
	}
	for _, bad := range []string{"", "name", "name@", "@1.0", "name@1", "name@x.0", "Bad@1.0"} {
		if _, _, _, ok := ParseFQVN(bad); ok {
			t.Fatalf("bad FQVN accepted: %q", bad)
		}
	}
	if FQVN("a", 1, 0) != "a@1.0" {
		t.Fatal("format wrong")
	}
}

func TestConcurrentDraftsSerializeWithoutCorruption(t *testing.T) {
	r, _ := openBootstrapped(t)
	base, _ := r.Active("item_7d_ctr")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 32)
	for g := 0; g < 8; g++ {
		go func(g int) {
			for i := 0; i < 20; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				d, err := r.Draft("item_7d_ctr")
				if errors.Is(err, ErrNotFound) {
					d = Version{Definition: base.Definition, Revision: 0}
				} else if err != nil {
					errs <- err
					return
				}
				d.Definition.Description = fmt.Sprintf("g%d-i%d", g, i)
				if _, err := r.SaveDraft(d.Definition, d.Revision, "concurrent"); err != nil &&
					!errors.Is(err, ErrConflict) {
					errs <- err
					return
				}
			}
		}(g)
	}
	// 没有等待原语需求：SaveDraft 内部有锁，跑一小段时间后检查状态自洽。
	time.Sleep(200 * time.Millisecond)
	cancel()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent write error: %v", err)
	}
	draft, err := r.Draft("item_7d_ctr")
	if err != nil {
		t.Fatalf("draft after concurrency: %v", err)
	}
	if draft.Fingerprint != Fingerprint(draft.Definition) {
		t.Fatal("draft fingerprint inconsistent")
	}
}

// ---- helpers ----

func withName(d Definition, n string) Definition { d.Name = n; return d }
func withConvention(d Definition, c string) Definition {
	d.Window.Convention = WindowConvention(c)
	return d
}
func withWindow(d Definition, s int64) Definition { d.Window.SizeSeconds = s; return d }
func withTZ(d Definition, tz string) Definition   { d.Time.TimeZone = tz; return d }
func withDefault(d Definition, def DefaultSpec) Definition {
	d.Default = def
	return d
}
