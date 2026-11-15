package store

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testValue(score float64) Value {
	return Value{Score: score, UpdatedAt: time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC), Source: "online"}
}

func mustOpen(t *testing.T, dir string, opts ...Option) *DurableStore {
	t.Helper()
	s, err := Open(dir, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestDurableRoundTripAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U1", "user_7d_order_cnt", testValue(3)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := mustOpen(t, dir)
	v, ok, err := s2.Get(ctx, "U1", "user_7d_order_cnt")
	if err != nil || !ok {
		t.Fatalf("get after reopen: ok=%v err=%v", ok, err)
	}
	if v.Score != 3 || v.Source != "online" {
		t.Fatalf("value mismatch: %+v", v)
	}
	if v.UpdatedAt != testValue(0).UpdatedAt {
		t.Fatalf("updated_at mismatch: %v", v.UpdatedAt)
	}
}

func TestDurableEmptyInitThenGet(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	if _, ok, err := s.Get(context.Background(), "U1", "x"); err != nil || ok {
		t.Fatalf("empty store: ok=%v err=%v", ok, err)
	}
	st := s.Stats()
	if st.Generation != 1 || st.Keys != 0 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func TestSetBatchIsVisibleAsAWhole(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	ctx := context.Background()
	err := s.SetBatch(ctx, []Entry{
		{UserID: "U1", Name: "a", Value: testValue(1)},
		{UserID: "U1", Name: "b", Value: testValue(2)},
	})
	if err != nil {
		t.Fatalf("set batch: %v", err)
	}
	got, err := s.MGet(ctx, "U1", []string{"a", "b"})
	if err != nil {
		t.Fatalf("mget: %v", err)
	}
	if len(got) != 2 || got["a"].Score != 1 || got["b"].Score != 2 {
		t.Fatalf("mget mismatch: %+v", got)
	}
}

// 崩溃后恢复：WAL 尾部写了一半的记录必须被丢弃，之前确认的写入必须仍在。
func TestRecoverTruncatesTornWALTail(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	walPath := s.walPath
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 模拟写到一半崩溃：在 WAL 末尾追加一个只有 3 字节的残缺头。
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	_ = f.Close()

	s2 := mustOpen(t, dir)
	v, ok, err := s2.Get(ctx, "U1", "a")
	if err != nil || !ok || v.Score != 1 {
		t.Fatalf("confirmed write lost: ok=%v v=%+v err=%v", ok, v, err)
	}
	if !s2.Stats().TornTail {
		t.Fatal("expected torn tail to be reported")
	}
	// 恢复后必须能继续追加且新记录可读。
	if err := s2.Set(ctx, "U1", "b", testValue(2)); err != nil {
		t.Fatalf("set after recovery: %v", err)
	}
}

// 完整长度但 CRC 不符的末尾记录：fsync 未完成时可能落盘任意内容，按残尾丢弃。
func TestRecoverDropsChecksumMismatchAtEnd(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	walPath := s.walPath
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	// 追加一条结构完整（长度字段正确）但 payload 被篡改的记录。
	bad := []byte(`{"entries":[{"key":"f:U1:b","value":{"score":9,"updated_at":"2026-11-14T00:00:00Z","source":"x"}}]}`)
	hdr := make([]byte, recordHeaderSize)
	putHeader(hdr, len(bad), 0xdeadbeef)
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if _, err := f.Write(append(hdr, bad...)); err != nil {
		t.Fatalf("append corrupt record: %v", err)
	}
	_ = f.Close()
	if len(raw) == 0 {
		t.Fatal("empty wal")
	}

	s2 := mustOpen(t, dir)
	if _, ok, _ := s2.Get(ctx, "U1", "a"); !ok {
		t.Fatal("confirmed write lost")
	}
	if _, ok, _ := s2.Get(ctx, "U1", "b"); ok {
		t.Fatal("corrupt trailing record must not be applied")
	}
}

// 文件中间出现 CRC 不符的记录是真正的损坏，必须报错而不是静默丢后续数据。
func TestRecoverRejectsMidFileWALCorruption(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	for _, n := range []string{"a", "b"} {
		if err := s.Set(ctx, "U1", n, testValue(1)); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	walPath := s.walPath
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// 翻转第一条记录 payload 中的一个字节（不是末尾）。
	raw[recordHeaderSize+2] ^= 0xff
	if err := os.WriteFile(walPath, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

// MANIFEST 指向的快照被篡改：拒绝启动，不能回退到旧代。
func TestRecoverRejectsCorruptManifestSnapshot(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	snapPath := filepath.Join(dir, snapshotName(s.gen))
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	tampered := []byte(`{"f:U1:a": {"score": 7, "updated_at": "2026-11-14T00:00:00Z", "source": "online"}}`)
	if err := os.WriteFile(snapPath, tampered, 0o600); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

// MANIFEST 丢失时按最高代选择；未提交的更高代（残缺）会被跳过。
func TestRecoverWithoutManifestChoosesNewestValidGeneration(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	gen := s.gen
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, manifestName)); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}
	// 伪造一个更高、残缺的代（模拟压缩中途崩溃留下的临时产物）。
	if err := os.WriteFile(filepath.Join(dir, snapshotName(gen+1)), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write bogus gen: %v", err)
	}

	s2 := mustOpen(t, dir)
	if v, ok, _ := s2.Get(ctx, "U1", "a"); !ok || v.Score != 1 {
		t.Fatalf("newest valid generation not selected: ok=%v v=%+v", ok, v)
	}
	if s2.gen != gen {
		t.Fatalf("expected generation %d, got %d", gen, s2.gen)
	}
	// MANIFEST 应被补写。
	if _, err := os.Stat(filepath.Join(dir, manifestName)); err != nil {
		t.Fatalf("manifest not regenerated: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, snapshotName(gen+1))); !os.IsNotExist(err) {
		t.Fatalf("bogus generation should be reclaimed, stat err=%v", err)
	}
}

// 压缩：新代切换后旧文件被回收，数据完整，WAL 归零。
func TestCompactionReclaimsOldGeneration(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	for i, name := range []string{"a", "b", "c"} {
		if err := s.Set(ctx, "U"+name, "x", testValue(float64(i))); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	oldGen := uint64(1)
	if _, err := os.Stat(filepath.Join(dir, snapshotName(oldGen))); !os.IsNotExist(err) {
		t.Fatalf("old snapshot not reclaimed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, walName(oldGen))); !os.IsNotExist(err) {
		t.Fatalf("old wal not reclaimed: %v", err)
	}
	if s.Stats().WALBytes != 0 {
		t.Fatalf("wal should be empty after compaction: %+v", s.Stats())
	}
	for i, name := range []string{"a", "b", "c"} {
		if v, ok, _ := s.Get(ctx, "U"+name, "x"); !ok || v.Score != float64(i) {
			t.Fatalf("U%s lost after compaction: ok=%v v=%+v", name, ok, v)
		}
	}
	// 重开后数据仍在。
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2 := mustOpen(t, dir)
	for i, name := range []string{"a", "b", "c"} {
		if v, ok, _ := s2.Get(ctx, "U"+name, "x"); !ok || v.Score != float64(i) {
			t.Fatalf("U%s lost after reopen: ok=%v v=%+v", name, ok, v)
		}
	}
}

func TestCompactionThresholdTriggeredByWrites(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, WithCompactionThreshold(512))
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := s.Set(ctx, "U1", "a", testValue(float64(i))); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if s.Stats().Compactions == 0 {
		t.Fatal("threshold should have triggered compaction")
	}
	if v, ok, _ := s.Get(ctx, "U1", "a"); !ok || v.Score != 19 {
		t.Fatalf("latest value lost: ok=%v v=%+v", ok, v)
	}
}

// 并发写：所有写入串行化到同一 WAL，重开后一条不少。
func TestConcurrentWritersAllPersist(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	ctx := context.Background()
	const writers, per = 8, 50
	var wg sync.WaitGroup
	errCh := make(chan error, writers*per)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := s.Set(ctx, "U1", featureName(w, i), testValue(float64(w*per+i))); err != nil {
					errCh <- err
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent set: %v", err)
	}
	if got := len(mustKeys(t, s)); got != writers*per {
		t.Fatalf("want %d keys, got %d", writers*per, got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := mustOpen(t, dir)
	if got := len(mustKeys(t, s2)); got != writers*per {
		t.Fatalf("after reopen want %d keys, got %d", writers*per, got)
	}
}

// 并发读与写/压缩交错，读到的必须是完整值。
func TestReadsStayConsistentDuringWritesAndCompaction(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, WithCompactionThreshold(1024))
	ctx := context.Background()

	stop := make(chan struct{})
	readsDone := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				readsDone <- nil
				return
			default:
			}
			values, err := s.MGet(ctx, "U1", []string{"a", "b"})
			if err != nil {
				readsDone <- err
				return
			}
			// 批量写保证 a、b 始终成对且来自同一批：a 与 b 的 Score 必须相等。
			if len(values) == 2 && values["a"].Score != values["b"].Score {
				readsDone <- errors.New("torn batch observed by reader")
				return
			}
		}
	}()

	for i := 0; i < 300; i++ {
		score := float64(i)
		if err := s.SetBatch(ctx, []Entry{
			{UserID: "U1", Name: "a", Value: testValue(score)},
			{UserID: "U1", Name: "b", Value: testValue(score)},
		}); err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	close(stop)
	if err := <-readsDone; err != nil {
		t.Fatalf("reader: %v", err)
	}
}

// 目录锁：第二个进程无法打开同一目录。
func TestSecondProcessIsLockedOut(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	if _, err := Open(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("expected ErrLocked, got %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// 关闭后可以重新打开。
	s2 := mustOpen(t, dir)
	if s2 == nil {
		t.Fatal("reopen after close failed")
	}
}

func TestClosedStoreRejectsOperations(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	ctx := context.Background()
	if _, _, err := s.Get(ctx, "U1", "a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("get after close: %v", err)
	}
	if err := s.Set(ctx, "U1", "a", testValue(1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("set after close: %v", err)
	}
}

func TestNonFiniteValuesSurvivePersistence(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	for name, score := range map[string]float64{
		"nan": math.NaN(), "inf": math.Inf(1), "ninf": math.Inf(-1),
	} {
		if err := s.Set(ctx, "U1", name, testValue(score)); err != nil {
			t.Fatalf("set %s: %v", name, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2 := mustOpen(t, dir)
	if v, ok, _ := s2.Get(ctx, "U1", "nan"); !ok || !math.IsNaN(v.Score) {
		t.Fatalf("NaN lost: ok=%v v=%v", ok, v.Score)
	}
	if v, ok, _ := s2.Get(ctx, "U1", "inf"); !ok || !math.IsInf(v.Score, 1) {
		t.Fatalf("+Inf lost: ok=%v v=%v", ok, v.Score)
	}
	if v, ok, _ := s2.Get(ctx, "U1", "ninf"); !ok || !math.IsInf(v.Score, -1) {
		t.Fatalf("-Inf lost: ok=%v v=%v", ok, v.Score)
	}
}

// 首次初始化在写完快照、还没写 MANIFEST 时崩溃：没有已确认写入，
// 下次启动必须能正常初始化，而不是报 corrupt。
func TestInterruptedInitializationRecovers(t *testing.T) {
	dir := t.TempDir()
	// 模拟 bootstrap 前半段留下的产物，无 MANIFEST、无 WAL。
	if err := os.WriteFile(filepath.Join(dir, snapshotName(1)), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write partial snapshot: %v", err)
	}
	ctx := context.Background()
	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if v, ok, _ := s.Get(ctx, "U1", "a"); !ok || v.Score != 1 {
		t.Fatalf("store unusable after interrupted init: ok=%v v=%+v", ok, v)
	}
	if s.Stats().Generation != 1 {
		t.Fatalf("expected fresh generation 1, got %d", s.Stats().Generation)
	}
}

// 相反的边界：WAL 里有已确认记录但没有 MANIFEST（例如手工删了 MANIFEST
// 且快照也坏了）——必须报错，不能重新初始化把数据悄悄清空。
func TestNoManifestWithAcknowledgedWALIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	snapPath := filepath.Join(dir, snapshotName(s.gen))
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, manifestName)); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}
	if err := os.WriteFile(snapPath, []byte("{broken"), 0o600); err != nil {
		t.Fatalf("corrupt snapshot: %v", err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

// 批量写入的原子性在崩溃边界上同样成立：整批只有一条 WAL 记录，
// 尾部残缺时整批都不应出现。
func TestCrashMidBatchDropsWholeBatch(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U1", "before", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	walPath := s.walPath
	if err := s.SetBatch(ctx, []Entry{
		{UserID: "U1", Name: "b1", Value: testValue(2)},
		{UserID: "U1", Name: "b2", Value: testValue(3)},
	}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// 砍掉最后一条记录的一半，模拟 fsync 只完成后半段。
	truncated := raw[:len(raw)-20]
	if err := os.WriteFile(walPath, truncated, 0o600); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	s2 := mustOpen(t, dir)
	if _, ok, _ := s2.Get(ctx, "U1", "before"); !ok {
		t.Fatal("earlier confirmed write lost")
	}
	for _, name := range []string{"b1", "b2"} {
		if _, ok, _ := s2.Get(ctx, "U1", name); ok {
			t.Fatalf("half-written batch leaked %q", name)
		}
	}
}

// MANIFEST 是版本权威：即使磁盘上存在更高的未提交代，也必须以它为准。
func TestManifestWinsOverHigherUncommittedGeneration(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	gen := s.gen
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// 模拟压缩在“新代文件已落盘、MANIFEST 尚未切换”时崩溃。
	if err := os.WriteFile(filepath.Join(dir, snapshotName(gen+1)),
		[]byte(`{"f:U1:a":{"score":99,"updated_at":"2026-11-14T00:00:00Z","source":"online"}}`), 0o600); err != nil {
		t.Fatalf("write uncommitted snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, walName(gen+1)), nil, 0o600); err != nil {
		t.Fatalf("write uncommitted wal: %v", err)
	}

	s2 := mustOpen(t, dir)
	if v, ok, _ := s2.Get(ctx, "U1", "a"); !ok || v.Score != 1 {
		t.Fatalf("manifest generation not used: ok=%v v=%+v", ok, v)
	}
	if s2.gen != gen {
		t.Fatalf("expected committed generation %d, got %d", gen, s2.gen)
	}
	if _, err := os.Stat(filepath.Join(dir, snapshotName(gen+1))); !os.IsNotExist(err) {
		t.Fatalf("uncommitted generation should be reclaimed: %v", err)
	}
}

// 读路径不持有写锁：即使写者/压缩占着 writeMu，在线读取也必须立即返回。
func TestReadsDoNotWaitForWriteLock(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	ctx := context.Background()
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, ok, err := s.Get(ctx, "U1", "a"); err != nil || !ok {
			t.Errorf("get: ok=%v err=%v", ok, err)
		}
		if _, err := s.MGet(ctx, "U1", []string{"a"}); err != nil {
			t.Errorf("mget: %v", err)
		}
		if _, err := s.Snapshot(ctx); err != nil {
			t.Errorf("snapshot: %v", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reads blocked behind writeMu")
	}
}

// WithSnapshotReplace：离线重算快照后，重启能让新值生效（同 key 覆盖），
// 同时保留快照没有覆盖的在线写入。
// 目录不可写（介质只读）时：首次初始化必须失败且不留下半套文件。
func TestOpenOnReadOnlyDirFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if _, err := Open(dir); err == nil {
		t.Fatal("expected open to fail on read-only directory")
	}
}

// 快照文件被截断：MANIFEST 的大小字段能发现不一致，拒绝启动。
func TestManifestDetectsTruncatedSnapshot(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	snapPath := filepath.Join(dir, snapshotName(s.gen))
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	st, err := os.Stat(snapPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(snapPath, st.Size()-5); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

// 全新目录 + 强制快照模式：快照直接成为第 1 代基线，不应再多写一代。
func TestSnapshotReplaceOnFreshDirKeepsGenerationOne(t *testing.T) {
	dir := t.TempDir()
	seed := filepath.Join(t.TempDir(), "seed.json")
	if err := os.WriteFile(seed, []byte(`{"f:U1:a":{"score":5,"updated_at":"2026-11-14T00:00:00Z","source":"offline"}}`), 0o600); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	s := mustOpen(t, dir, WithSnapshotReplace(seed))
	if s.Stats().Generation != 1 {
		t.Fatalf("expected generation 1, got %d", s.Stats().Generation)
	}
	if v, ok, _ := s.Get(context.Background(), "U1", "a"); !ok || v.Score != 5 {
		t.Fatalf("seed not applied: ok=%v v=%+v", ok, v)
	}
}

func TestSnapshotReplaceOverwritesAndPreserves(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	seed := filepath.Join(t.TempDir(), "seed.json")
	if err := os.WriteFile(seed, []byte(`{"f:U1:a":{"score":5,"updated_at":"2026-11-14T00:00:00Z","source":"offline"}}`), 0o600); err != nil {
		t.Fatalf("write seed: %v", err)
	}

	s := mustOpen(t, dir, WithSeedSnapshot(seed))
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := s.Set(ctx, "U2", "b", testValue(2)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重新打开并强制以快照为准：a 回到 5，b 保留。
	s2 := mustOpen(t, dir, WithSnapshotReplace(seed))
	if v, ok, _ := s2.Get(ctx, "U1", "a"); !ok || v.Score != 5 {
		t.Fatalf("snapshot replace did not win: ok=%v v=%+v", ok, v)
	}
	if v, ok, _ := s2.Get(ctx, "U2", "b"); !ok || v.Score != 2 {
		t.Fatalf("snapshot replace dropped unrelated key: ok=%v v=%+v", ok, v)
	}
}

// 强制导入也必须是原子的：导入后重新打开，读到的是完整合并结果。
func TestSnapshotReplaceIsDurable(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	seed := filepath.Join(t.TempDir(), "seed.json")
	if err := os.WriteFile(seed, []byte(`{"f:U1:a":{"score":5,"updated_at":"2026-11-14T00:00:00Z","source":"offline"}}`), 0o600); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	s := mustOpen(t, dir)
	if err := s.Set(ctx, "U2", "b", testValue(2)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2 := mustOpen(t, dir, WithSnapshotReplace(seed))
	if err := s2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s3 := mustOpen(t, dir)
	if v, ok, _ := s3.Get(ctx, "U1", "a"); !ok || v.Score != 5 {
		t.Fatalf("replaced value lost after restart: ok=%v v=%+v", ok, v)
	}
	if v, ok, _ := s3.Get(ctx, "U2", "b"); !ok || v.Score != 2 {
		t.Fatalf("preserved value lost after restart: ok=%v v=%+v", ok, v)
	}
}

func TestSeedSnapshotImportedOnlyWhenEmpty(t *testing.T) {
	dir := t.TempDir()
	seed := filepath.Join(t.TempDir(), "seed.json")
	if err := os.WriteFile(seed, []byte(`{"f:U1:a":{"score":5,"updated_at":"2026-11-14T00:00:00Z","source":"offline"}}`), 0o600); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	ctx := context.Background()
	s := mustOpen(t, dir, WithSeedSnapshot(seed))
	if v, ok, _ := s.Get(ctx, "U1", "a"); !ok || v.Score != 5 {
		t.Fatalf("seed not imported: ok=%v v=%+v", ok, v)
	}
	if !s.Stats().SeedImported {
		t.Fatal("seed import should be reported")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// 已经写过数据：再次打开不应导入 seed，也不应覆盖增量写入。
	s2 := mustOpen(t, dir, WithSeedSnapshot(seed))
	if err := s2.Set(ctx, "U1", "a", testValue(6)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if s2.Stats().SeedImported {
		t.Fatal("seed must not be re-imported on non-empty store")
	}
}

func TestSnapshotReturnsConsistentCopy(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	ctx := context.Background()
	if err := s.SetBatch(ctx, []Entry{
		{UserID: "U1", Name: "a", Value: testValue(1)},
		{UserID: "U1", Name: "b", Value: testValue(2)},
	}); err != nil {
		t.Fatalf("set: %v", err)
	}
	all, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(all) != 2 || all["f:U1:a"].Score != 1 || all["f:U1:b"].Score != 2 {
		t.Fatalf("snapshot mismatch: %+v", all)
	}
}

// 批量读取的一致性：并发写入期间取的 Snapshot 必须来自同一个一致视图，
// 不会出现同一批次的 a/b 分离。
func TestSnapshotConsistentUnderConcurrentBatches(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, WithCompactionThreshold(2048))
	ctx := context.Background()

	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			all, err := s.Snapshot(ctx)
			if err != nil {
				done <- err
				return
			}
			for i := 0; i < 20; i++ {
				a, aok := all["f:U"+itoa(i)+":a"]
				b, bok := all["f:U"+itoa(i)+":b"]
				if aok != bok {
					done <- errors.New("half-written batch visible in snapshot")
					return
				}
				if aok && a.Score != b.Score {
					done <- errors.New("snapshot has mixed batch versions")
					return
				}
			}
		}
	}()

	for n := 0; n < 200; n++ {
		entries := make([]Entry, 0, 40)
		for i := 0; i < 20; i++ {
			entries = append(entries,
				Entry{UserID: "U" + itoa(i), Name: "a", Value: testValue(float64(n))},
				Entry{UserID: "U" + itoa(i), Name: "b", Value: testValue(float64(n))},
			)
		}
		if err := s.SetBatch(ctx, entries); err != nil {
			t.Fatalf("batch %d: %v", n, err)
		}
	}
	close(stop)
	if err := <-done; err != nil {
		t.Fatalf("reader: %v", err)
	}
}

// 磁盘故障语义：append 失败时写入返回错误、内存不更新；
// 已确认的写入在重新打开后仍在。
func TestWriteFailureIsReportedAndNotApplied(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Set(ctx, "U1", "a", testValue(1)); err != nil {
		t.Fatalf("set: %v", err)
	}
	// 模拟介质故障：直接关掉底层 WAL 文件描述符。
	if err := s.wal.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}
	if err := s.Set(ctx, "U1", "a", testValue(2)); err == nil {
		t.Fatal("expected write error after disk failure")
	}
	if v, ok, _ := s.Get(ctx, "U1", "a"); !ok || v.Score != 1 {
		t.Fatalf("failed write must not be visible: ok=%v v=%+v", ok, v)
	}
	if err := s.Close(); err != nil {
		// 关闭时对坏 fd 做 Sync 会失败；锁仍必须释放。
		t.Logf("close after failure: %v", err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	if v, ok, _ := s2.Get(ctx, "U1", "a"); !ok || v.Score != 1 {
		t.Fatalf("last confirmed write lost: ok=%v v=%+v", ok, v)
	}
}

func featureName(w, i int) string {
	return "w" + itoa(w) + "_" + itoa(i)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func mustKeys(t *testing.T, s *DurableStore) []string {
	t.Helper()
	k, err := s.Keys(context.Background())
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	return k
}

func putHeader(hdr []byte, n int, crc uint32) {
	hdr[0] = byte(n)
	hdr[1] = byte(n >> 8)
	hdr[2] = byte(n >> 16)
	hdr[3] = byte(n >> 24)
	hdr[4] = byte(crc)
	hdr[5] = byte(crc >> 8)
	hdr[6] = byte(crc >> 16)
	hdr[7] = byte(crc >> 24)
}
