// DurableStore 是 Store 的本地持久化实现。
//
// 磁盘布局（一个目录即一个存储，目录内的文件由本包独占管理）：
//
//	MANIFEST                          已提交的版本（权威）
//	snapshot-<gen>.json               该代的全量基线
//	wal-<gen>.log                     该代基线之后的追加写日志
//	LOCK                              进程间互斥文件锁
//
// 设计约定（调用方可以依赖这些语义）：
//
//  1. 写入原子性。每次 Set / SetBatch 先向 WAL 追加一条完整记录并 fsync，
//     成功之后才在内存中生效。读者看不到未落盘的写入；进程在 append 中途
//     崩溃最多留下一条不完整记录，恢复时丢弃。
//
//  2. 恢复与版本选择。MANIFEST 是唯一权威：它指向的 generation 的
//     snapshot + wal 被加载，其他代文件在恢复时回收。MANIFEST 缺失时，
//     按 generation 从高到低选择第一个能完整解码的快照（用于手工删除/丢失
//     MANIFEST 的场景），并补写 MANIFEST。MANIFEST 存在但无法解析、或指向
//     的快照 CRC/大小不符，都返回 ErrCorrupt，绝不静默回退到旧代——那会
//     丢掉已经确认的写入。
//
//  3. WAL 尾部语义。文件末尾若是写了一半的记录（头不完整、payload 不完整），
//     视为未提交的残尾，截断后继续；完整记录 CRC 不符且位于文件末尾，同样
//     按残尾处理（fsync 未完成时落盘内容可能任意）；CRC 不符且其后还有字节，
//     或记录可解码但内容非法，返回 ErrCorrupt。
//
//  4. 压缩（compaction）与读路径。WAL 超过阈值后写新代快照并原子切换
//     MANIFEST。压缩只阻塞写者（writeMu），在线读取只在校验/拷贝内存 map 时
//     持有短暂读锁，不会等待磁盘重写。未完成的压缩留下的临时文件、新代文件
//     在恢复时回收。
//
//  5. 磁盘故障。append 或 fsync 失败时该次写入返回错误且内存不更新；
//     尽力把 WAL 截断回写前长度。若回滚本身也失败，存储进入“写入中毒”状态：
//     后续写入一律返回 ErrPoisoned（读取仍返回最后一次确认落盘的值），
//     避免在未知尾状态之上继续追加。
package store

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrLocked 表示存储目录已被另一个进程打开。
var ErrLocked = errors.New("store: directory is locked by another process")

// ErrCorrupt 表示磁盘上的权威状态损坏，无法安全恢复。
var ErrCorrupt = errors.New("store: corrupt durable state")

// ErrPoisoned 表示此前一次磁盘故障后 WAL 尾状态未知，写路径已被封禁。
var ErrPoisoned = errors.New("store: write path poisoned by earlier disk failure")

const (
	manifestName   = "MANIFEST"
	lockName       = "LOCK"
	manifestFormat = "featureline.store/v1"

	recordHeaderSize = 8
	// maxRecordBytes 防止损坏的长度字段导致超大分配。
	maxRecordBytes = 64 << 20

	defaultCompactionThreshold = 8 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Option 配置 DurableStore。
type Option func(*options)

type options struct {
	compactionThreshold int64
	seedSnapshot        string
	seedMode            seedMode
}

type seedMode int

const (
	// seedIfEmpty 只在存储为空时导入，避免覆盖增量写入。
	seedIfEmpty seedMode = iota
	// seedReplace 显式要求以快照为准：整份快照作为新基线写入，
	// 与快照同 key 的值被覆盖，其余已有值保留。
	seedReplace
)

// WithCompactionThreshold 设置 WAL 达到多少字节后触发压缩。
// n <= 0 表示使用默认值（8 MiB）。
func WithCompactionThreshold(n int64) Option {
	return func(o *options) { o.compactionThreshold = n }
}

// WithSeedSnapshot 指定一份与旧版 JSON 快照格式相同的文件。
// 仅当存储中还没有任何键时导入（首次启动或清空后的迁移），
// 已有数据时忽略，避免覆盖增量写入。需要强制刷新时用 WithSnapshotReplace。
func WithSeedSnapshot(path string) Option {
	return func(o *options) { o.seedSnapshot = path; o.seedMode = seedIfEmpty }
}

// WithSnapshotReplace 把快照作为权威基线导入，即使存储已有数据。
// 用于离线批处理重新产出快照后让服务端生效；导入是单个原子操作
// （新代快照 + MANIFEST 切换），不会让读者看到半份快照。
func WithSnapshotReplace(path string) Option {
	return func(o *options) { o.seedSnapshot = path; o.seedMode = seedReplace }
}

// observed 是只在写路径更新的观测快照，整体原子替换。
type observed struct {
	generation        uint64
	walBytes          int64
	walRecords        uint64
	compactions       uint64
	compactionErr     string
	tornTailRecovered bool
	seedImported      bool
}

// DurableStats 是 DurableStore 的运行状态，供 /debug 使用。
type DurableStats struct {
	Keys            int    `json:"keys"`
	Online          int    `json:"online"`
	Offline         int    `json:"offline"`
	Generation      uint64 `json:"generation"`
	WALBytes        int64  `json:"wal_bytes"`
	WALRecords      uint64 `json:"wal_records"`
	Compactions     uint64 `json:"compactions"`
	CompactionError string `json:"compaction_error,omitempty"`
	TornTail        bool   `json:"torn_tail_recovered,omitempty"`
	SeedImported    bool   `json:"seed_imported,omitempty"`
}

// DurableStore 是带崩溃恢复能力的本地特征存储。
type DurableStore struct {
	dir  string
	lock *fileLock

	// writeMu 串行化 WAL 追加、压缩与关闭。它不参与在线读取。
	writeMu sync.Mutex
	// poisoned 非 nil 时写路径不可用（仅由 writeMu 保护）。
	poisoned error

	// mu 保护 data。读路径只取 RLock。
	mu   sync.RWMutex
	data map[string]Value

	gen       uint64
	wal       *os.File
	walPath   string
	walBytes  int64
	threshold int64

	walRecords     uint64
	compactions    uint64
	compactRetryAt int64

	// meta 是观测计数器的原子快照，由写路径在持有 writeMu 时发布。
	// Stats 只读它，不需要 writeMu——压缩期间 /debug 不会被阻塞。
	meta atomic.Pointer[observed]

	closed atomic.Bool
}

// Open 打开或初始化一个持久化存储目录。目录会被进程级文件锁独占。
func Open(dir string, opts ...Option) (*DurableStore, error) {
	o := options{compactionThreshold: defaultCompactionThreshold}
	for _, opt := range opts {
		opt(&o)
	}
	if dir == "" {
		return nil, errors.New("store: empty directory")
	}
	if o.compactionThreshold <= 0 {
		o.compactionThreshold = defaultCompactionThreshold
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create directory %s: %w", dir, err)
	}
	lk, err := acquireLock(filepath.Join(dir, lockName))
	if err != nil {
		return nil, err
	}
	s := &DurableStore{
		dir:       dir,
		lock:      lk,
		data:      map[string]Value{},
		threshold: o.compactionThreshold,
	}
	if err := s.recover(o); err != nil {
		_ = lk.release()
		return nil, err
	}
	return s, nil
}

// ---------- 恢复 ----------

type generation struct {
	gen      uint64
	snapshot string
	wal      string
	crc      uint32
	size     int64
}

type manifest struct {
	Format        string `json:"format"`
	Generation    uint64 `json:"generation"`
	Snapshot      string `json:"snapshot"`
	SnapshotCRC32 uint32 `json:"snapshot_crc32"`
	SnapshotBytes int64  `json:"snapshot_bytes"`
	WAL           string `json:"wal"`
}

func (s *DurableStore) recover(o options) error {
	seedPath := o.seedSnapshot
	if err := removeTempFiles(s.dir); err != nil {
		return err
	}

	g, regenerated, err := chooseGeneration(s.dir)
	if err != nil {
		return err
	}

	seedImported := false
	bootstrapped := false
	if g == nil {
		seed, err := readSeed(seedPath)
		if err != nil {
			return err
		}
		// 走到这里意味着此前的中断初始化留下了不完整文件，且没有任何
		// 已确认写入（见 chooseGeneration）。清掉残留，从第 1 代重新开始，
		// 避免 bootstrap 追加到旧 WAL 或撞上已存在的文件名。
		if err := removeAllGenerations(s.dir); err != nil {
			return err
		}
		if err := bootstrap(s.dir, seed); err != nil {
			return err
		}
		seedImported = len(seed) > 0
		bootstrapped = true
		g, _, err = chooseGeneration(s.dir)
		if err != nil {
			return err
		}
		if g == nil {
			return fmt.Errorf("%w: bootstrap produced no generation", ErrCorrupt)
		}
	}

	data, err := loadSnapshot(s.dir, g)
	if err != nil {
		return err
	}

	entries, validEnd, torn, err := replayWAL(filepath.Join(s.dir, g.wal))
	if err != nil {
		return err
	}
	for _, e := range entries {
		data[e.key] = e.value
	}
	if torn {
		if err := truncateFile(filepath.Join(s.dir, g.wal), validEnd); err != nil {
			return fmt.Errorf("store: truncate torn wal tail: %w", err)
		}
		if err := syncDir(s.dir); err != nil {
			return fmt.Errorf("store: sync directory after wal truncate: %w", err)
		}
	}

	if regenerated {
		// MANIFEST 丢失但数据代完整：把选中的代重新记为权威。
		m := manifest{
			Format:        manifestFormat,
			Generation:    g.gen,
			Snapshot:      g.snapshot,
			SnapshotCRC32: g.crc,
			SnapshotBytes: g.size,
			WAL:           g.wal,
		}
		if _, err := writeManifest(s.dir, m); err != nil {
			return err
		}
	}

	wal, err := openWAL(filepath.Join(s.dir, g.wal))
	if err != nil {
		return err
	}
	// 从这里到 Open 成功返回之间若出错，必须自己关掉当前 WAL，
	// 否则 fd 泄漏且目录锁释放后文件仍被占用。
	// 注意压缩会替换 s.wal，所以关闭时要读字段而不是闭包里的 wal。
	recovered := false
	defer func() {
		if !recovered && s.wal != nil {
			_ = s.wal.Close()
		}
	}()

	s.data = data
	s.gen = g.gen
	s.wal = wal
	s.walPath = filepath.Join(s.dir, g.wal)
	s.walBytes = validEnd
	s.walRecords = uint64(len(entries))

	if err := removeOtherGenerations(s.dir, g.gen); err != nil {
		return err
	}

	// 迁移：WithSeedSnapshot 只在存储为空时导入；
	// WithSnapshotReplace 无条件以快照为基线。
	// 刚刚 bootstrap 的全新存储已经把快照写进第 1 代基线，
	// 再压缩一次只会白白多写一代，因此跳过。
	if !bootstrapped && seedPath != "" && (len(data) == 0 || o.seedMode == seedReplace) {
		seed, err := readSeed(seedPath)
		if err != nil {
			return err
		}
		if len(seed) > 0 {
			merged := seed
			if o.seedMode == seedReplace {
				// “快照为准、其余保留”：同一 key 用快照值，
				// 快照未覆盖的已有值（例如在线写入）继续保留。
				merged = make(map[string]Value, len(data)+len(seed))
				for k, v := range data {
					merged[k] = v
				}
				for k, v := range seed {
					merged[k] = v
				}
			}
			// recover 期间尚无并发访问，这里显式加锁是为了遵守
			// compactTo 的“调用方持有 writeMu”约定，而不是依赖隐式前提。
			s.writeMu.Lock()
			err := s.compactTo(merged)
			s.writeMu.Unlock()
			if err != nil {
				return fmt.Errorf("store: import seed snapshot: %w", err)
			}
			s.data = merged
			seedImported = true
		}
	}

	s.publishRecovered(torn, seedImported)
	recovered = true
	return nil
}

// chooseGeneration 实现恢复时的版本选择。
func chooseGeneration(dir string) (*generation, bool, error) {
	raw, err := os.ReadFile(filepath.Join(dir, manifestName))
	switch {
	case err == nil:
		var m manifest
		if jerr := json.Unmarshal(raw, &m); jerr != nil {
			return nil, false, fmt.Errorf("%w: manifest: %v", ErrCorrupt, jerr)
		}
		if m.Format != manifestFormat || m.Generation == 0 || m.Snapshot == "" || m.WAL == "" {
			return nil, false, fmt.Errorf("%w: manifest has invalid fields", ErrCorrupt)
		}
		if _, serr := os.Stat(filepath.Join(dir, m.Snapshot)); serr != nil {
			return nil, false, fmt.Errorf("%w: manifest snapshot %s: %v", ErrCorrupt, m.Snapshot, serr)
		}
		if _, serr := os.Stat(filepath.Join(dir, m.WAL)); serr != nil {
			return nil, false, fmt.Errorf("%w: manifest wal %s: %v", ErrCorrupt, m.WAL, serr)
		}
		return &generation{
			gen:      m.Generation,
			snapshot: m.Snapshot,
			wal:      m.WAL,
			crc:      m.SnapshotCRC32,
			size:     m.SnapshotBytes,
		}, false, nil
	case os.IsNotExist(err):
		// 没有 MANIFEST：从现存文件中选最高且自洽的一代。
	default:
		return nil, false, fmt.Errorf("store: read manifest: %w", err)
	}

	gens, err := listGenerations(dir)
	if err != nil {
		return nil, false, err
	}
	for i := len(gens) - 1; i >= 0; i-- {
		g := gens[i]
		if g.snapshot == "" || g.wal == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, g.snapshot)); err != nil {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, g.wal)); err != nil {
			continue
		}
		// 快照能解码才算有效候选；CRC 没有记录，校验交给 loadSnapshot。
		if _, err := decodeSnapshotFile(filepath.Join(dir, g.snapshot)); err != nil {
			continue
		}
		st, err := os.Stat(filepath.Join(dir, g.snapshot))
		if err != nil {
			continue
		}
		g.size = st.Size()
		g.crc = checksumFile(filepath.Join(dir, g.snapshot))
		return &g, true, nil
	}
	if len(gens) > 0 {
		// 没有 MANIFEST、也没有完整可解码的代。只有“从未存在已确认写入”
		// 时才允许当作全新存储。判据必须可靠：写入的提交顺序是
		// WAL fsync -> 内存可见，而 MANIFEST 只由 bootstrap/compaction 写；
		// 所以 WAL 里出现有效记录，就意味着可能有已确认写入，必须报错。
		pending, err := anyAcknowledgedWALRecords(dir)
		if err != nil {
			return nil, false, err
		}
		if !pending {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("%w: no decodable generation among %d candidates", ErrCorrupt, len(gens))
	}
	return nil, false, nil // 空目录，全新存储
}

func listGenerations(dir string) ([]generation, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("store: list directory: %w", err)
	}
	byGen := map[uint64]*generation{}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		gen, kind, ok := parseGenerationName(e.Name())
		if !ok {
			continue
		}
		g := byGen[gen]
		if g == nil {
			g = &generation{gen: gen}
			byGen[gen] = g
		}
		if kind == "snapshot" {
			g.snapshot = e.Name()
		} else {
			g.wal = e.Name()
		}
	}
	out := make([]generation, 0, len(byGen))
	for _, g := range byGen {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].gen < out[j].gen })
	return out, nil
}

// anyAcknowledgedWALRecords 判断目录里是否可能已有“已确认写入”。
// 依据：任何一次成功写入都会先留下 MANIFEST 指向的 WAL 记录；
// 因此只要某个 WAL 能解析出至少一条有效记录，就不能当作全新存储。
func anyAcknowledgedWALRecords(dir string) (bool, error) {
	gens, err := listGenerations(dir)
	if err != nil {
		return false, err
	}
	for _, g := range gens {
		if g.wal == "" {
			continue
		}
		entries, _, _, err := replayWAL(filepath.Join(dir, g.wal))
		if err != nil {
			// WAL 本身损坏：保守地视为存在已确认写入。
			return true, nil
		}
		if len(entries) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func parseGenerationName(name string) (uint64, string, bool) {
	var kind, rest string
	switch {
	case strings.HasPrefix(name, "snapshot-") && strings.HasSuffix(name, ".json"):
		kind, rest = "snapshot", strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), ".json")
	case strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log"):
		kind, rest = "wal", strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log")
	default:
		return 0, "", false
	}
	gen, err := strconv.ParseUint(rest, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return gen, kind, true
}

func snapshotName(gen uint64) string { return fmt.Sprintf("snapshot-%016d.json", gen) }
func walName(gen uint64) string      { return fmt.Sprintf("wal-%016d.log", gen) }

// ---------- 快照读写 ----------

func loadSnapshot(dir string, g *generation) (map[string]Value, error) {
	path := filepath.Join(dir, g.snapshot)
	data, err := decodeSnapshotFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCorrupt, g.snapshot, err)
	}
	if g.crc != 0 || g.size != 0 {
		st, serr := os.Stat(path)
		if serr != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrCorrupt, g.snapshot, serr)
		}
		if g.size != 0 && st.Size() != g.size {
			return nil, fmt.Errorf("%w: %s size %d != manifest %d", ErrCorrupt, g.snapshot, st.Size(), g.size)
		}
		if g.crc != 0 && checksumFile(path) != g.crc {
			return nil, fmt.Errorf("%w: %s checksum mismatch", ErrCorrupt, g.snapshot)
		}
	}
	return data, nil
}

func decodeSnapshotFile(path string) (map[string]Value, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var wire map[string]valueJSON
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	out := make(map[string]Value, len(wire))
	for k, wv := range wire {
		v, err := wv.toValue()
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

// writeSnapshot 以“临时文件 + rename + 目录 fsync”的方式原子写入一份快照，
// 返回内容 CRC 与字节数。
func writeSnapshot(dir string, gen uint64, data map[string]Value) (uint32, int64, error) {
	final := filepath.Join(dir, snapshotName(gen))
	tmp := filepath.Join(dir, "."+snapshotName(gen)+".tmp")

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, 0, err
	}
	h := crc32.New(crcTable)
	w := bufio.NewWriter(io.MultiWriter(f, h))
	if err := encodeSnapshot(w, data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return 0, 0, err
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return 0, 0, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return 0, 0, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return 0, 0, err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return 0, 0, err
	}
	if err := syncDir(dir); err != nil {
		return 0, 0, err
	}
	st, err := os.Stat(final)
	if err != nil {
		return 0, 0, err
	}
	return h.Sum32(), st.Size(), nil
}

func encodeSnapshot(w io.Writer, data map[string]Value) error {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if _, err := io.WriteString(w, "{"); err != nil {
		return err
	}
	for i, k := range keys {
		if i > 0 {
			if _, err := io.WriteString(w, ",\n  "); err != nil {
				return err
			}
		} else if _, err := io.WriteString(w, "\n  "); err != nil {
			return err
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return err
		}
		if _, err := w.Write(kb); err != nil {
			return err
		}
		if _, err := io.WriteString(w, ": "); err != nil {
			return err
		}
		vb, err := json.Marshal(toValueJSON(data[k]))
		if err != nil {
			return err
		}
		if _, err := w.Write(vb); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "\n}\n")
	return err
}

func checksumFile(path string) uint32 {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return crc32.Checksum(b, crcTable)
}

// ---------- WAL ----------

type walEntryJSON struct {
	Key   string    `json:"key"`
	Value valueJSON `json:"value"`
}

type walRecordJSON struct {
	Entries []walEntryJSON `json:"entries"`
}

type appliedEntry struct {
	key   string
	value Value
}

// appendRecord 追加一条完整记录并 fsync。失败时尽力回滚到写前长度。
func (s *DurableStore) appendRecord(payload []byte) error {
	if int64(len(payload)) > maxRecordBytes {
		return fmt.Errorf("store: record too large (%d bytes)", len(payload))
	}
	start, err := s.wal.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	hdr := make([]byte, recordHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, crcTable))
	buf := make([]byte, 0, len(hdr)+len(payload))
	buf = append(buf, hdr...)
	buf = append(buf, payload...)
	if _, err := s.wal.Write(buf); err != nil {
		return s.rollback(start, err)
	}
	if err := s.wal.Sync(); err != nil {
		return s.rollback(start, err)
	}
	return nil
}

func (s *DurableStore) rollback(start int64, cause error) error {
	if err := s.wal.Truncate(start); err != nil {
		s.poisoned = fmt.Errorf("%w: rollback after %v: %v", ErrPoisoned, cause, err)
		return s.poisoned
	}
	if err := s.wal.Sync(); err != nil {
		s.poisoned = fmt.Errorf("%w: rollback sync after %v: %v", ErrPoisoned, cause, err)
		return s.poisoned
	}
	return fmt.Errorf("store: wal append: %w", cause)
}

func openWAL(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("store: open wal: %w", err)
	}
	return f, nil
}

// replayWAL 解析 WAL。返回有效前缀内的记录、有效字节长度以及是否丢弃了残尾。
func replayWAL(path string) ([]appliedEntry, int64, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, false, fmt.Errorf("store: read wal: %w", err)
	}
	var out []appliedEntry
	pos := int64(0)
	for pos < int64(len(raw)) {
		remain := int64(len(raw)) - pos
		if remain < recordHeaderSize {
			return out, pos, true, nil
		}
		n := int64(binary.LittleEndian.Uint32(raw[pos : pos+4]))
		crc := binary.LittleEndian.Uint32(raw[pos+4 : pos+8])
		if n > maxRecordBytes {
			return nil, 0, false, fmt.Errorf("%w: wal record length %d exceeds limit", ErrCorrupt, n)
		}
		if remain < recordHeaderSize+n {
			// 头完整但 payload 不完整：写到一半崩溃。
			return out, pos, true, nil
		}
		payload := raw[pos+recordHeaderSize : pos+recordHeaderSize+n]
		if crc32.Checksum(payload, crcTable) != crc {
			if pos+recordHeaderSize+n == int64(len(raw)) {
				// 末尾完整长度的记录 CRC 不符：fsync 未完成留下的撕裂块。
				return out, pos, true, nil
			}
			return nil, 0, false, fmt.Errorf("%w: wal checksum mismatch at offset %d", ErrCorrupt, pos)
		}
		var rec walRecordJSON
		if err := json.Unmarshal(payload, &rec); err != nil {
			return nil, 0, false, fmt.Errorf("%w: wal record at offset %d: %v", ErrCorrupt, pos, err)
		}
		for _, e := range rec.Entries {
			v, err := e.Value.toValue()
			if err != nil {
				return nil, 0, false, fmt.Errorf("%w: wal value at offset %d: %v", ErrCorrupt, pos, err)
			}
			out = append(out, appliedEntry{key: e.Key, value: v})
		}
		pos += recordHeaderSize + n
	}
	return out, pos, false, nil
}

func truncateFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
}

// ---------- MANIFEST 与回收 ----------

// writeManifest 原子替换 MANIFEST。
//
// 返回值 committed 表示 rename 已经执行：此时 MANIFEST 在文件系统命名空间
// 里已指向新内容，即使随后的目录 fsync 失败，也不能再当作“未提交”处理
// （删除它指向的新代文件会让下次启动面对悬空引用）。调用方应据此区分
// “绝对没提交”与“已提交但耐久性未知”。
func writeManifest(dir string, m manifest) (committed bool, err error) {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return false, err
	}
	raw = append(raw, '\n')
	tmp := filepath.Join(dir, manifestName+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return false, fmt.Errorf("store: write manifest: %w", err)
	}
	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return false, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return false, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return false, err
	}
	if err := os.Rename(tmp, filepath.Join(dir, manifestName)); err != nil {
		_ = os.Remove(tmp)
		return false, err
	}
	if err := syncDir(dir); err != nil {
		return true, err
	}
	return true, nil
}

// syncDir 让 rename/删除等目录变更持久化。
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func removeTempFiles(dir string) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".tmp") {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

// removeOtherGenerations 回收非当前代的快照/WAL，以及未提交的更高代残留。
func removeOtherGenerations(dir string, keep uint64) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		gen, _, ok := parseGenerationName(e.Name())
		if !ok || gen == keep {
			continue
		}
		_ = os.Remove(filepath.Join(dir, e.Name()))
	}
	return syncDir(dir)
}

// removeAllGenerations 用于继续一次中断的全新初始化：此时不存在任何
// 已确认写入，残留的快照/WAL 都可以安全删除。
func removeAllGenerations(dir string) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		if _, _, ok := parseGenerationName(e.Name()); ok {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return syncDir(dir)
}

// ---------- 初始化 / 压缩 ----------

func bootstrap(dir string, seed map[string]Value) error {
	const gen = uint64(1)
	if seed == nil {
		seed = map[string]Value{}
	}
	crc, size, err := writeSnapshot(dir, gen, seed)
	if err != nil {
		return fmt.Errorf("store: write initial snapshot: %w", err)
	}
	path := filepath.Join(dir, walName(gen))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("store: create wal: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	_, err = writeManifest(dir, manifest{
		Format:        manifestFormat,
		Generation:    gen,
		Snapshot:      snapshotName(gen),
		SnapshotCRC32: crc,
		SnapshotBytes: size,
		WAL:           walName(gen),
	})
	return err
}

// compactTo 写入新一代基线并原子切换 MANIFEST，然后回收旧代。
// 调用方必须持有 writeMu（Open 期间尚无并发，等价于独占）。
//
// 关键顺序：新快照与新 WAL 都 fsync 且目录 fsync 之后才写 MANIFEST。
// 在此之前崩溃，磁盘上只会多出一组“未被 MANIFEST 引用”的文件，
// 恢复时按未提交内容回收，已提交的旧代不受影响。
func (s *DurableStore) compactTo(data map[string]Value) error {
	newGen := s.gen + 1
	crc, size, err := writeSnapshot(s.dir, newGen, data)
	if err != nil {
		return err
	}
	newWalPath := filepath.Join(s.dir, walName(newGen))
	nw, err := os.OpenFile(newWalPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		_ = os.Remove(filepath.Join(s.dir, snapshotName(newGen)))
		return err
	}
	if err := nw.Sync(); err != nil {
		_ = nw.Close()
		_ = os.Remove(newWalPath)
		return err
	}
	if err := syncDir(s.dir); err != nil {
		_ = nw.Close()
		_ = os.Remove(newWalPath)
		return err
	}
	m := manifest{
		Format:        manifestFormat,
		Generation:    newGen,
		Snapshot:      snapshotName(newGen),
		SnapshotCRC32: crc,
		SnapshotBytes: size,
		WAL:           walName(newGen),
	}
	if committed, err := writeManifest(s.dir, m); err != nil {
		if !committed {
			// MANIFEST 未切换：新代从未被引用，可以安全删除。
			_ = nw.Close()
			_ = os.Remove(newWalPath)
			_ = os.Remove(filepath.Join(s.dir, snapshotName(newGen)))
			return err
		}
		// MANIFEST 已在命名空间里指向新代，只是目录 fsync 失败
		// （掉电后可能回退到旧 MANIFEST）。此时不能删除新代文件：
		// 否则一旦 fsync 实际生效，下次启动就会指向不存在的文件。
		// 按“已提交”继续使用新代；耐久性由下一次成功的目录 fsync 弥补。
	}

	// ---- 提交点：此后 MANIFEST 指向新一代 ----
	old := s.wal
	oldGen := s.gen
	s.wal = nw
	s.walPath = newWalPath
	s.gen = newGen
	s.walBytes = 0
	s.walRecords = 0
	s.compactions++
	s.compactRetryAt = 0
	if old != nil {
		_ = old.Close()
	}
	if oldGen > 0 {
		_ = removeOtherGenerations(s.dir, newGen)
	}
	s.publishLocked("")
	return nil
}

// compactLocked 在持有 writeMu 时按当前数据做一次压缩。
func (s *DurableStore) compactLocked(reason string) error {
	s.mu.RLock()
	data := make(map[string]Value, len(s.data))
	for k, v := range s.data {
		data[k] = v
	}
	s.mu.RUnlock()

	if err := s.compactTo(data); err != nil {
		s.compactRetryAt = s.walBytes + s.threshold
		s.publishLocked(err.Error())
		return fmt.Errorf("store: compaction (%s): %w", reason, err)
	}
	return nil
}

// Compact 主动把 WAL 合并进基线。失败时数据仍安全（WAL 完整），
// 只是继续保留较大的 WAL。
func (s *DurableStore) Compact() error {
	if s.closed.Load() {
		return ErrClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.poisoned != nil {
		return s.poisoned
	}
	if s.walBytes == 0 {
		return nil
	}
	return s.compactLocked("manual")
}

// ---------- 读 ----------

func (s *DurableStore) Get(_ context.Context, userID, name string) (Value, bool, error) {
	if s.closed.Load() {
		return Value{}, false, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key(userID, name)]
	return v, ok, nil
}

// MGet 在一次读锁内取同一用户的一组特征，结果来自同一个一致的内存视图。
func (s *DurableStore) MGet(_ context.Context, userID string, names []string) (map[string]Value, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Value, len(names))
	for _, n := range names {
		if v, ok := s.data[key(userID, n)]; ok {
			out[n] = v
		}
	}
	return out, nil
}

// Snapshot 返回全部特征在某个时间点的一致副本（批量读取）。
func (s *DurableStore) Snapshot(_ context.Context) (map[string]Value, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Value, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out, nil
}

func (s *DurableStore) Keys(_ context.Context) ([]string, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.data))
	for k := range s.data {
		out = append(out, k)
	}
	return out, nil
}

// ---------- 写 ----------

func (s *DurableStore) Set(ctx context.Context, userID, name string, v Value) error {
	return s.SetBatch(ctx, []Entry{{UserID: userID, Name: name, Value: v}})
}

// SetBatch 原子地写入一组特征值：单条 WAL 记录 + 一次 fsync，
// 读者只可能看到整批写入前后的状态，不会看到中间状态。
func (s *DurableStore) SetBatch(ctx context.Context, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if s.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	if s.poisoned != nil {
		return s.poisoned
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	rec := walRecordJSON{Entries: make([]walEntryJSON, 0, len(entries))}
	for _, e := range entries {
		rec.Entries = append(rec.Entries, walEntryJSON{
			Key:   key(e.UserID, e.Name),
			Value: toValueJSON(e.Value),
		})
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("store: encode wal record: %w", err)
	}
	if err := s.appendRecord(payload); err != nil {
		return err
	}

	// fsync 已成功，此后才对读者可见。
	s.mu.Lock()
	for _, e := range entries {
		s.data[key(e.UserID, e.Name)] = e.Value
	}
	s.mu.Unlock()

	s.walBytes += recordHeaderSize + int64(len(payload))
	s.walRecords++
	s.publishLocked(s.currentObserved().compactionErr)

	// 压缩只阻塞写者；在线读取不受影响。
	if s.walBytes >= s.threshold && s.walBytes >= s.compactRetryAt {
		_ = s.compactLocked("threshold")
	}
	return nil
}

// Close 同步并关闭 WAL、释放目录锁。可重复调用。
func (s *DurableStore) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var first error
	if s.wal != nil {
		// 此前一次写失败可能已经把 WAL 关掉；此时 Sync 返回的是
		// “已关闭”而不是新的磁盘故障，不重复上报。
		if err := s.wal.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
			first = err
		}
		if err := s.wal.Close(); err != nil && !errors.Is(err, os.ErrClosed) && first == nil {
			first = err
		}
		s.wal = nil
	}
	if s.lock != nil {
		if err := s.lock.release(); err != nil && first == nil {
			first = err
		}
		s.lock = nil
	}
	return first
}

// Stats 返回运行状态。它只读原子快照与数据锁，不与写路径/压缩竞争 writeMu。
func (s *DurableStore) Stats() DurableStats {
	s.mu.RLock()
	keys, online, offline := len(s.data), 0, 0
	for _, v := range s.data {
		if v.Source == "online" {
			online++
		} else {
			offline++
		}
	}
	s.mu.RUnlock()

	o := s.currentObserved()
	return DurableStats{
		Keys:            keys,
		Online:          online,
		Offline:         offline,
		Generation:      o.generation,
		WALBytes:        o.walBytes,
		WALRecords:      o.walRecords,
		Compactions:     o.compactions,
		CompactionError: o.compactionErr,
		TornTail:        o.tornTailRecovered,
		SeedImported:    o.seedImported,
	}
}

// currentObserved 读取已发布的观测快照（无锁）。
func (s *DurableStore) currentObserved() observed {
	if o := s.meta.Load(); o != nil {
		return *o
	}
	return observed{}
}

// publishLocked 发布计数器；恢复期才变化的字段沿用上一次快照。
// 调用方必须持有 writeMu。
func (s *DurableStore) publishLocked(compactionErr string) {
	prev := s.currentObserved()
	s.publishMeta(observed{
		generation:        s.gen,
		walBytes:          s.walBytes,
		walRecords:        s.walRecords,
		compactions:       s.compactions,
		compactionErr:     compactionErr,
		tornTailRecovered: prev.tornTailRecovered,
		seedImported:      prev.seedImported,
	})
}

// publishRecovered 在恢复结束时发布一次观测快照。
func (s *DurableStore) publishRecovered(torn, seedImported bool) {
	s.publishMeta(observed{
		generation:        s.gen,
		walBytes:          s.walBytes,
		walRecords:        s.walRecords,
		compactions:       s.compactions,
		tornTailRecovered: torn,
		seedImported:      seedImported,
	})
}

func (s *DurableStore) publishMeta(o observed) {
	cp := o
	s.meta.Store(&cp)
}

// ---------- 值编码 ----------
//
// encoding/json 不支持 NaN/Inf。旧实现直接 json.Marshal 整个快照，
// 遇到 NaN 会静默失败（整个落盘被跳过）。这里把非有限值编码成字符串，
// 保证任何 Value 都能被持久化且可无损读回。
type valueJSON struct {
	Score     any       `json:"score"`
	UpdatedAt time.Time `json:"updated_at"`
	Source    string    `json:"source"`
}

func toValueJSON(v Value) valueJSON {
	var score any = v.Score
	switch {
	case math.IsNaN(v.Score):
		score = "NaN"
	case math.IsInf(v.Score, 1):
		score = "+Inf"
	case math.IsInf(v.Score, -1):
		score = "-Inf"
	}
	return valueJSON{Score: score, UpdatedAt: v.UpdatedAt, Source: v.Source}
}

func (w valueJSON) toValue() (Value, error) {
	v := Value{UpdatedAt: w.UpdatedAt, Source: w.Source}
	switch x := w.Score.(type) {
	case float64:
		v.Score = x
	case string:
		switch x {
		case "NaN":
			v.Score = math.NaN()
		case "+Inf":
			v.Score = math.Inf(1)
		case "-Inf":
			v.Score = math.Inf(-1)
		default:
			return Value{}, fmt.Errorf("invalid score %q", x)
		}
	case nil:
		return Value{}, errors.New("null score")
	default:
		return Value{}, fmt.Errorf("invalid score type %T", w.Score)
	}
	return v, nil
}

// readSeed 读取旧版 JSON 快照；文件不存在或 path 为空时返回 nil。
func readSeed(path string) (map[string]Value, error) {
	if path == "" {
		return nil, nil
	}
	data, err := decodeSnapshotFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read seed snapshot %s: %w", path, err)
	}
	return data, nil
}

// 保证接口实现（编译期检查）。
var _ Store = (*DurableStore)(nil)
var _ BatchWriter = (*DurableStore)(nil)
