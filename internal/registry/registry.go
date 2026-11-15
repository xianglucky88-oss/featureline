package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	stateFile    = "state.json"
	auditFile    = "audit.jsonl"
	lockFile     = ".lock"
	schemaV1     = 1
	initialOwner = "platform"
)

// Registry 是基于文件的特征定义注册表。
//
// 并发模型：进程内用 RWMutex 串行化所有变更；进程间用 lockFile 上的文件锁。
// 持久化模型：state.json 整体写入临时文件后 rename 原子替换；
// 每次变更再向 audit.jsonl 追加一条不可变审计记录。崩溃后以 state.json 为准恢复，
// 不会出现“发布到一半”的半成品版本。
type Registry struct {
	dir string
	mu  sync.RWMutex
	fh  *os.File
	st  State
	now func() time.Time
}

// AuditEntry 是审计链上的一条记录。
type AuditEntry struct {
	Seq      int64          `json:"seq"`
	At       time.Time      `json:"at"`
	Action   string         `json:"action"`
	Name     string         `json:"name,omitempty"`
	FQVN     string         `json:"fqvn,omitempty"`
	Revision int64          `json:"revision,omitempty"`
	Change   ChangeClass    `json:"change_class,omitempty"`
	Detail   map[string]any `json:"detail,omitempty"`
}

// Open 打开（或初始化）dir 下的注册表。dir 不存在时创建。
func Open(dir string) (*Registry, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("registry: mkdir: %w", err)
	}
	fh, err := os.OpenFile(filepath.Join(dir, lockFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("registry: lock: %w", err)
	}
	if err := flockExclusive(fh); err != nil {
		_ = fh.Close()
		return nil, fmt.Errorf("registry: another process holds the registry lock: %w", err)
	}
	r := &Registry{dir: dir, fh: fh, now: func() time.Time { return time.Now().UTC() }}
	if err := r.load(); err != nil {
		_ = unlock(fh)
		_ = fh.Close()
		return nil, err
	}
	return r, nil
}

// Bootstrap 用 SeedDefinitions 发布首批 1.0 版本（仅在空注册表时生效）。
func (r *Registry) Bootstrap() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.st.Versions) > 0 {
		return nil
	}
	now := r.now()
	for _, d := range SeedDefinitions() {
		fqvn := FQVN(d.Name, 1, 0)
		ts := now
		v := Version{
			FQVN: fqvn, Name: d.Name, Major: 1, Minor: 0,
			Status: StatusPublished, Definition: d,
			Fingerprint: Fingerprint(d), Revision: -1,
			CreatedAt: now, PublishedAt: &ts,
		}
		r.st.Versions[fqvn] = v
		r.st.Active[d.Name] = fqvn
		if err := r.appendAuditLocked("publish", d.Name, fqvn, -1, "", nil); err != nil {
			return err
		}
	}
	return r.persistLocked()
}

// Close 释放文件锁。注册表关闭不影响任何已发布版本继续服务。
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	err := unlock(r.fh)
	if cerr := r.fh.Close(); err == nil {
		err = cerr
	}
	return err
}

func (r *Registry) load() error {
	r.st = State{Versions: map[string]Version{}, Drafts: map[string]Version{}, Active: map[string]string{}}
	raw, err := os.ReadFile(filepath.Join(r.dir, stateFile))
	if errors.Is(err, os.ErrNotExist) {
		r.st.SchemaVersion = schemaV1
		return nil
	}
	if err != nil {
		return fmt.Errorf("registry: read state: %w", err)
	}
	if err := json.Unmarshal(raw, &r.st); err != nil {
		return fmt.Errorf("registry: corrupt state file: %w", err)
	}
	if err := r.checkInvariants(); err != nil {
		return err
	}
	return nil
}

// checkInvariants 是崩溃恢复与防误写的最后一道闸：
// 激活指针必须指向存在的已发布版本；草稿不得出现在 Versions 里。
func (r *Registry) checkInvariants() error {
	for name, fqvn := range r.st.Active {
		v, ok := r.st.Versions[fqvn]
		if !ok {
			return fmt.Errorf("registry: invariant broken, active %s points to missing %s", name, fqvn)
		}
		if v.Status == StatusDraft {
			return fmt.Errorf("registry: invariant broken, active %s points to draft %s", name, fqvn)
		}
	}
	for fqvn, v := range r.st.Versions {
		if v.Status == StatusDraft {
			return fmt.Errorf("registry: invariant broken, draft stored as version %s", fqvn)
		}
		if v.Fingerprint != Fingerprint(v.Definition) {
			return fmt.Errorf("registry: invariant broken, fingerprint mismatch on %s", fqvn)
		}
	}
	return nil
}

func (r *Registry) persistLocked() error {
	r.st.UpdatedAt = r.now()
	raw, err := json.MarshalIndent(&r.st, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(r.dir, stateFile+".tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("registry: write state: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(r.dir, stateFile)); err != nil {
		return fmt.Errorf("registry: replace state: %w", err)
	}
	return nil
}

func (r *Registry) appendAuditLocked(action, name, fqvn string, rev int64, class ChangeClass, detail map[string]any) error {
	entries, _ := r.countAudit()
	e := AuditEntry{
		Seq: entries + 1, At: r.now(), Action: action,
		Name: name, FQVN: fqvn, Revision: rev, Change: class, Detail: detail,
	}
	raw, err := json.Marshal(&e)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(r.dir, auditFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func (r *Registry) countAudit() (int64, error) {
	raw, err := os.ReadFile(filepath.Join(r.dir, auditFile))
	if err != nil {
		return 0, nil
	}
	var n int64
	for _, b := range raw {
		if b == '\n' {
			n++
		}
	}
	return n, nil
}

// validate 校验单个定义的自洽性。
func validate(d Definition) error {
	if !ValidName(d.Name) {
		return fmt.Errorf("%w: %q", ErrInvalidName, d.Name)
	}
	switch d.Entity {
	case EntityUser, EntityItem:
	default:
		return fmt.Errorf("%w: unknown entity %q", ErrInvalidDefinition, d.Entity)
	}
	switch d.ValueType {
	case ValueFloat64, ValueInt64, ValueString:
	default:
		return fmt.Errorf("%w: unknown value_type %q", ErrInvalidDefinition, d.ValueType)
	}
	if d.Window.Convention != WindowHalfOpenLeft {
		return fmt.Errorf("%w: window convention must be %q", ErrInvalidDefinition, WindowHalfOpenLeft)
	}
	if d.Window.SizeSeconds < 0 {
		return fmt.Errorf("%w: negative window size", ErrInvalidDefinition)
	}
	if d.Time.TimeColumn == "" || d.Time.RightBound == "" || d.Time.TimeZone == "" {
		return fmt.Errorf("%w: incomplete time semantics", ErrInvalidDefinition)
	}
	if d.Default.Type != d.ValueType {
		return fmt.Errorf("%w: default type %q != value type %q",
			ErrInvalidDefinition, d.Default.Type, d.ValueType)
	}
	if d.Default.Type != ValueString && d.Default.Value == "" {
		return fmt.Errorf("%w: empty numeric default", ErrInvalidDefinition)
	}
	return nil
}
