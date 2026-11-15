package registry

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Bind 基于一组 FQVN 生成不可变绑定清单。
//
// 规则：
//   - 每个 FQVN 必须存在且已发布；弃用版本不能进入新绑定（ErrDeprecated），
//     消费者应改用 Successor，这正是“可验证的弃用迁移规则”；
//   - 清单里记录的是 FQVN + 指纹双重坐标，后续解析时任一对不上就失败，
//     杜绝同名版本内容被调换；
//   - 绑定本身带指纹并落盘，训练任务、特征快照、在线实例各自持有同一份清单，
//     即完成“同一份已发布定义”的三方固定。
func (r *Registry) Bind(fqvnList []string, asOf *time.Time) (Binding, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := map[string]bool{}
	entries := make([]BindingEntry, 0, len(fqvnList))
	for _, fqvn := range fqvnList {
		if seen[fqvn] {
			continue
		}
		seen[fqvn] = true
		v, ok := r.st.Versions[fqvn]
		if !ok {
			return Binding{}, fmt.Errorf("%w: %s", ErrNotFound, fqvn)
		}
		if v.Status == StatusDeprecated {
			return Binding{}, fmt.Errorf("%w: %s (successor=%s)",
				ErrDeprecated, fqvn, v.Successor)
		}
		entries = append(entries, BindingEntry{
			Name: v.Name, FQVN: fqvn, Fingerprint: v.Fingerprint,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })

	id, err := newManifestID()
	if err != nil {
		return Binding{}, err
	}
	return Binding{
		ManifestID:  id,
		Fingerprint: BindingFingerprint(id, entries),
		AsOf:        asOf,
		Entries:     entries,
		CreatedAt:   r.now(),
	}, nil
}

// BindActive 为当前全部激活版本生成绑定（新训练任务的便捷入口）。
func (r *Registry) BindActive(asOf *time.Time) (Binding, error) {
	active := r.ListActive()
	fqvns := make([]string, 0, len(active))
	for _, v := range active {
		fqvns = append(fqvns, v.FQVN)
	}
	return r.Bind(fqvns, asOf)
}

// Resolve 校验绑定能否在当前注册表上完整解析，并返回对应的版本集合。
//
// 旧模型升级滞后时，它绑定的旧 major 依然能解析（版本永不删除）；
// 一旦某个 FQVN 缺失或指纹变化，返回 ErrBindingStale——
// 调用方必须显式处理，而不是静默读到另一套语义的数值。
func (r *Registry) Resolve(b Binding) (map[string]Version, error) {
	if err := ValidateBinding(b); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]Version, len(b.Entries))
	for _, e := range b.Entries {
		v, ok := r.st.Versions[e.FQVN]
		if !ok {
			return nil, fmt.Errorf("%w: %s no longer exists", ErrBindingStale, e.FQVN)
		}
		if v.Fingerprint != e.Fingerprint {
			return nil, fmt.Errorf("%w: %s fingerprint drift", ErrBindingStale, e.FQVN)
		}
		out[e.Name] = v
	}
	return out, nil
}

// ValidateBinding 校验绑定清单：自身指纹（防手改）、名称唯一、FQVN 自洽。
func ValidateBinding(b Binding) error {
	if b.Fingerprint != BindingFingerprint(b.ManifestID, b.Entries) {
		return fmt.Errorf("%w: binding manifest fingerprint mismatch", ErrBindingStale)
	}
	names := map[string]bool{}
	for _, e := range b.Entries {
		name, _, _, ok := ParseFQVN(e.FQVN)
		if !ok || name != e.Name || e.Fingerprint == "" {
			return fmt.Errorf("%w: malformed entry %+v", ErrBindingStale, e)
		}
		if names[e.Name] {
			return fmt.Errorf("%w: duplicated name %s", ErrBindingStale, e.Name)
		}
		names[e.Name] = true
	}
	return nil
}

// WriteBinding 把绑定清单原子写到 path。
func WriteBinding(path string, b Binding) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadBinding 读取绑定清单并做完整性校验。
func ReadBinding(path string) (Binding, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Binding{}, err
	}
	var b Binding
	if err := json.Unmarshal(raw, &b); err != nil {
		return Binding{}, fmt.Errorf("binding: corrupt %s: %w", path, err)
	}
	if err := ValidateBinding(b); err != nil {
		return Binding{}, err
	}
	return b, nil
}

func (r *Registry) readAudit() ([]AuditEntry, error) {
	raw, err := os.ReadFile(filepath.Join(r.dir, auditFile))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []AuditEntry
	for i, line := range splitLines(raw) {
		if len(line) == 0 {
			continue
		}
		var e AuditEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("audit: corrupt line %d: %w", i+1, err)
		}
		out = append(out, e)
	}
	return out, nil
}

func splitLines(raw []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, b := range raw {
		if b == '\n' {
			out = append(out, raw[start:i])
			start = i + 1
		}
	}
	if start < len(raw) {
		out = append(out, raw[start:])
	}
	return out
}

func newManifestID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "bm-" + hex.EncodeToString(b[:]), nil
}
