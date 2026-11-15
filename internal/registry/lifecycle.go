package registry

import (
	"fmt"
	"sort"
	"time"
)

// SaveDraft 新建或更新同名定义的草稿。
//
// expectRevision 是乐观锁：首次传 0；之后必须回传当前草稿的 revision。
// 并发的第二方提交会收到 ErrConflict，需要重新拉取草稿、合并后再提交，
// 因此同名定义的并发修改不会互相静默覆盖。
func (r *Registry) SaveDraft(d Definition, expectRevision int64, changeNote string) (Version, error) {
	if err := validate(d); err != nil {
		return Version{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if cur, ok := r.st.Drafts[d.Name]; ok {
		if cur.Revision != expectRevision {
			return Version{}, fmt.Errorf("%w: name=%s expect=%d actual=%d",
				ErrConflict, d.Name, expectRevision, cur.Revision)
		}
	} else if expectRevision != 0 {
		return Version{}, fmt.Errorf("%w: draft of %s vanished", ErrConflict, d.Name)
	}

	basedOn := r.st.Active[d.Name]
	v := Version{
		FQVN:        "",
		Name:        d.Name,
		Status:      StatusDraft,
		Definition:  d,
		Fingerprint: Fingerprint(d),
		BasedOn:     basedOn,
		ChangeNote:  changeNote,
		Revision:    expectRevision + 1,
		CreatedAt:   r.now(),
	}
	r.st.Drafts[d.Name] = v
	if err := r.appendAuditLocked("save_draft", d.Name, "", v.Revision, "",
		map[string]any{"based_on": basedOn}); err != nil {
		return Version{}, err
	}
	if err := r.persistLocked(); err != nil {
		return Version{}, err
	}
	return v, nil
}

// PublishResult 是发布结果。
type PublishResult struct {
	Version   Version
	Class     ChangeClass
	Activated bool
}

// Publish 把草稿发布为不可变版本。
//
// 版本号规则（由兼容性判定自动决定，不可人为指定）：
//   - 特征首次发布 → 1.0；
//   - 相对当前激活版本只有兼容变化 → 同 major 下 minor+1，并自动成为激活版本；
//   - 不兼容变化 → major+1、minor 归零，保持“未激活”（Activated=false）。
//     它必须经 Activate 显式切换才会成为默认版本；旧 major 的绑定全程读旧语义。
//
// 幂等：草稿内容与某已发布版本指纹相同，直接返回该版本，不制造重复版本号，
// 因此“发布中断后重试”不会产生垃圾版本。
func (r *Registry) Publish(name, actor string) (PublishResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	draft, ok := r.st.Drafts[name]
	if !ok {
		return PublishResult{}, fmt.Errorf("%w: %s", ErrNoDraft, name)
	}
	if fqvn, dup := r.findSameFingerprintLocked(name, draft.Fingerprint); dup {
		delete(r.st.Drafts, name)
		if err := r.appendAuditLocked("publish_idempotent", name, fqvn, draft.Revision, "", nil); err != nil {
			return PublishResult{}, err
		}
		if err := r.persistLocked(); err != nil {
			return PublishResult{}, err
		}
		v := r.st.Versions[fqvn]
		return PublishResult{Version: v, Class: ChangeCompatible, Activated: r.st.Active[name] == fqvn}, nil
	}

	var (
		major, minor int
		class        ChangeClass = ChangeCompatible
	)
	activeFQVN, hasActive := r.st.Active[name]
	if !hasActive {
		major, minor = 1, 0
	} else {
		active := r.st.Versions[activeFQVN]
		class = Classify(active.Definition, draft.Definition)
		if class == ChangeBreaking {
			major, minor = active.Major+1, 0
		} else {
			major, minor = active.Major, r.highestMinorLocked(name, active.Major)+1
		}
	}

	fqvn := FQVN(name, major, minor)
	if _, exists := r.st.Versions[fqvn]; exists {
		return PublishResult{}, fmt.Errorf("registry: internal error, %s already exists", fqvn)
	}
	now := r.now()
	pub := Version{
		FQVN: fqvn, Name: name, Major: major, Minor: minor,
		Status: StatusPublished, Definition: draft.Definition,
		Fingerprint: draft.Fingerprint, BasedOn: draft.BasedOn,
		ChangeNote: draft.ChangeNote, Revision: -1,
		CreatedAt: now, PublishedAt: &now,
	}
	r.st.Versions[fqvn] = pub
	delete(r.st.Drafts, name)

	activated := false
	// 兼容发布自动激活；不兼容发布保持上一版继续服务，等待显式 Activate。
	if class == ChangeCompatible || !hasActive {
		r.st.Active[name] = fqvn
		activated = true
	}
	detail := map[string]any{"actor": actor, "activated": activated}
	if err := r.appendAuditLocked("publish", name, fqvn, draft.Revision, class, detail); err != nil {
		return PublishResult{}, err
	}
	if err := r.persistLocked(); err != nil {
		return PublishResult{}, err
	}
	return PublishResult{Version: pub, Class: class, Activated: activated}, nil
}

// Activate 显式把某已发布版本设为该特征的默认版本（受控发布的切换动作）。
// 用于不兼容新版本的灰度切换，也用于回退。
func (r *Registry) Activate(fqvn, actor string) error {
	name, _, _, ok := ParseFQVN(fqvn)
	if !ok {
		return fmt.Errorf("%w: %q", ErrInvalidName, fqvn)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.st.Versions[fqvn]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, fqvn)
	}
	if v.Status == StatusDraft {
		return fmt.Errorf("%w: %s", ErrImmutable, fqvn)
	}
	prev := r.st.Active[name]
	r.st.Active[name] = fqvn
	if err := r.appendAuditLocked("activate", name, fqvn, -1, "",
		map[string]any{"actor": actor, "previous": prev}); err != nil {
		return err
	}
	return r.persistLocked()
}

// Rollback 把默认版本切回该特征此前发布过的任一版本。
// 被回退的版本不删除、不改写，只是激活指针移动——已确认版本的正常服务不会被治理动作阻断。
func (r *Registry) Rollback(name, targetFQVN, actor string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	target, ok := r.st.Versions[targetFQVN]
	if !ok || target.Name != name {
		return fmt.Errorf("%w: %s", ErrNotFound, targetFQVN)
	}
	prev := r.st.Active[name]
	if prev == targetFQVN {
		return nil
	}
	r.st.Active[name] = targetFQVN
	if err := r.appendAuditLocked("rollback", name, targetFQVN, -1, "",
		map[string]any{"actor": actor, "previous": prev}); err != nil {
		return err
	}
	return r.persistLocked()
}

// Deprecate 标记版本弃用并给出迁移去向。弃用不删除版本：
// 已存在的绑定仍可解析、仍可服务，只有“新建绑定引用弃用版本”会被拒绝，
// 从而给消费者留出升级窗口，又不阻断线上流量。
func (r *Registry) Deprecate(fqvn, successor string, sunsetAt *time.Time, actor string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.st.Versions[fqvn]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, fqvn)
	}
	if successor != "" {
		s, ok := r.st.Versions[successor]
		if !ok || s.Name != v.Name {
			return fmt.Errorf("%w: successor %s", ErrNotFound, successor)
		}
	}
	now := r.now()
	v.Status = StatusDeprecated
	v.DeprecatedAt = &now
	v.Successor = successor
	v.SunsetAt = sunsetAt
	r.st.Versions[fqvn] = v
	// 若弃用的是激活版本，自动把激活指针移到后继；没有后继则保持不变，
	// 因为“没有可切换目标时阻断服务”比“继续服务一个弃用版本”更糟。
	if r.st.Active[v.Name] == fqvn && successor != "" {
		r.st.Active[v.Name] = successor
	}
	if err := r.appendAuditLocked("deprecate", v.Name, fqvn, -1, "",
		map[string]any{"actor": actor, "successor": successor}); err != nil {
		return err
	}
	return r.persistLocked()
}

// Get 读取任一版本（含弃用版本——历史快照必须可追溯）。
func (r *Registry) Get(fqvn string) (Version, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.st.Versions[fqvn]
	if !ok {
		return Version{}, fmt.Errorf("%w: %s", ErrNotFound, fqvn)
	}
	return v, nil
}

// Draft 读取某特征的当前草稿。
func (r *Registry) Draft(name string) (Version, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.st.Drafts[name]
	if !ok {
		return Version{}, fmt.Errorf("%w: draft %s", ErrNotFound, name)
	}
	return v, nil
}

// Active 返回某特征的默认发布版本。
func (r *Registry) Active(name string) (Version, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fqvn, ok := r.st.Active[name]
	if !ok {
		return Version{}, fmt.Errorf("%w: active %s", ErrNotFound, name)
	}
	return r.st.Versions[fqvn], nil
}

// ListVersions 返回某特征按版本号排序的全部版本（含弃用）。
func (r *Registry) ListVersions(name string) []Version {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Version
	for _, v := range r.st.Versions {
		if v.Name == name {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Major != out[j].Major {
			return out[i].Major < out[j].Major
		}
		return out[i].Minor < out[j].Minor
	})
	return out
}

// ListActive 返回全部激活版本，按特征名排序——这是新消费者的默认视图。
func (r *Registry) ListActive() []Version {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.st.Active))
	for n := range r.st.Active {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]Version, 0, len(names))
	for _, n := range names {
		out = append(out, r.st.Versions[r.st.Active[n]])
	}
	return out
}

// ListDrafts 返回全部草稿，按特征名排序。
func (r *Registry) ListDrafts() []Version {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.st.Drafts))
	for n := range r.st.Drafts {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]Version, 0, len(names))
	for _, n := range names {
		out = append(out, r.st.Drafts[n])
	}
	return out
}

// Audit 返回审计链（历史追溯用）。
func (r *Registry) Audit() ([]AuditEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.readAudit()
}

func (r *Registry) findSameFingerprintLocked(name, fp string) (string, bool) {
	for fqvn, v := range r.st.Versions {
		if v.Name == name && v.Fingerprint == fp {
			return fqvn, true
		}
	}
	return "", false
}

func (r *Registry) highestMinorLocked(name string, major int) int {
	hi := -1
	for _, v := range r.st.Versions {
		if v.Name == name && v.Major == major && v.Minor > hi {
			hi = v.Minor
		}
	}
	return hi
}
