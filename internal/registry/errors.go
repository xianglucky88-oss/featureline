package registry

import "errors"

var (
	// ErrNotFound 版本或草稿不存在。
	ErrNotFound = errors.New("registry: not found")
	// ErrConflict 乐观锁冲突：同名草稿已被并发修改。
	ErrConflict = errors.New("registry: revision conflict (concurrent draft modification)")
	// ErrImmutable 试图修改已发布版本或修改不可变字段。
	ErrImmutable = errors.New("registry: published version is immutable")
	// ErrInvalidName 特征名不合法。
	ErrInvalidName = errors.New("registry: invalid feature name")
	// ErrInvalidDefinition 定义语义不合法（未知类型、窗口约定等）。
	ErrInvalidDefinition = errors.New("registry: invalid definition")
	// ErrNoDraft 发布目标没有草稿。
	ErrNoDraft = errors.New("registry: no draft to publish")
	// ErrInactiveBreaking 不兼容新版本必须显式激活，不能静默替换默认版本。
	ErrInactiveBreaking = errors.New("registry: breaking release stays inactive until explicitly activated")
	// ErrBindingStale 绑定引用的版本与注册表里的指纹不一致，或版本已不存在。
	ErrBindingStale = errors.New("registry: binding cannot be resolved against current registry")
	// ErrDeprecated 已弃用版本不能再被新绑定引用（旧绑定不受影响）。
	ErrDeprecated = errors.New("registry: version deprecated, use successor for new bindings")
)
