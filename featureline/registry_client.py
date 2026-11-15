"""featureline.registry_client —— 训练侧（Python）的注册表客户端。

与 Go 侧 internal/registry 共享同一套磁盘格式与规范化规则：

    - state.json / audit.jsonl / binding.json 的字段名完全一致；
    - canonical_json 与 Go 的 canonical() 逐字节等价（由跨语言测试锁定）；
    - 指纹算法同为 SHA-256(canonical_json(definition))。

训练任务不直接改注册表（写操作走 Go 服务的治理面或 Go CLI），
这里只提供只读访问 + 绑定解析 + 快照清单读写，保证训练固定在已发布版本上。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# 与 Go 侧常量保持一致。
WINDOW_HALF_OPEN_LEFT = "[start,end)"
ENTITY_USER = "user"
ENTITY_ITEM = "item"
VALUE_FLOAT64 = "float64"
VALUE_INT64 = "int64"
VALUE_STRING = "string"
STATUS_DEPRECATED = "deprecated"


class BindingError(Exception):
    """绑定无法解析（版本缺失、指纹漂移或清单被篡改）。"""


def canonical_json(obj: Any) -> bytes:
    """与 Go internal/registry.canonical 逐字节一致的规范化编码。

    规则：UTF-8、紧凑（无空白）、键名字典序、不做 HTML 转义、
    ensure_ascii=False、数字按 JSON 原文（Python int 序列化为整数，
    与 Go json.Number 输出形态一致；本项目语义字段里没有浮点字面量，
    默认值统一为字符串，因此不存在 1e+06/1000000.0 之类的浮点分歧）。
    """
    return json.dumps(
        _normalize(obj),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _normalize(obj: Any) -> Any:
    # None 切片规整为空列表，对齐 Go 侧 nil -> [] 的处理。
    if isinstance(obj, dict):
        return {k: _normalize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_normalize(v) for v in obj]
    return obj


def fingerprint(definition: dict[str, Any]) -> str:
    # 对齐 Go Fingerprint：缺失或 null 的 tags 规整为空数组后再编码。
    definition = {**definition, "tags": definition.get("tags") or []}
    return hashlib.sha256(canonical_json(definition)).hexdigest()


def binding_fingerprint(manifest_id: str, entries: list[dict[str, Any]]) -> str:
    ordered = sorted((dict(e) for e in entries), key=lambda e: e["name"])
    return hashlib.sha256(
        canonical_json({"manifest_id": manifest_id, "entries": ordered})
    ).hexdigest()


def make_fqvn(name: str, major: int, minor: int) -> str:
    return f"{name}@{major}.{minor}"


def parse_fqvn(fqvn: str) -> tuple[str, int, int]:
    name, _, rest = fqvn.partition("@")
    maj_s, _, min_s = rest.partition(".")
    if not name or not maj_s or not min_s:
        raise ValueError(f"bad FQVN: {fqvn!r}")
    return name, int(maj_s), int(min_s)


@dataclass
class RegistryClient:
    """文件注册表的只读视图。"""

    root: Path

    @classmethod
    def open(cls, root: str | Path) -> "RegistryClient":
        c = cls(root=Path(root))
        c.state_path.parent.mkdir(parents=True, exist_ok=True)
        if not c.state_path.exists():
            raise FileNotFoundError(f"registry state not found: {c.state_path}")
        return c

    @property
    def state_path(self) -> Path:
        return self.root / "state.json"

    def state(self) -> dict[str, Any]:
        with self.state_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def version(self, fqvn: str) -> dict[str, Any]:
        st = self.state()
        try:
            return st["versions"][fqvn]
        except KeyError as e:
            raise BindingError(f"{fqvn} no longer exists in registry") from e

    def active(self, name: str) -> dict[str, Any]:
        st = self.state()
        fqvn = st["active"][name]
        return st["versions"][fqvn]

    def list_active(self) -> list[dict[str, Any]]:
        st = self.state()
        return [st["versions"][st["active"][n]] for n in sorted(st["active"])]

    def resolve(self, binding: "Binding") -> dict[str, dict[str, Any]]:
        """按绑定清单解析出版本集合；任何不一致都抛 BindingError。"""
        binding.validate()
        st = self.state()
        out: dict[str, dict[str, Any]] = {}
        for e in binding.entries:
            v = st["versions"].get(e.fqvn)
            if v is None:
                raise BindingError(f"{e.fqvn} no longer exists in registry")
            if v["fingerprint"] != e.fingerprint:
                raise BindingError(f"{e.fqvn} fingerprint drift")
            out[e.name] = v
        return out


@dataclass
class Binding:
    """不可变绑定清单（与 Go registry.Binding 同构）。"""

    manifest_id: str
    entries: list["BindingEntry"]
    fingerprint: str = ""
    as_of: datetime | None = None
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.fingerprint:
            self.fingerprint = binding_fingerprint(
                self.manifest_id, [e.to_dict() for e in self.entries]
            )

    def validate(self) -> None:
        expect = binding_fingerprint(
            self.manifest_id, [e.to_dict() for e in self.entries]
        )
        if self.fingerprint != expect:
            raise BindingError("binding manifest fingerprint mismatch")
        seen: set[str] = set()
        for e in self.entries:
            name, _, _ = parse_fqvn(e.fqvn)
            if name != e.name or not e.fingerprint:
                raise BindingError(f"malformed binding entry: {e}")
            if e.name in seen:
                raise BindingError(f"duplicated name in binding: {e.name}")
            seen.add(e.name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_id": self.manifest_id,
            "fingerprint": self.fingerprint,
            "as_of": _iso(self.as_of),
            "entries": [e.to_dict() for e in self.entries],
            "created_at": _iso(self.created_at or datetime.now(timezone.utc)),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Binding":
        b = cls(
            manifest_id=raw["manifest_id"],
            fingerprint=raw["fingerprint"],
            entries=[BindingEntry(**e) for e in raw["entries"]],
            as_of=_parse(raw.get("as_of")),
            created_at=_parse(raw.get("created_at")),
        )
        b.validate()
        return b

    @classmethod
    def read(cls, path: str | Path) -> "Binding":
        with Path(path).open("r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))

    def write(self, path: str | Path) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(p)


@dataclass(frozen=True)
class BindingEntry:
    name: str
    fqvn: str
    fingerprint: str

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "fqvn": self.fqvn, "fingerprint": self.fingerprint}


def _iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if dt else None


def _parse(raw: str | None) -> datetime | None:
    if not raw:
        return None
    s = raw.replace("Z", "+00:00")
    if "." in s:
        head, rest = s.split(".", 1)
        frac, sep, tz = rest.partition("+")
        if not sep:
            frac, sep, tz = rest.partition("-")
        frac = (frac + "000000")[:6]
        s = f"{head}.{frac}{sep}{tz}"
    return datetime.fromisoformat(s).astimezone(timezone.utc)
