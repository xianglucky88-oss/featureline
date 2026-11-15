package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

// FQVN 拼装完全限定版本名：name@major.minor。
// 这是注册表中不可变的版本坐标，训练绑定、存储键、快照清单都引用它。
func FQVN(name string, major, minor int) string {
	return name + "@" + itoa(major) + "." + itoa(minor)
}

// ParseFQVN 解析 FQVN。
func ParseFQVN(fqvn string) (name string, major, minor int, ok bool) {
	at := strings.IndexByte(fqvn, '@')
	if at <= 0 || at == len(fqvn)-1 {
		return "", 0, 0, false
	}
	name = fqvn[:at]
	rest := fqvn[at+1:]
	dot := strings.IndexByte(rest, '.')
	if dot <= 0 || dot == len(rest)-1 {
		return "", 0, 0, false
	}
	major, ok1 := atoi(rest[:dot])
	minor, ok2 := atoi(rest[dot+1:])
	if !ok1 || !ok2 || !ValidName(name) {
		return "", 0, 0, false
	}
	return name, major, minor, true
}

// Fingerprint 计算 Definition 的跨语言规范化指纹。
//
// 规范化规则（Go 与 Python 必须逐字节一致，由跨语言测试锁定）：
//   - UTF-8、紧凑 JSON（无空白）、键名字典序；
//   - 关闭 HTML 转义（不输出 < > & 的  转义）；
//   - 只编码 Definition 的语义字段，时间戳、状态等元数据不参与；
//   - nil 切片规整为空数组，避免 null 与 [] 的跨语言分歧；
//   - 数字保留 JSON 原文（UseNumber），不经过浮点中转；
//   - 默认值统一是字符串，杜绝浮点格式分歧（如 1e+06 vs 1000000.0）。
func Fingerprint(d Definition) string {
	if d.Tags == nil {
		d.Tags = []string{}
	}
	sum := sha256.Sum256(canonical(d))
	return hex.EncodeToString(sum[:])
}

// BindingFingerprint 计算绑定清单的语义指纹。
// entries 按名称排序；as_of、created_at 等实例元数据不参与。
func BindingFingerprint(manifestID string, entries []BindingEntry) string {
	sorted := make([]BindingEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	obj := map[string]any{"manifest_id": manifestID, "entries": sorted}
	sum := sha256.Sum256(canonical(obj))
	return hex.EncodeToString(sum[:])
}

// canonical 返回与 Python featureline.registry_client.canonical_json 一致的字节序列。
func canonical(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err) // 入参全是可 JSON 化的基础类型
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var anyv any
	if err := dec.Decode(&anyv); err != nil {
		panic(err)
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	writeCanonical(&b, enc, anyv)
	return []byte(b.String())
}

// writeCanonical 递归写出紧凑且键有序的 JSON。
// 单独持有 encoder 是为了复用其字符串转义（含 SetEscapeHTML(false) 的配置）。
func writeCanonical(b *strings.Builder, enc *json.Encoder, v any) {
	switch t := v.(type) {
	case nil:
		b.WriteString("null")
	case json.Number:
		// 保留数字原文，避免 int64→float64→科学计数法的跨语言分歧。
		b.WriteString(string(t))
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeCanonical(b, enc, k)
			b.WriteByte(':')
			writeCanonical(b, enc, t[k])
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			writeCanonical(b, enc, e)
		}
		b.WriteByte(']')
	default:
		// string / bool：复用 Encoder 的转义规则，裁掉其追加的换行。
		_ = enc.Encode(v)
		trimTrailingNewline(b)
	}
}

func trimTrailingNewline(b *strings.Builder) {
	s := b.String()
	if len(s) > 0 && s[len(s)-1] == '\n' {
		b.Reset()
		b.WriteString(s[:len(s)-1])
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		n--
		buf[n] = '-'
	}
	return string(buf[n:])
}

func atoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
		if s == "" {
			return 0, false
		}
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	if neg {
		n = -n
	}
	return n, true
}
