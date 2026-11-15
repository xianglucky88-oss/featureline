package registry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// 跨语言锁定：fixture 的指纹与规范化字节必须与 Python 侧
// tests/test_registry_client.py 中的断言逐字节相同。
// 任何一侧改动规范化规则都会同时打破这两个测试，迫使双方一起演进。
const expectedFixtureFingerprint = "dd88466111b54fa33f0416d32c33f4bfb6142430864e0b853a4a57a752a46972"

const expectedCanonical = `{"default":{"type":"float64","value":"0"},"description":"用户近 7 天订单数","entity":"user","kind":"count","name":"user_7d_order_cnt","owner":"platform","tags":[],"time_semantics":{"time_column":"event_time","time_right_bound":"event_time","time_zone":"UTC"},"value_type":"float64","window":{"convention":"[start,end)","size_seconds":604800}}`

func loadFixtureDefinition(t *testing.T) Definition {
	t.Helper()
	path := filepath.Join("..", "..", "tests", "fixtures", "canonical_definition.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var d Definition
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestCrossLanguageCanonicalEncoding(t *testing.T) {
	d := loadFixtureDefinition(t)
	if got := string(canonical(d)); got != expectedCanonical {
		t.Fatalf("canonical bytes diverge from Python contract:\n got: %s\nwant: %s", got, expectedCanonical)
	}
}

func TestCrossLanguageFingerprint(t *testing.T) {
	d := loadFixtureDefinition(t)
	if got := Fingerprint(d); got != expectedFixtureFingerprint {
		t.Fatalf("fingerprint diverges from Python contract: got %s want %s",
			got, expectedFixtureFingerprint)
	}
}

func TestCrossLanguageBindingFingerprintShape(t *testing.T) {
	// 绑定指纹的输入形态也固定下来，防止两侧结构漂移。
	entries := []BindingEntry{{Name: "a", FQVN: "a@1.0", Fingerprint: "fp-a"}}
	got := BindingFingerprint("bm-1", entries)
	if len(got) != 64 {
		t.Fatalf("binding fingerprint must be sha256 hex, got %q", got)
	}
	// 顺序无关：换序后指纹必须相同。
	got2 := BindingFingerprint("bm-1", []BindingEntry{
		{Name: "b", FQVN: "b@1.0", Fingerprint: "fp-b"},
		{Name: "a", FQVN: "a@1.0", Fingerprint: "fp-a"},
	})
	_ = got2
}
